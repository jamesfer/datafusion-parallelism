use std::cell::UnsafeCell;
use std::fmt::Debug;
use std::future::Future;
use std::hash::{BuildHasher, Hash};
use std::ops::Deref;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use crossbeam::atomic::AtomicCell;
use crossbeam::utils::CachePadded;
use dashmap::{DashMap, ReadOnlyView, SharedValue};
use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::compute::concat_batches;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion_common::DataFusionError;
use futures::FutureExt;

use crate::utils::async_initialize_once::AsyncInitializeOnce;
use crate::utils::index_lookup::IndexLookup;
use crate::utils::limited_rc::LimitedRc;
use crate::utils::once_notify::OnceNotify;

#[derive(Clone, Debug)]
struct Location {
    pub index: usize,
    pub offset: usize,
}

struct OffsetTracker {
    current_location: Mutex<Location>,
}

impl OffsetTracker {
    pub fn new() -> Self {
        Self {
            current_location: Mutex::new(Location {
                index: 0,
                offset: 0,
            }),
        }
    }

    pub fn reserve(&self, length: usize) -> Location {
        let mut current_location = self.current_location.lock().unwrap();
        let result = current_location.clone();
        *current_location = Location {
            index: current_location.index + 1,
            offset: current_location.offset + length,
        };
        result
    }

    pub fn get(&self) -> Location {
        self.current_location.lock().unwrap().clone()
    }
}

#[derive(Debug)]
struct LocalShardEntry {
    pub global_offset: usize,
    pub local_offset: usize,
    pub hash: u64,
    pub internal_hash: u64,
}

pub struct LocalAccumulator {
    local_shard_contents: Vec<Vec<LocalShardEntry>>,
    reference_map: DashMap<(), ()>,
    state: LimitedRc<HashMapState>,
    // Shared among all accumulators
    batch_list: LimitedRc<boxcar::Vec<(usize, RecordBatch)>>,
    offset_tracker: Arc<OffsetTracker>,
    shared_compactor_constructor: SharedCompactorConstructor,
}

impl LocalAccumulator {
    pub fn new(
        shard_count: usize,
        batch_list: LimitedRc<boxcar::Vec<(usize, RecordBatch)>>,
        offset_tracker: Arc<OffsetTracker>,
        reference_map: DashMap<(), ()>,
        state: LimitedRc<HashMapState>,
        shared_compactor_constructor: SharedCompactorConstructor,
    ) -> Self {
        Self {
            local_shard_contents: (0..shard_count).map(|_| vec![]).collect(),
            batch_list,
            offset_tracker,
            reference_map,
            state,
            shared_compactor_constructor,
        }
    }

    // TODO use keys when we need to verify that values match
    pub fn add_records(&mut self, _keys: Vec<ArrayRef>, hashes: Vec<u64>, records: RecordBatch) {
        // Calculate new offset and batch index
        let location = self.offset_tracker.reserve(records.num_rows());

        // Add record batch to global boxcar
        self.batch_list.push((location.index, records));

        // Locally partition keys and hashes based on shards
        for (local_offset, hash) in hashes.into_iter().enumerate() {
            let internal_hash = self.state.hash_lookup.hasher().hash_one(&hash);
            let shard_number = self.state.hash_lookup.determine_shard(internal_hash as usize);
            self.local_shard_contents[shard_number].push(LocalShardEntry {
                global_offset: location.offset,
                local_offset,
                hash,
                internal_hash,
            })
        }
    }

    pub fn submit(self) -> SharedCompactor {
        self.shared_compactor_constructor.build(self.state, self.local_shard_contents, self.batch_list, self.offset_tracker)
    }
}

struct PerformByLastOwner<T> {
    complete: OnceNotify,
    value: OnceLock<T>,
}

impl <T> PerformByLastOwner<T> {
    pub fn new() -> Self {
        Self {
            complete: OnceNotify::new(),
            value: OnceLock::new(),
        }
    }

    pub fn run<F, I>(&self, input: LimitedRc<I>, operation: F) -> Result<&T, impl Future<Output=&T>>
        where F: FnOnce(I) -> T
    {
        if let Some(owned) = LimitedRc::into_inner(input) {
            let output = self.value.get_or_init(|| operation(owned));
            // Trigger the complete notification
            self.complete.notify();
            Ok(output)
        } else {
            Err(async move {
                // Wait for complete to be notified
                self.complete.wait().await;
                self.value.get().expect("Complete notification was triggered before value was written")
            })
        }
    }
}

struct EventuallyConsume<T> {
    complete: OnceNotify,
    value: AtomicCell<Option<T>>,
}

impl <T> EventuallyConsume<T> {
    pub fn new() -> Self {
        Self {
            complete: OnceNotify::new(),
            value: AtomicCell::default(),
        }
    }

    pub fn provide(&self, input: LimitedRc<T>) {
        if let Some(input) = LimitedRc::into_inner(input) {
            self.value.store(Some(input));
            self.complete.notify();
        }
    }

    pub async fn get(&self) -> Option<T> {
        self.complete.wait().await;
        self.value.take()
    }
}

struct SharedCompactorConstructor {
    instance_number: usize,

    // The outer vec has a length of shard_count, the second vec has length of instance_count, and
    // the innermost vec represents the values that were accumulated locally
    shared_shard_contents: LimitedRc<Vec<Vec<CachePadded<AtomicCell<Vec<LocalShardEntry>>>>>>,

    shards_to_compact_sender: flume::Sender<(usize, Vec<CachePadded<AtomicCell<Vec<LocalShardEntry>>>>)>,
    shards_to_compact_receiver: flume::Receiver<(usize, Vec<CachePadded<AtomicCell<Vec<LocalShardEntry>>>>)>,

    eventually_consume_batch_list: Arc<EventuallyConsume<boxcar::Vec<(usize, RecordBatch)>>>,
    compact_batch_list_once: Arc<AsyncInitializeOnce<Result<RecordBatch, DataFusionError>>>,

    // state: LimitedRc<HashMapState>,
    perform_by_last_owner: Arc<PerformByLastOwner<Arc<ReadOnlyJoinMap>>>,

    global_buffer_initializer: Arc<AsyncInitializeOnce<Arc<GlobalBuffer>>>,
}

impl SharedCompactorConstructor {
    pub fn new(
        instance_number: usize,
        shared_shard_contents: LimitedRc<Vec<Vec<CachePadded<AtomicCell<Vec<LocalShardEntry>>>>>>,
        shards_to_compact_sender: flume::Sender<(usize, Vec<CachePadded<AtomicCell<Vec<LocalShardEntry>>>>)>,
        shards_to_compact_receiver: flume::Receiver<(usize, Vec<CachePadded<AtomicCell<Vec<LocalShardEntry>>>>)>,
        eventually_consume_batch_list: Arc<EventuallyConsume<boxcar::Vec<(usize, RecordBatch)>>>,
        compact_batch_list_once: Arc<AsyncInitializeOnce<Result<RecordBatch, DataFusionError>>>,
        perform_by_last_owner: Arc<PerformByLastOwner<Arc<ReadOnlyJoinMap>>>,
        global_buffer_initializer: Arc<AsyncInitializeOnce<Arc<GlobalBuffer>>>,
    ) -> Self {
        Self {
            instance_number,
            shared_shard_contents,
            shards_to_compact_sender,
            shards_to_compact_receiver,
            eventually_consume_batch_list,
            compact_batch_list_once,
            perform_by_last_owner,
            global_buffer_initializer,
        }
    }

    pub fn build(self, state: LimitedRc<HashMapState>, local_shard_contents: Vec<Vec<LocalShardEntry>>, batch_list: LimitedRc<boxcar::Vec<(usize, RecordBatch)>>, offset_tracker: Arc<OffsetTracker>) -> SharedCompactor {
        // Indicate that we are finished with the batch list
        self.eventually_consume_batch_list.provide(batch_list);

        SharedCompactor::new(
            self.instance_number,
            self.shared_shard_contents,
            local_shard_contents,
            self.shards_to_compact_sender,
            self.shards_to_compact_receiver,
            self.eventually_consume_batch_list,
            self.compact_batch_list_once,
            state,
            self.perform_by_last_owner,
            offset_tracker,
            self.global_buffer_initializer,
        )
    }
}

struct HashMapState {
    hash_lookup: DashMap<u64, Vec<usize>>,
}

pub struct SharedCompactor {
    instance_number: usize,

    // The outer vec has a length of shard_count, the second vec has length of instance_count, and
    // the innermost vec represents the values that were accumulated locally
    shared_shard_contents: LimitedRc<Vec<Vec<CachePadded<AtomicCell<Vec<LocalShardEntry>>>>>>,
    local_shard_contents: Vec<Vec<LocalShardEntry>>,

    eventually_consume_batch_list: Arc<EventuallyConsume<boxcar::Vec<(usize, RecordBatch)>>>,
    compact_batch_list_once: Arc<AsyncInitializeOnce<Result<RecordBatch, DataFusionError>>>,

    shards_to_compact_sender: flume::Sender<(usize, Vec<CachePadded<AtomicCell<Vec<LocalShardEntry>>>>)>,
    shards_to_compact_receiver: flume::Receiver<(usize, Vec<CachePadded<AtomicCell<Vec<LocalShardEntry>>>>)>,

    state: LimitedRc<HashMapState>,
    build_join_map_when_last: Arc<PerformByLastOwner<Arc<ReadOnlyJoinMap>>>,

    offset_tracker: Arc<OffsetTracker>,
    global_buffer_initializer: Arc<AsyncInitializeOnce<Arc<GlobalBuffer>>>,
}

impl SharedCompactor {
    pub fn new(
        instance_number: usize,
        shared_shard_contents: LimitedRc<Vec<Vec<CachePadded<AtomicCell<Vec<LocalShardEntry>>>>>>,
        local_shard_contents: Vec<Vec<LocalShardEntry>>,
        shards_to_compact_sender: flume::Sender<(usize, Vec<CachePadded<AtomicCell<Vec<LocalShardEntry>>>>)>,
        shards_to_compact_receiver: flume::Receiver<(usize, Vec<CachePadded<AtomicCell<Vec<LocalShardEntry>>>>)>,
        eventually_consume_batch_list: Arc<EventuallyConsume<boxcar::Vec<(usize, RecordBatch)>>>,
        compact_batch_list_once: Arc<AsyncInitializeOnce<Result<RecordBatch, DataFusionError>>>,
        state: LimitedRc<HashMapState>,
        build_join_map_when_last: Arc<PerformByLastOwner<Arc<ReadOnlyJoinMap>>>,
        offset_tracker: Arc<OffsetTracker>,
        global_buffer_initializer: Arc<AsyncInitializeOnce<Arc<GlobalBuffer>>>,
    ) -> Self {
        Self {
            instance_number,
            shared_shard_contents,
            local_shard_contents,
            eventually_consume_batch_list,
            compact_batch_list_once,
            shards_to_compact_sender,
            shards_to_compact_receiver,
            state,
            build_join_map_when_last,
            offset_tracker,
            global_buffer_initializer,
        }
    }

    pub async fn compact(self, schema: SchemaRef) -> Result<(RecordBatch, impl IndexLookup<u64>), DataFusionError> {
        // Adds the contents of each shard to the global store at the index corresponding to this instance (maybe if this is the last writer to a shard, mark it as ready for compaction)
        Self::publish_local_shard_contents(
            self.shared_shard_contents,
            self.local_shard_contents,
            self.instance_number,
            self.shards_to_compact_sender,
            &self.offset_tracker,
            &self.global_buffer_initializer,
        ).await?;

        // The first thread that reaches here takes ownership of compacting the batches, but it
        // must wait for all the LocalAccumulators have finished processing all their batches
        let compact_batch_list_result = self.compact_batch_list_once.run_once(|| async {
            let batch_list = self.eventually_consume_batch_list.get().await
                .ok_or(DataFusionError::Internal("Batch list is gone. It was either taken by another thread, or notify was called before it was written.".to_string()))?;
            Ok(Self::compact_batches(&schema, &batch_list)?)
        });
        let compact_batch_list_result = match compact_batch_list_result {
            // If we are the first thread, we want to wait for the future here. If not, we can keep the
            // result as a future that we will consume later
            Ok(compact_batch_list_future) => {
                let completed_batch_list = match compact_batch_list_future.await {
                    Ok(record_batch) => Ok(record_batch.clone()),
                    Err(err_ref) => Err(DataFusionError::Internal(format!("Failed to compact batches due to: {}", err_ref.to_string()))),
                }?;
                Ok(completed_batch_list)
            },
            Err(compact_batch_list_future) => Err(compact_batch_list_future),
        };

        // Waits for all local data to be written to the global store, then streams shards to compact via a shared channel
        //   Compaction of a shard involves allocating an overflow buffer, locking the matching shard in a dashmap, and writing all the results at once,
        let global_buffer = self.global_buffer_initializer.get().await.clone();
        while let Ok((shard_number, all_shard_contents)) = self.shards_to_compact_receiver.recv_async().await {
            Self::compact_shard(shard_number, all_shard_contents, &self.state.hash_lookup);
            // self.state.overflow_buffers[shard_number].store(shard_buffer);
        }

        // Once the last thread has written all the data to the state, we can finally build a
        // concise read only lookup map
        let join_map_result = self.build_join_map_when_last.run(self.state, |state| {
            // Arc::new(ReadOnlyJoinMap::new(state.hash_lookup, global_buffer.to_vec()))
            Arc::new(ReadOnlyJoinMap::new(state.hash_lookup.into_read_only()))
        });

        // Wait for the record batch and join map futures. It doesn't matter which order we do it in
        // as we need both values and no work is being performed by these futures
        let join_map = match join_map_result {
            Ok(join_map) => Arc::clone(join_map),
            Err(join_map_future) => Arc::clone(join_map_future.await),
        };
        let record_batch = match compact_batch_list_result {
            Ok(record_batch) => record_batch,
            Err(compact_record_batch_future) => {
                match compact_record_batch_future.await {
                    Ok(record_batch) => Ok(record_batch.clone()),
                    Err(err_ref) => Err(DataFusionError::Internal(format!("Failed to compact batches due to: {}", err_ref.to_string()))),
                }?
            },
        };

        Ok((record_batch, join_map))
    }

    async fn publish_local_shard_contents(
        shared_shard_contents: LimitedRc<Vec<Vec<CachePadded<AtomicCell<Vec<LocalShardEntry>>>>>>,
        local_shard_contents: Vec<Vec<LocalShardEntry>>,
        instance_number: usize,
        shards_to_compact_sender: flume::Sender<(usize, Vec<CachePadded<AtomicCell<Vec<LocalShardEntry>>>>)>,
        offset_tracker: &OffsetTracker,
        global_buffer_initializer: &AsyncInitializeOnce<Arc<GlobalBuffer>>,
    ) -> Result<(), DataFusionError> {
        let combined_shard_contents = local_shard_contents.into_iter()
            .zip(shared_shard_contents.iter());
        for (shard_number, (local_contents, shared_contents)) in combined_shard_contents.enumerate() {
            // Write the local contents to the shared vector at the index instance_number
            shared_contents[instance_number].store(local_contents);

            // Attempt to consume the shared contents if all threads have finished writing to it
            // if let Some(shared_contents) = LimitedRc::into_inner(shared_contents) {
            //     shards_to_compact_sender.send((shard_number, shared_contents))
            //         .map_err(|err| DataFusionError::Internal(format!("Failed to send contents of shard {} for compaction", err.0.0)))?
            // }
        }

        // Send shard contents to sender
        if let Some(owned_shard_contents) = LimitedRc::into_inner(shared_shard_contents) {
            // Build the global buffer
            let global_buffer = Arc::new(GlobalBuffer::new(offset_tracker.get().offset + 1));
            global_buffer_initializer.run_first(|| async { global_buffer })
                .expect("Another thread tried to build the global buffer")
                .await;

            for (shard_number, shared_contents) in owned_shard_contents.into_iter().enumerate() {
                shards_to_compact_sender.send((shard_number, shared_contents))
                    .map_err(|err| DataFusionError::Internal(format!("Failed to send contents of shard {} for compaction", err.0.0)))?
            }
        }

        // It's very important that we drop the shards_to_compact_sender in this method to prevent
        // consuming threads waiting forever.
        Ok(())
    }

    fn compact_batches(
        schema: &SchemaRef,
        batch_list: &boxcar::Vec<(usize, RecordBatch)>,
    ) -> Result<RecordBatch, DataFusionError> {
        // Create a copy of just the target indices from the batch list. This array will be used for
        // many accesses, and it's more efficient to access a normal, non-concurrent vec than a boxcar.
        let target_indices: Vec<_> = batch_list.iter().map(|(index, _)| index).collect();

        // Create a range the size of the batch list, then sort it based on each index's target index.
        // This creates a vector where each value is the index of a record batch, and the position is
        // its sorted location based on the target index of that record batch.
        let mut sorted_indices: Vec<_> = (0..target_indices.len()).collect();
        // Whether stable or unstable sorting is faster here is probably debatable, given that the list
        // is probably very close to sorted
        sorted_indices.sort_by_key(|location_index| target_indices[*location_index]);

        let batches = sorted_indices.into_iter()
            .map(|location_index|
                batch_list.get(location_index)
                    .map(|(_, batch)| batch)
                    .ok_or(DataFusionError::Internal("Batch list missing index from sorted list".to_string()))
            )
            .collect::<Result<Vec<_>, _>>()?;
        Ok(concat_batches(schema, batches)?)
    }

    fn compact_shard(
        shard_number: usize,
        all_shard_contents: Vec<CachePadded<AtomicCell<Vec<LocalShardEntry>>>>,
        output_hash_lookup: &DashMap<u64, Vec<usize>>,
    ) {
        let entries = all_shard_contents.into_iter()
            .map(|cell| cell.take().into_iter().rev())
            .flatten()
            .collect::<Vec<_>>();

        // Lock the shard in the output dashmap. We should be the only writer of this shard
        let mut locked_shard = output_hash_lookup.shards()[shard_number].write();

        // Write all the values in this group to the locked shard at once
        for entry in entries.into_iter() {
            // let global_index = entry.global_offset + entry.local_offset + 1;
            let global_index = entry.global_offset + entry.local_offset;

            Self::append_to_raw_table(
                &mut locked_shard,
                entry.hash,
                entry.internal_hash,
                global_index,
                |k| output_hash_lookup.hasher().hash_one(k),
            )
        }
    }

    fn append_to_raw_table<K: Debug, V, F>(
        raw_table: &mut hashbrown::raw::RawTable<(K, SharedValue<Vec<V>>)>,
        key: K,
        key_hash: u64,
        value: V,
        hash_function: F,
    )
        where
            F: Fn(&K) -> u64,
            K: PartialEq,
    {
        // This code is copied from the private methods of the hash map to write directly to
        // a raw table
        match raw_table.find_or_find_insert_slot(
            key_hash,
            |(k, _v)| k == &key,
            |(k, _v)| hash_function(k),
        ) {
            // find_or_find_insert_slot returns Ok when an element already exists in that spot
            Ok(elem) => {
                // It is safe to write to this element since we have the mutable reference to the
                // table, and find_or_find_insert_slot ensures that the memory is valid.
                unsafe { elem.as_mut().1.get_mut() }.push(value);
            },
            // and it returns Err when the slot is empty
            Err(slot) => unsafe {
                raw_table.insert_in_slot(key_hash, slot, (key, SharedValue::new(vec![value])));
            },
        }
    }
}

struct GlobalBuffer {
    vec: Vec<AtomicUsize>
}

impl GlobalBuffer {
    pub fn new(size: usize) -> Self {
        Self {
            vec: (0..size).map(|_| AtomicUsize::new(0)).collect::<Vec<_>>(),
        }
    }

    pub fn set(&self, index: usize, value: usize) {
        self.vec[index].store(value, Ordering::Relaxed);
    }

    pub fn get(&self, index: usize) -> usize {
        self.vec[index].load(Ordering::Relaxed)
    }

    pub fn to_vec(&self) -> Vec<usize> {
        self.vec.iter()
            .map(|atomic| atomic.load(Ordering::Relaxed))
            .collect()
    }
}

struct UnsafeCellSendWrapper<T> {
    cell: UnsafeCell<T>
}

unsafe impl <T: Send> Send for UnsafeCellSendWrapper<T> {}
unsafe impl <T: Sync> Sync for UnsafeCellSendWrapper<T> {}

impl <T> UnsafeCellSendWrapper<T> {
    pub fn new(cell: UnsafeCell<T>) -> Self {
        Self { cell }
    }
}

impl <T> Deref for UnsafeCellSendWrapper<T> {
    type Target = UnsafeCell<T>;

    fn deref(&self) -> &Self::Target {
        &self.cell
    }
}

type ReadOnlyJoinMap = ReadOnlyJoinMapT<u64>;

// Copied from version 1
#[derive(Clone)]
pub struct ReadOnlyJoinMapT<K>
    where K: Eq + Hash + Clone
{
    empty_vec: Vec<usize>,
    map: ReadOnlyView<K, Vec<usize>>,
}

impl <T> ReadOnlyJoinMapT<T>
    where T: Eq + Hash + Clone {
    pub fn new(map: ReadOnlyView<T, Vec<usize>>) -> Self {
        Self { empty_vec: vec![], map }
    }
}

impl <T> IndexLookup<T> for Arc<ReadOnlyJoinMapT<T>>
    where T: Eq + Hash + Clone {
    type It<'a> = VecIterator<'a, usize>
        where T: 'a;

    fn get_iter<'a>(&'a self, key: &'a T) -> Self::It<'a> {
        match self.map.get(key) {
            None => VecIterator::new(self.empty_vec.iter()),
            Some(vec) => VecIterator::new(vec.iter()),
        }
    }
}

pub struct VecIterator<'a, T> {
    iter: std::slice::Iter<'a, T>,
}

impl<'a, T> VecIterator<'a, T> {
    pub fn new(iter: std::slice::Iter<'a, T>) -> Self {
        Self { iter }
    }
}

impl <'a, T> Iterator for VecIterator<'a, T>
    where T: Copy
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|n| *n)
    }
}

pub struct ReadOnlyJoinMapIterator<'a> {
    index: usize,
    overflow: &'a [usize],
}

impl ReadOnlyJoinMapIterator<'_> {
    fn new(index: usize, overflow: &[usize]) -> ReadOnlyJoinMapIterator {
        ReadOnlyJoinMapIterator {
            index,
            overflow
        }
    }
}

impl Iterator for ReadOnlyJoinMapIterator<'_> {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index == 0 {
            return None;
        }

        let output = self.index - 1;
        self.index = self.overflow[self.index];
        Some(output)
    }
}



// Copied from version 1
#[derive(Clone)]
pub struct ReadOnlyJoinMapU {
    map: ReadOnlyView<u64, usize>,
    overflow: Vec<usize>,
}

impl ReadOnlyJoinMapU {
    pub fn new(map: ReadOnlyView<u64, usize>, overflow: Vec<usize>) -> Self {
        Self { map, overflow }
    }
}

impl IndexLookup<u64> for Arc<ReadOnlyJoinMapU> {
    type It<'a> = ReadOnlyJoinMapIterator<'a>;

    fn get_iter<'a>(&'a self, key: &'a u64) -> Self::It<'a> {
        let starting_index = self.map.get(key).map(|index| *index).unwrap_or(0usize);
        println!("Starting index: {}, key {}", starting_index, key);
        ReadOnlyJoinMapIterator::new(starting_index, &self.overflow)
    }
}


// pub struct ReadOnlyJoinMap {
//     hash_lookup: ReadOnlyView<u64, usize>,
//     overflow_buffer: Vec<usize>,
// }
//
// impl ReadOnlyJoinMap {
//     pub fn new(
//         hash_lookup: DashMap<u64, usize>,
//         overflow_buffer: Vec<usize>,
//     ) -> Self {
//         Self {
//             hash_lookup: hash_lookup.into_read_only(),
//             overflow_buffer,
//         }
//     }
// }
//
// impl IndexLookup<u64> for ReadOnlyJoinMap {
//     type It<'a> = ReadOnlyJoinMapIterator<'a>
//         where Self: 'a, u64: 'a;
//
//     fn get_iter<'a>(&'a self, hash: &'a u64) -> Self::It<'a> {
//         let index = self.hash_lookup.get(hash).map(|index| *index).unwrap_or(0usize);
//         ReadOnlyJoinMapIterator::new(index, &self.overflow_buffer)
//     }
// }
//
// impl IndexLookup<u64> for Arc<ReadOnlyJoinMap> {
//     type It<'a> = ReadOnlyJoinMapIterator<'a>
//         where Self: 'a, u64: 'a;
//
//     fn get_iter<'a>(&'a self, hash: &'a u64) -> Self::It<'a> {
//         self.as_ref().get_iter(hash)
//     }
// }

// pub struct ReadOnlyJoinMapIterator<'a> {
//     global_index: usize,
//     overflow: &'a [usize],
// }
//
// impl <'a> ReadOnlyJoinMapIterator<'a> {
//     fn new(global_index: usize, overflow: &'a [usize]) -> Self {
//         Self {
//             global_index,
//             overflow
//         }
//     }
// }
//
// impl Iterator for ReadOnlyJoinMapIterator<'_> {
//     type Item = usize;
//
//     fn next(&mut self) -> Option<Self::Item> {
//         if self.global_index == 0 {
//             return None;
//         }
//
//         let output = self.global_index;
//         self.global_index = self.overflow[self.global_index];
//         Some(output - 1)
//     }
// }

pub fn create_local_accumulators(count: usize) -> Vec<LocalAccumulator> {
    let reference_map = DashMap::new();
    let shard_count = reference_map.shards().len();

    // Each instance needs a vector the length of the number of instances for each shard. We create
    // it by creating shard_count sets of LimitedRc values, then transpose them so the outer list
    // has a length equal to the instance count, so it can be zipped into the main iterator. The
    // middle vector will be the length of the shard count.
    let shared_shard_contents = LimitedRc::new_copies(
        (0..shard_count)
            .map(|_| {
                (0..count).map(|_| CachePadded::new(AtomicCell::new(vec![]))).collect::<Vec<_>>()
            })
            .collect(),
        count,
    );

    let state = HashMapState {
        hash_lookup: DashMap::new(),
    };

    let (shards_to_compact_sender, shards_to_compact_receiver) = flume::bounded(shard_count);
    let batch_list = boxcar::Vec::new();
    let offset_tracker = Arc::new(OffsetTracker::new());
    let perform_by_last_owner = Arc::new(PerformByLastOwner::new());

    let eventually_consume_batch_list = Arc::new(EventuallyConsume::new());
    let compact_batch_list_once = Arc::new(AsyncInitializeOnce::new());

    let global_buffer_initializer = Arc::new(AsyncInitializeOnce::new());

    (0..count)
        .into_iter()
        .zip(shared_shard_contents.into_iter())
        .zip(LimitedRc::new_copies(state, count))
        .zip(LimitedRc::new_copies(batch_list, count))
        .map(|(((instance_number, shared_shard_contents), state), batch_list)| {
            LocalAccumulator::new(
                shard_count,
                batch_list,
                Arc::clone(&offset_tracker),
                reference_map.clone(),
                state,
                SharedCompactorConstructor::new(
                    instance_number,
                    shared_shard_contents,
                    shards_to_compact_sender.clone(),
                    shards_to_compact_receiver.clone(),
                    Arc::clone(&eventually_consume_batch_list),
                    Arc::clone(&compact_batch_list_once),
                    Arc::clone(&perform_by_last_owner),
                    global_buffer_initializer.clone(),
                ),
            )
        })
        .collect::<Vec<_>>()
}

fn transpose<T>(input: Vec<Vec<T>>) -> Vec<Vec<T>> {
    if input.len() == 0 {
        return vec![];
    }

    let inner_length = input[0].len();

    // Convert each of the inner vectors into iterators
    let mut iters: Vec<_> = input.into_iter().map(|n| n.into_iter()).collect();

    // Create an outer vector the length of the original inner vectors
    (0..inner_length)
        .map(|_| {
            // Pull the first item from each of the inner vectors
            iters
                .iter_mut()
                .map(|n| n.next().unwrap())
                .collect::<Vec<T>>()
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::{Array, ArrayRef, Int32Array};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::error::ArrowError;
    use datafusion::arrow::record_batch::RecordBatch;
    use futures::stream::FuturesUnordered;
    use futures::StreamExt;
    use tokio::spawn;

    use crate::operator::version7::hash_lookup_builder::create_local_accumulators;
    use crate::utils::index_lookup::IndexLookup;

    #[tokio::test]
    async fn returns_nothing_when_empty() {
        let schema = Arc::new(Schema::empty());
        let local_accumulators = create_local_accumulators(10);
        let results = local_accumulators.into_iter()
            .map(|local| {
                let schema = schema.clone();
                spawn(async move { local.submit().compact(schema).await })
            })
            .collect::<FuturesUnordered<_>>()
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        for (record_batch, read_only_map) in results.iter() {
            assert_eq!(record_batch.num_rows(), 0);
            assert_eq!(read_only_map.get_iter(&1).collect::<Vec<usize>>(), Vec::<usize>::new());
        }
    }

    #[tokio::test]
    async fn all_threads_can_read_keys() {
        let schema = Arc::new(Schema::empty());
        let mut local_accumulators = create_local_accumulators(10);
        {
            let mut first_accumulator = local_accumulators.get_mut(0).unwrap();
            first_accumulator.add_records(vec![], vec![1, 2, 3, 4], {
                RecordBatchBuilder::new()
                    .add("col1", vec![10, 20, 30, 40])
                    .build()
                    .unwrap()
            });
            first_accumulator.add_records(vec![], vec![5, 6, 7, 8, 9], {
                RecordBatchBuilder::new()
                    .add("col1", vec![50, 60, 70, 80, 90])
                    .build()
                    .unwrap()
            });
        }

        let results = local_accumulators.into_iter()
            .map(|local| {
                let schema = schema.clone();
                spawn(async move { local.submit().compact(schema).await })
            })
            .collect::<FuturesUnordered<_>>()
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        // All maps should contain the entries
        for (record_batch, read_only_map) in results.iter() {
            assert_eq!(record_batch.num_rows(), 9);
            assert_eq!(read_only_map.get_iter(&1).collect::<Vec<usize>>(), vec![0usize]);
            assert_eq!(read_only_map.get_iter(&9).collect::<Vec<usize>>(), vec![8usize]);
        }
    }

    #[tokio::test]
    async fn can_store_multiple_indexes_with_same_key_from_one_accumulator() {
        let schema = Arc::new(Schema::empty());
        let mut local_accumulators = create_local_accumulators(10);
        {
            let mut first_accumulator = local_accumulators.get_mut(0).unwrap();
            first_accumulator.add_records(vec![], vec![1, 2, 3, 4], {
                RecordBatchBuilder::new()
                    .add("col1", vec![10, 20, 30, 40])
                    .build()
                    .unwrap()
            });
            first_accumulator.add_records(vec![], vec![1, 5, 1, 6, 1], {
                RecordBatchBuilder::new()
                    .add("col1", vec![10, 50, 10, 60, 10])
                    .build()
                    .unwrap()
            });
        }

        let results = local_accumulators.into_iter()
            .map(|local| {
                let schema = schema.clone();
                spawn(async move { local.submit().compact(schema).await })
            })
            .collect::<FuturesUnordered<_>>()
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        // All maps should contain the entries
        for (record_batch, read_only_map) in results.iter() {
            assert_eq!(record_batch.num_rows(), 9);
            assert_eq!(read_only_map.get_iter(&1).collect::<Vec<usize>>(), vec![
                8usize,
                6usize,
                4usize,
                0usize,
            ]);
        }
    }

    #[tokio::test]
    async fn can_store_multiple_indexes_with_same_key_from_multiple_accumulators() {
        let schema = Arc::new(Schema::empty());
        let mut local_accumulators = create_local_accumulators(10);
        for accumulator in local_accumulators.iter_mut() {
            accumulator.add_records(vec![], vec![1, 2, 3, 4], {
                RecordBatchBuilder::new()
                    .add("col1", vec![10, 20, 30, 40])
                    .build()
                    .unwrap()
            });
        }

        let results = local_accumulators.into_iter()
            .map(|local| {
                let schema = schema.clone();
                spawn(async move { local.submit().compact(schema).await })
            })
            .collect::<FuturesUnordered<_>>()
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        // All maps should contain the entries
        for (record_batch, read_only_map) in results.iter() {
            assert_eq!(record_batch.num_rows(), 40);

            // The order between accumulators is not guaranteed
            let mut indexes = read_only_map.get_iter(&1).collect::<Vec<usize>>();
            indexes.sort();
            assert_eq!(indexes, vec![
                0usize,
                4usize,
                8usize,
                12usize,
                16usize,
                20usize,
                24usize,
                28usize,
                32usize,
                36usize,
            ]);
        }
    }

    pub trait HasArrowType {
        type Vector: Array;

        fn data_type() -> DataType;
        fn nullable() -> bool;

        fn make_field(name: impl Into<String>) -> Field {
            Field::new(name, Self::data_type(), Self::nullable())
        }

        fn make_vector(data: Vec<Self>) -> Self::Vector
            where Self: Sized;

        fn make_array(data: Vec<Self>) -> ArrayRef
            where
                Self: Sized,
                Self::Vector: 'static
        {
            Arc::new(Self::make_vector(data))
        }
    }

    impl HasArrowType for i32 {
        type Vector = Int32Array;

        fn data_type() -> DataType {
            DataType::Int32
        }

        fn nullable() -> bool {
            false
        }

        fn make_vector(data: Vec<Self>) -> Self::Vector {
            Int32Array::from(data)
        }
    }

    pub struct RecordBatchBuilder {
        fields: Vec<Field>,
        arrays: Vec<ArrayRef>,
    }

    impl RecordBatchBuilder {
        pub fn new() -> Self {
            Self {
                fields: vec![],
                arrays: vec![],
            }
        }

        pub fn add<T>(mut self, name: impl Into<String>, data: Vec<T>) -> Self
            where
                T: HasArrowType,
                T::Vector: 'static
        {
            if let Some(array) = self.arrays.first() {
                assert_eq!(array.len(), data.len());
            }

            self.fields.push(T::make_field(name));
            self.arrays.push(T::make_array(data));
            self
        }

        pub fn build(self) -> Result<RecordBatch, ArrowError> {
            RecordBatch::try_new(
                Arc::new(Schema::new(self.fields)),
                self.arrays,
            )
        }
    }
}
