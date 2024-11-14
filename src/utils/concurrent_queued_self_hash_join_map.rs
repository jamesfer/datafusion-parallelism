use crate::utils::bypass_hasher::BypassHasher;
use crate::utils::index_lookup::IndexLookup;
use crate::utils::limited_rc::LimitedRc;
use crate::utils::self_hash_map_types::{new_self_dash_map, SelfHashDashMap};
use boxcar;
use crossbeam::atomic::AtomicCell;
use dashmap;
use dashmap::{Map, ReadOnlyView, SharedValue};
use datafusion_common::DataFusionError;
use futures::SinkExt;
use std::collections::{HashMap, VecDeque};
use std::hash::{BuildHasher, Hash};
use std::io::Read;
use std::mem;
use std::ops::DerefMut;
use std::sync::{Arc, Mutex, OnceLock};
use tokio::sync::broadcast::{Receiver, Sender};

#[derive(Debug)]
struct ConcurrentJoinOverflowBuffer {
    value_count: Mutex<usize>,
    // TODO this could be a regular vec
    vec: boxcar::Vec<Arc<OnceLock<Vec<usize>>>>,
}

impl ConcurrentJoinOverflowBuffer {
    fn new() -> ConcurrentJoinOverflowBuffer {
        ConcurrentJoinOverflowBuffer {
            value_count: Mutex::new(0),
            vec: boxcar::vec![{
                // We need to store an initial unit vector to represent the end of each chain. All
                // indices are offset by 1, so we can differentiate between occupied and unoccupied
                // cells in the buffer. The initial unit vector ensures that nothing will actually be
                // stored at index 0 as it will never be read.
                let cell = OnceLock::new();
                cell.set(vec![0usize])
                    .expect("Failed to set initial buffer");
                Arc::new(cell)
            }],
        }
    }

    fn create_new_buffer_cell(&self, buffer_size: usize) -> (usize, usize, Arc<OnceLock<Vec<usize>>>) {
        // Increment the global size counter and insert the new buffer while holding the mutex
        let cell = Arc::new(OnceLock::new());
        let (buffer_index, offset) = self.append_cell(buffer_size, cell.clone());
        (buffer_index, offset, cell)
    }

    fn append_cell(&self, buffer_size: usize, cell: Arc<OnceLock<Vec<usize>>>) -> (usize, usize) {
        // Update the current size of the buffer behind the lock
        let mut total_size = self.value_count.lock().unwrap();
        let offset = *total_size;
        *total_size += buffer_size;

        // Append the cell while holding the count lock
        (self.vec.push(cell), offset)
    }

    fn compact(&self) -> Vec<usize> {
        let total_size = self.value_count.lock().unwrap();
        let mut output = Vec::<usize>::with_capacity(*total_size);

        for (index, block) in self.vec.iter() {
            // output.append(&mut block.take().expect("Buffer was not returned to cell before compact was called"));
            output.extend_from_slice(block.get()
                .unwrap_or_else(|| panic!("Buffer at index {} was not returned to cell before compact was called", index)));
        }

        output
    }
}

#[derive(Debug, Clone)]
struct HashIndexPair {
    hash: u64,
    index: usize,
}

#[derive(Debug)]
struct QueuedChunk {
    data: Vec<HashIndexPair>,
    offset: usize,
}

#[derive(Debug)]
struct QueuedShardContents {
    chunk: QueuedChunk,
    buffer_number: usize,
}

#[derive(Debug)]
struct QueuedBuffer {
    references: usize,
    buffer: Vec<usize>,
    destination_cell: Arc<OnceLock<Vec<usize>>>,
}

struct QueueWriter<'a> {
    buffer_number: usize,
    enqueued: usize,
    shard_contents: &'a mut Vec<VecDeque<QueuedShardContents>>,
}

impl <'a> QueueWriter<'a> {
    pub fn write(&mut self, shard_number: usize, chunk: QueuedChunk) {
        self.shard_contents[shard_number].push_back(QueuedShardContents {
            buffer_number: self.buffer_number,
            chunk,
        });
        self.enqueued += 1;
    }
}

struct BufferQueue {
    buffer: Vec<usize>,
    destination_cell: Arc<OnceLock<Vec<usize>>>,
    queue: Vec<(usize, QueuedChunk)>,
}

impl BufferQueue {
    pub fn new(buffer: Vec<usize>, destination_cell: Arc<OnceLock<Vec<usize>>>) -> Self {
        Self { buffer, destination_cell, queue: vec![] }
    }

    pub fn enqueue(&mut self, shard_number: usize, chunk: QueuedChunk) {
        self.queue.push((shard_number, chunk));
    }
}

#[derive(Debug)]
struct ShardQueue {
    next_buffer_number: usize,
    shard_contents: Vec<VecDeque<QueuedShardContents>>,
    queued_buffers: HashMap<usize, QueuedBuffer>,
}

impl ShardQueue {
    pub fn new(shard_count: usize) -> Self {
        Self {
            next_buffer_number: 0,
            shard_contents: (0..shard_count).into_iter().map(|_| VecDeque::new()).collect(),
            queued_buffers: HashMap::new(),
        }
    }

    pub fn buffer_queue_length(&self) -> usize {
        self.queued_buffers.len()
    }

    // pub fn print(&self) {
    //     for b in self.queued_buffers.iter() {
    //         println!("Buffer number: {}, references: {}", b.0, b.1.references);
    //     }
    // }

    pub fn add_buffer(&mut self, buffer_queue: BufferQueue, buffer_index: usize) {
        // If any of the shards referencing the buffer were queued, add the buffer to the internal
        // map
        if buffer_queue.queue.len() > 0 {
            // Find the number used to identify this buffer
            let buffer_number = self.next_buffer_number;
            self.next_buffer_number += 1;

            let references = buffer_queue.queue.len();

            // Add each of the chunks to the matching shard queue
            for (shard_number, chunk) in buffer_queue.queue {
                self.shard_contents[shard_number].push_back(QueuedShardContents {
                    buffer_number,
                    chunk,
                });
            }

            // Finally, add the buffer to the separate buffer queue
            self.queued_buffers.insert(buffer_number, QueuedBuffer {
                buffer: buffer_queue.buffer,
                destination_cell: buffer_queue.destination_cell,
                references,
            })
                .map(|_existing| panic!("Buffer number {} already exists in the queue", buffer_number));
        } else {
            // If no chunks were enqueued, we can immediately pass the buffer to the destination cell
            buffer_queue.destination_cell.set(buffer_queue.buffer)
                .expect("Buffer destination cell already set");
        }
    }

    // pub fn enqueue_contents_for_buffer<F>(
    //     &mut self,
    //     mut buffer: Vec<usize>,
    //     destination_cell: Arc<OnceCell<Vec<usize>>>,
    //     operation: F,
    // )
    // where
    //     F: FnOnce(&mut Vec<usize>, &mut QueueWriter)
    // {
    //     // Find the number used to identify this buffer
    //     let buffer_number = self.next_buffer_number;
    //
    //     // Run the callback to see how many chunks where enqueued
    //     let mut writer = QueueWriter {
    //         buffer_number,
    //         enqueued: 0,
    //         shard_contents: &mut self.shard_contents,
    //     };
    //     operation(&mut buffer, &mut writer);
    //
    //     // Enqueue the buffer so we can refer to it later
    //     if writer.enqueued > 0 {
    //         self.queued_buffers.insert(buffer_number, QueuedBuffer {
    //             buffer,
    //             destination_cell,
    //             references: writer.enqueued,
    //         })
    //             .map(|_existing| panic!("Buffer number {} already exists in the queue", buffer_number));
    //
    //         // Increment the buffer number to prevent duplicates
    //         self.next_buffer_number += 1;
    //     } else {
    //         // If no chunks were enqueued, we can immediately pass the buffer to the destination cell
    //         destination_cell.set(buffer)
    //             .expect("Buffer destination cell already set");
    //     }
    // }

    pub fn with_queued_shard_chunks<F>(&mut self, shard_number: usize, mut operation: F)
    where
        F: FnMut(QueuedChunk, &mut Vec<usize>)
    {
        for shard_content in self.shard_contents[shard_number].drain(..) {
            Self::consume_shard_chunk(shard_content, &mut self.queued_buffers, &mut operation);
        }
    }

    fn consume_shard_chunk<F>(
        shard_content: QueuedShardContents,
        queued_buffers: &mut HashMap<usize, QueuedBuffer>,
        operation: &mut F,
    )
    where
        F: FnMut(QueuedChunk, &mut Vec<usize>)
    {
        // Run the operation function with the shard content and the matching buffer inside a
        // block to limit the scope of the reference
        let remaining_references = {
            let mut queued_buffer = queued_buffers.get_mut(&shard_content.buffer_number)
                .expect("Buffer number not found in the queue");
            operation(shard_content.chunk, &mut queued_buffer.buffer);
            queued_buffer.references -= 1;
            queued_buffer.references
        };

        // If there are no more references, pass the buffer to the destination cell
        if remaining_references == 0 {
            let queued_buffer = queued_buffers.remove(&shard_content.buffer_number)
                .expect("Buffer somehow removed from the queue");
            queued_buffer.destination_cell.set(queued_buffer.buffer)
                .expect("Buffer destination cell already set");
        }
    }

}

pub struct ConcurrentQueuedSelfHashJoinMapInstance {
    map: LimitedRc<SelfHashDashMap<usize>>,
    // Stores chunks of data that were not able to be immediately written to the map
    shard_queue: ShardQueue,
    buffers: Arc<ConcurrentJoinOverflowBuffer>,

    compacted_join_map_sender: Arc<AtomicCell<Option<Sender<Arc<ReadOnlyJoinMap>>>>>,
    compacted_join_map_receiver: Receiver<Arc<ReadOnlyJoinMap>>,
}

impl ConcurrentQueuedSelfHashJoinMapInstance {
    pub fn new(
        map: LimitedRc<SelfHashDashMap<usize>>,
        buffers: Arc<ConcurrentJoinOverflowBuffer>,
        compacted_join_map_sender: Arc<AtomicCell<Option<Sender<Arc<ReadOnlyJoinMap>>>>>,
        compacted_join_map_receiver: Receiver<Arc<ReadOnlyJoinMap>>
    ) -> Self {
        let shard_queue = ShardQueue::new(map.shards().len());
        Self {
            map,
            buffers,
            compacted_join_map_sender,
            compacted_join_map_receiver,
            shard_queue,
        }
    }

    pub async fn compact(mut self) -> Arc<ReadOnlyJoinMap> {
        // Finish processing all items in the queue
        for (shard_number, shard) in self.map.shards().iter().enumerate() {
            let mut locked_shard_map = None;
            self.shard_queue.with_queued_shard_chunks(shard_number, |chunk, buffer| {
                // Attempt the lock the shard
                let map = locked_shard_map.get_or_insert_with(|| shard.write());

                // Write all the values in this group to the locked shard at once
                for HashIndexPair { hash, index } in chunk.data {
                    Self::write_key(buffer, map, hash, index, chunk.offset);
                }
            });
        }

        assert_eq!(self.shard_queue.buffer_queue_length(), 0);

        match LimitedRc::into_inner(self.map) {
            None => {
                // Wait for another thread to perform the compaction
                self.compacted_join_map_receiver.recv().await
                    .map_err(|err| DataFusionError::Internal(format!("Receive error while waiting for compacted join map {:?}", err)))
                    .unwrap()
            }
            Some(map) => {
                let sender = self.compacted_join_map_sender.take()
                    .expect("Compacted join map sender already taken by another thread");
                let read_only_map = Arc::new(ReadOnlyJoinMap::new(map.into_read_only(), self.buffers.compact()));

                // Share the map with the other threads
                sender.send(read_only_map.clone())
                    .map_err(|err| DataFusionError::Internal("Send error while sending compacted join map".to_string()))
                    .unwrap();
                read_only_map
            }
        }
    }

    pub fn insert_all(&mut self, keys: Vec<u64>) -> usize {
        let (buffer_index, offset, destination_cell) = self.buffers.create_new_buffer_cell(keys.len());
        let buffer = vec![0; keys.len()];
        self.write_all_entries(keys, offset, buffer, destination_cell, buffer_index);
        buffer_index
    }

    fn write_all_entries(
        &mut self,
        keys: Vec<u64>,
        offset: usize,
        buffer: Vec<usize>,
        destination_cell: Arc<OnceLock<Vec<usize>>>,
        buffer_index: usize,
    ) {
        // Group the keys by shard
        let mut shard_groups = vec![vec![]; self.map._shard_count()];
        for (index, key) in keys.into_iter().enumerate() {
            shard_groups[self.map.determine_shard(key as usize)].push(HashIndexPair{ hash: key, index })
        }

        // Enqueue the buffer to be processed later if needed. If we never enqueue a chunk, the
        // buffer is not enqueued
        let mut q = BufferQueue::new(
            buffer,
            destination_cell,
        );
        for (shard_num, shard_group) in shard_groups.into_iter().enumerate() {
            if shard_group.len() > 0 {
                self.write_entries_to_shard(shard_num, shard_group, offset, &mut q);
            }
        }
        self.shard_queue.add_buffer(q, buffer_index);
    }

    fn write_entries_to_shard(
        &mut self,
        shard_num: usize,
        shard_group: Vec<HashIndexPair>,
        offset: usize,
        buffer_queue: &mut BufferQueue,
    ) {
        let shard = &self.map.shards()[shard_num];

        // Attempt the lock the shard if it is available
        match shard.try_write() {
            None => {
                // Enqueue the work for later
                buffer_queue.enqueue(shard_num, QueuedChunk {
                    offset,
                    data: shard_group,
                });
            }
            Some(mut guard) => {
                let shard_map = guard.deref_mut();

                // Write all the values in this group to the locked shard at once
                Self::write_all_keys(shard_group, offset, &mut buffer_queue.buffer, shard_map);

                // Write all the queued values
                self.shard_queue.with_queued_shard_chunks(shard_num, |queued_chunk, queued_buffer| {
                    // Write all the values in this group to the locked shard at once
                    Self::write_all_keys(queued_chunk.data, queued_chunk.offset, queued_buffer, shard_map);
                });
            }
        };
    }

    fn write_all_keys(
        shard_group: Vec<HashIndexPair>,
        offset: usize,
        buffer: &mut Vec<usize>,
        shard_map: &mut hashbrown::raw::RawTable<(u64, SharedValue<usize>)>,
    ) {
        for HashIndexPair { hash, index } in shard_group {
            Self::write_key(buffer, shard_map, hash, index, offset);
        }
    }

    fn write_key(
        buffer: &mut Vec<usize>,
        mut shard_map: &mut hashbrown::raw::RawTable<(u64, SharedValue<usize>)>,
        key: u64,
        index: usize,
        offset: usize,
    ) {
        let stored_index = index + offset + 1;

        // This code is copied from the private methods of the hash map
        match shard_map.find_or_find_insert_slot(
            key,
            |(k, _v)| *k == key,
            |(k, _v)| *k,
        ) {
            Ok(elem) => {
                let existing = mem::replace(unsafe { elem.as_mut().1.get_mut() }, stored_index);
                buffer[index] = existing;
            },
            Err(slot) => unsafe {
                shard_map.insert_in_slot(key, slot, (key, SharedValue::new(stored_index)));
            },
        }
    }
}

#[derive(Clone)]
pub struct ReadOnlyJoinMap {
    map: ReadOnlyView<u64, usize, BypassHasher>,
    overflow: Vec<usize>,
}

impl ReadOnlyJoinMap {
    pub fn new(map: ReadOnlyView<u64, usize, BypassHasher>, overflow: Vec<usize>) -> ReadOnlyJoinMap {
        ReadOnlyJoinMap { map, overflow }
    }

    pub fn get_all(&self, key: &u64) -> Vec<usize> {
        match self.map.get(key) {
            None => vec![],
            Some(index) => {
                let mut output = vec![index - 1];

                let mut next_index = self.overflow[*index];
                while next_index > 0 {
                    output.push(next_index - 1);
                    next_index = self.overflow[next_index];
                }

                output
            }
        }
    }

    pub fn contains(&self, key: &u64) -> bool {
        self.map.contains_key(key)
    }

    fn get_overflow(&self) -> &Vec<usize> {
        &self.overflow
    }

    fn get_entries(&self) -> Vec<(&u64, &usize)> {
        self.map.iter().collect()
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn entry_count(&self) -> usize {
        self.map.len() + self.overflow.iter().filter(|i| **i != 0usize).count()
    }
}

impl IndexLookup<u64> for ReadOnlyJoinMap {
    type It<'a> = ReadOnlyJoinMapIterator<'a>;

    fn get_iter<'a>(&'a self, key: &'a u64) -> Self::It<'a> {
        let starting_index = self.map.get(key).map(|index| *index).unwrap_or(0usize);
        // println!("Concurrent raw, key: {}, starting index: {}, map value: {:?}, map size: {}, map keys: {:?}", key, starting_index, self.map.get(key), self.map.len(), self.map.keys().collect::<Vec<_>>());
        ReadOnlyJoinMapIterator::new(starting_index, &self.overflow)
    }
}

impl  IndexLookup<u64> for Arc<ReadOnlyJoinMap> {
    type It<'a> = ReadOnlyJoinMapIterator<'a>;

    fn get_iter<'a>(&'a self, key: &'a u64) -> Self::It<'a> {
        let starting_index = self.map.get(key).map(|index| *index).unwrap_or(0usize);
        // println!("Concurrent raw, key: {}, starting index: {}, map value: {:?}, map size: {}, map keys: {:?}", key, starting_index, self.map.get(key), self.map.len(), self.map.keys().collect::<Vec<_>>());
        ReadOnlyJoinMapIterator::new(starting_index, &self.overflow)
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

pub fn make_concurrent_queued_self_hash_join_map(instance_count: usize) -> Vec<ConcurrentQueuedSelfHashJoinMapInstance> {
    let maps = LimitedRc::new_copies(new_self_dash_map(), instance_count);
    let buffers = Arc::new(ConcurrentJoinOverflowBuffer::new());

    let sender = Sender::new(1);
    let sender_cell = Arc::new(AtomicCell::new(Some(sender.clone())));
    let receivers = (0..instance_count).map(|_| sender.subscribe());
    maps.into_iter()
        .zip(receivers)
        .map(|(map, receiver)| ConcurrentQueuedSelfHashJoinMapInstance::new(
            map,
            buffers.clone(),
            sender_cell.clone(),
            receiver,
        ))
        .collect()
}

#[cfg(test)]
mod tests {
    use crate::utils::concurrent_queued_self_hash_join_map::make_concurrent_queued_self_hash_join_map;
    use futures::stream::FuturesOrdered;
    use futures::StreamExt;

    #[tokio::test]
    async fn returns_empty_vecs_when_empty() {
        let join_maps = make_concurrent_queued_self_hash_join_map(4);
        let compacted_maps = join_maps.into_iter()
            .map(|map| map.compact())
            .collect::<FuturesOrdered<_>>()
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Vec<_>>();
        for readonly in compacted_maps {
            let expected: Vec<usize> = vec![];
            assert_eq!(readonly.get_all(&123), expected);
        }
    }

    #[tokio::test]
    async fn follows_chains_of_indexes() {
        let pairs = vec![(1u64, vec![1, 4, 3]), (2u64, vec![2, 7])];

        // Build the join map from the reversed indices
        let mut join_maps = make_concurrent_queued_self_hash_join_map(4);
        let mut join_map = join_maps.get_mut(0).unwrap();
        join_map.insert_all(vec![1, 2, 2, 1, 1, 4]);
        join_map.insert_all(vec![4, 2, 1]);

        let compacted_maps = join_maps.into_iter()
            .map(|map| map.compact())
            .collect::<FuturesOrdered<_>>()
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Vec<_>>();

        for readonly in compacted_maps {
            // Indices are reversed
            assert_eq!(readonly.get_all(&1), vec![8, 4, 3, 0]);
            assert_eq!(readonly.get_all(&2), vec![7, 2, 1]);
            assert_eq!(readonly.get_all(&4), vec![6, 5]);
        }
    }

    #[tokio::test]
    async fn follows_a_chain_with_a_zero() {
        // Build the join map from the reversed indices
        let mut join_maps = make_concurrent_queued_self_hash_join_map(4);
        let mut join_map = join_maps.get_mut(0).unwrap();
        join_map.insert_all(vec![1, 1, 3, 1]);

        let compacted_maps = join_maps.into_iter()
            .map(|map| map.compact())
            .collect::<FuturesOrdered<_>>()
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Vec<_>>();

        for readonly in compacted_maps {
            // Indices are reversed
            assert_eq!(readonly.get_all(&1), vec![3, 1, 0]);
        }
    }

    #[tokio::test]
    async fn follows_chains_spanning_multiple_maps() {
        let mut join_maps = make_concurrent_queued_self_hash_join_map(4);

        for join_map in join_maps.iter_mut() {
            join_map.insert_all(vec![1, 2, 1]);
        }

        for join_map in join_maps.iter_mut() {
            join_map.insert_all(vec![3, 2, 1]);
        }

        let compacted_maps = join_maps.into_iter()
            .map(|map| map.compact())
            .collect::<FuturesOrdered<_>>()
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Vec<_>>();

        for readonly in compacted_maps {
            assert_eq!(
                readonly.get_all(&1),
                vec![23, 20, 17, 14, 11, 9, 8, 6, 5, 3, 2, 0],
            );
        }
    }
}
