use std::collections::HashMap;
use std::mem;
use std::sync::{Arc, Mutex, OnceLock};
use crossbeam::atomic::AtomicCell;

use crate::utils::index_lookup::IndexLookup;
use crate::utils::limited_rc::LimitedRc;
use crate::utils::once_notify::OnceNotify;

const fn ptr_size_bits() -> usize {
    mem::size_of::<usize>() * 8
}

const fn determine_shard(hash: u64, shard_count: usize) -> usize {
    // The << 7 was copied from the dashmap library
    ((hash << 7) >> (ptr_size_bits() - shard_count.trailing_zeros() as usize)) as usize
}

pub struct Segment {
    lookup: HashMap<u64, (usize, usize)>,
    buffer: Vec<(usize, usize)>,
}

impl Segment {
    fn empty() -> Self {
        Segment {
            lookup: HashMap::new(),
            buffer: vec![(0, 0)],
        }
    }

    fn with_capacity(size: usize) -> Self {
        Self {
            // Estimate that the size of the hash map will be half the size of the number of
            // elements
            lookup: HashMap::with_capacity(size / 2),
            buffer: vec![(0, 0); size + 1],
        }
    }

    fn insert(&mut self, key: u64, local_index: usize, global_index: usize) {
        // We need to add one to the stored index to account
        let adjusted_index = local_index + 1;
        if let Some(existing) = self.lookup.insert(key, (adjusted_index, global_index)) {
            self.buffer[adjusted_index] = existing;
        }
    }
}

struct Shard {
    chunks: boxcar::Vec<Vec<(u64, usize)>>,
}

impl Shard {
    fn new_empty() -> Self {
        Self {
            chunks: boxcar::Vec::new(),
        }
    }

    fn enqueue(&self, vec: Vec<(u64, usize)>) {
        self.chunks.push(vec);
    }

    fn compact(self) -> Segment {
        // TODO this could be done with a From implementation
        let total_size = self.chunks.iter()
            .map(|(_, chunk)| chunk.len())
            .sum();

        let mut segment = Segment::with_capacity(total_size);
        let mut offset = 0;
        for chunk in self.chunks {
            let chunk_length = chunk.len();
            for (local_index, (hash, global_index)) in chunk.into_iter().enumerate() {
                segment.insert(hash, local_index + offset, global_index);
            }
            offset += chunk_length;
        }

        segment
    }
}

pub struct LocalShardListBuilder {
    shard_count: usize,
    compacted_receiver: tokio::sync::broadcast::Receiver<(usize, Arc<Segment>)>,
    receiver: flume::Receiver<(usize, Shard)>,
    compacted_shards_sender: tokio::sync::broadcast::Sender<(usize, Arc<Segment>)>,
}

impl LocalShardListBuilder {
    pub async fn compact_into_read_only_join_map(mut self) -> ReadonlyPartitionedConcurrentJoinMap {
        Self::compact_shards_cooperatively(self.receiver, self.compacted_shards_sender).await;
        let shards = Self::wait_for_completed_shards(self.shard_count, &mut self.compacted_receiver).await;
        ReadonlyPartitionedConcurrentJoinMap { shards }
    }

    async fn compact_shards_cooperatively(receiver: flume::Receiver<(usize, Shard)>, compacted_shards_sender: tokio::sync::broadcast::Sender<(usize, Arc<Segment>)>) {
        // Consume each of the shards to compact and add them to the completed broadcast channel
        while let Ok((index, shard)) = receiver.recv_async().await {
            compacted_shards_sender.send((index, Arc::new(shard.compact())))
                .map_err(|_| "Sending to the broadcast channel should never fail".to_string())
                .unwrap();
        }
        // We must drop the sender here to prevent other threads waiting forever
    }

    async fn wait_for_completed_shards(i: usize, receiver: &mut tokio::sync::broadcast::Receiver<(usize, Arc<Segment>)>) -> Vec<Arc<Segment>> {
        // Each thread consumes all the completed shards from the broadcast channel and stores
        // a local copy
        let mut completed_shards = (0..i).map(|_| Arc::new(Segment::empty())).collect::<Vec<_>>();
        while let Ok((index, shard)) = receiver.recv().await {
            completed_shards[index] = shard;
        }
        completed_shards
    }
}

struct ShardCompactor {
    shards_to_compact_sender: flume::Sender<(usize, Shard)>,
    shards_to_compact_receiver: flume::Receiver<(usize, Shard)>,
    compacted_shards_sender: tokio::sync::broadcast::Sender<(usize, Arc<Segment>)>,
    once_notify: Arc<OnceNotify>,
}

impl ShardCompactor {
    fn create(copies: usize, shards: usize) -> Vec<ShardCompactor> {
        let (sender, receiver) = flume::bounded(shards);
        let broadcaster = tokio::sync::broadcast::Sender::new(shards);
        let once_notify = Arc::new(OnceNotify::new());

        (0..copies)
            .map(|_| Self {
                shards_to_compact_sender: sender.clone(),
                shards_to_compact_receiver: receiver.clone(),
                compacted_shards_sender: broadcaster.clone(),
                once_notify: once_notify.clone(),
            })
            .collect()
    }

    pub async fn distribute_shards(self, shards: LimitedRc<Vec<Shard>>) -> LocalShardListBuilder {
        let shard_count = shards.len();

        // Subscribe to the compacted shards first, so we don't miss any of them
        let compacted_shards_sender = self.compacted_shards_sender;
        let compacted_receiver = compacted_shards_sender.subscribe();

        // Tries to consume the shards ARC variable to send the shards through the distribution
        // channel. If this thread is not the last owner, will wait for the distribution.
        Self::distribute_shards_for_compaction_and_wait(shards, self.shards_to_compact_sender, self.once_notify).await;

        LocalShardListBuilder {
            shard_count,
            compacted_receiver,
            receiver: self.shards_to_compact_receiver,
            compacted_shards_sender
        }
    }

    // pub async fn only_compact_shards(self, shards: LimitedRc<Vec<Shard>>) -> LocalShardListBuilder {
    //     let shard_count = shards.len();
    //
    //     // Subscribe to the compacted shards, so we don't miss any of them
    //     let compacted_shards_sender = self.compacted_shards_sender;
    //     let compacted_receiver = compacted_shards_sender.subscribe();
    //
    //     Self::distribute_shards_for_compaction(shards, self.shards_to_compact_sender);
    //
    //     Self::compact_shards_cooperatively(self.shards_to_compact_receiver, compacted_shards_sender).await;
    //
    //     LocalShardListBuilder {
    //         shard_count,
    //         compacted_receiver,
    //     }
    // }

    pub async fn compact(self, shards: LimitedRc<Vec<Shard>>) -> Vec<Arc<Segment>> {
        let shard_count = shards.len();

        // Subscribe to the compacted shards, so we don't miss any of them
        let compacted_shards_sender = self.compacted_shards_sender;
        let mut compacted_receiver = compacted_shards_sender.subscribe();

        Self::distribute_shards_for_compaction(shards, self.shards_to_compact_sender);

        Self::compact_shards_cooperatively(self.shards_to_compact_receiver, compacted_shards_sender).await;

        Self::build_local_shard_list(shard_count, &mut compacted_receiver).await
    }

    async fn build_local_shard_list(shard_count: usize, compacted_receiver: &mut tokio::sync::broadcast::Receiver<(usize, Arc<Segment>)>) -> Vec<Arc<Segment>> {
        // Each thread consumes all the completed shards from the broadcast channel and stores
        // a local copy
        let mut completed_shards = (0..shard_count).map(|_| Arc::new(Segment::empty())).collect::<Vec<_>>();
        while let Ok((index, shard)) = compacted_receiver.recv().await {
            completed_shards[index] = shard;
        }

        completed_shards
    }

    async fn compact_shards_cooperatively(receiver: flume::Receiver<(usize, Shard)>, compacted_shards_sender: tokio::sync::broadcast::Sender<(usize, Arc<Segment>)>) {
        // Consume each of the shards to compact and add them to the completed broadcast channel
        while let Ok((index, shard)) = receiver.recv_async().await {
            compacted_shards_sender.send((index, Arc::new(shard.compact())))
                .map_err(|_| "Sending to the broadcast channel should never fail".to_string())
                .unwrap();
        }
        // We must drop the sender here to prevent other threads waiting forever
    }

    async fn distribute_shards_for_compaction_and_wait(shards: LimitedRc<Vec<Shard>>, shards_to_compact_sender: flume::Sender<(usize, Shard)>, once_notify: Arc<OnceNotify>) {
        // The last thread distributes the shards via a channel so all threads can do some work
        if let Some(shards) = LimitedRc::into_inner(shards) {
            // Send the notification that the last owner has consumed the shards
            once_notify.notify();

            for (index, shard) in shards.into_iter().enumerate() {
                shards_to_compact_sender.send((index, shard)).unwrap();
            }

            // We must drop the sender afterwards to prevent each thread waiting for infinite shards
            drop(shards_to_compact_sender);
        } else {
            // We must drop the sender afterwards to prevent each thread waiting for infinite shards
            drop(shards_to_compact_sender);

            // Wait for the shards to actually be distributed
            once_notify.wait().await;
        }
    }

    fn distribute_shards_for_compaction(shards: LimitedRc<Vec<Shard>>, shards_to_compact_sender: flume::Sender<(usize, Shard)>) {
        // The last thread distributes the shards via a channel so all threads can do some work
        if let Some(shards) = LimitedRc::into_inner(shards) {
            for (index, shard) in shards.into_iter().enumerate() {
                shards_to_compact_sender.send((index, shard)).unwrap();
            }
        }
        // We must drop the sender afterwards to prevent each thread waiting for infinite shards
    }
}

pub struct WritablePartitionedConcurrentJoinMap {
    global_offset: LimitedRc<Mutex<(usize, usize)>>,
    shards: LimitedRc<Vec<Shard>>,
    shard_compactor: ShardCompactor,
}

impl WritablePartitionedConcurrentJoinMap {
    fn new(
        global_offset: LimitedRc<Mutex<(usize, usize)>>,
        shards: LimitedRc<Vec<Shard>>,
        compactor: ShardCompactor) -> Self {
        Self {
            global_offset,
            shards,
            shard_compactor: compactor,
        }
    }

    pub fn insert_all(&self, hashes: Vec<u64>) -> usize {
        // Increment the global offset to ensure this vector's offsets can be made relative to all
        // previously inserted vectors
        let (index, offset) = {
            let mut global_offset = self.global_offset.lock().unwrap();
            let index = global_offset.0;
            let offset = global_offset.1;
            *global_offset = (index + 1, offset + hashes.len());
            (index, offset)
        };

        // Group into shards
        let mut shard_groups = vec![vec![]; self.shards.len()];
        for (index, hash) in hashes.iter().enumerate() {
            let shard_number = determine_shard(*hash, self.shards.len());
            shard_groups[shard_number].push((*hash, index + offset))
        }

        // Append each group to the shard
        for (shard_number, group) in shard_groups.into_iter().enumerate() {
            self.shards[shard_number].enqueue(group);
        }

        index
    }

    pub async fn consume_and_distribute_shards(self) -> LocalShardListBuilder {
        self.shard_compactor.distribute_shards(self.shards).await
    }

    pub async fn compact(self) -> ReadonlyPartitionedConcurrentJoinMap {
        ReadonlyPartitionedConcurrentJoinMap {
            shards: self.shard_compactor.compact(self.shards).await,
        }
    }
}

pub struct ReadonlyPartitionedConcurrentJoinMap {
    shards: Vec<Arc<Segment>>,
}

impl IndexLookup<u64> for ReadonlyPartitionedConcurrentJoinMap {
    type It<'a> = ReadOnlyJoinMapIterator<'a>;

    fn get_iter<'a>(&'a self, hash: &'a u64) -> Self::It<'a> {
        let shard_number = determine_shard(*hash, self.shards.len());
        let shard = &self.shards[shard_number];
        let (local_index, global_index) = shard.lookup.get(&hash)
            .map(|indexes| *indexes)
            .unwrap_or((0usize, 0usize));
        ReadOnlyJoinMapIterator::new(local_index, global_index, &shard.buffer)
    }
}


pub struct ReadOnlyJoinMapIterator<'a> {
    local_index: usize,
    global_index: usize,
    overflow: &'a [(usize, usize)],
}

impl ReadOnlyJoinMapIterator<'_> {
    fn new(local_index: usize, global_index: usize, overflow: &[(usize, usize)]) -> ReadOnlyJoinMapIterator {
        ReadOnlyJoinMapIterator {
            local_index,
            global_index,
            overflow
        }
    }
}

impl Iterator for ReadOnlyJoinMapIterator<'_> {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        if self.local_index == 0 {
            return None;
        }

        let output = self.global_index;
        (self.local_index, self.global_index) = self.overflow[self.local_index];
        Some(output)
    }
}

// Copied from dashmap
fn default_shard_amount() -> usize {
    static DEFAULT_SHARD_AMOUNT: OnceLock<usize> = OnceLock::new();
    *DEFAULT_SHARD_AMOUNT.get_or_init(|| {
        (std::thread::available_parallelism().map_or(1, usize::from) * 4).next_power_of_two()
    })
}

pub fn create_writable_join_map(count: usize) -> Vec<WritablePartitionedConcurrentJoinMap> {
    let shard_number = default_shard_amount();
    let shards = (0..shard_number).map(|_| Shard::new_empty()).collect();

    // Create a limited number of copies of the shards that will be shared between all join map
    // instances
    let shard_copies = LimitedRc::new_copies(shards, count);

    // Create the compactors which are all linked to each other
    let compactors = ShardCompactor::create(count, shard_number);

    let global_offset = LimitedRc::new_copies(Mutex::new((0usize, 0usize)), count);

    global_offset.into_iter()
        .zip(shard_copies.into_iter())
        .zip(compactors.into_iter())
        .map(|((global_offset, shards), compactor)| WritablePartitionedConcurrentJoinMap::new(global_offset, shards, compactor))
        .collect()
}

#[cfg(test)]
mod tests {
    use futures::stream::FuturesUnordered;
    use futures::StreamExt;
    use rand::random;
    use tokio::spawn;

    use crate::utils::index_lookup::IndexLookup;
    use crate::utils::partitioned_concurrent_join_map::create_writable_join_map;

    #[tokio::test]
    async fn returns_nothing_when_empty() {
        let maps = create_writable_join_map(10);
        let read_only_maps = maps.into_iter()
            .map(|map| spawn(map.compact()))
            .collect::<FuturesUnordered<_>>()
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        for read_only_map in read_only_maps.iter() {
            assert_eq!(read_only_map.get_iter(&1).collect::<Vec<usize>>(), Vec::<usize>::new());
        }
    }

    #[tokio::test]
    async fn all_threads_can_read_keys() {
        let maps = create_writable_join_map(10);
        maps.get(0).unwrap().insert_all(vec![1, 2, 1, 2]);

        let read_only_maps = maps.into_iter()
            .map(|map| spawn(map.compact()))
            .collect::<FuturesUnordered<_>>()
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        for read_only_map in read_only_maps.iter() {
            assert_eq!(read_only_map.get_iter(&1).collect::<Vec<usize>>(), vec![2, 0]);
            assert_eq!(read_only_map.get_iter(&2).collect::<Vec<usize>>(), vec![3, 1]);
        }
    }

    #[tokio::test]
    async fn can_follow_chains_across_blocks() {
        let maps = create_writable_join_map(10);
        maps.get(0).unwrap().insert_all(vec![1, 2, 1, 2]);
        maps.get(0).unwrap().insert_all(vec![3, 2, 1, 3]);

        let read_only_maps = maps.into_iter()
            .map(|map| spawn(map.compact()))
            .collect::<FuturesUnordered<_>>()
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        for read_only_map in read_only_maps.iter() {
            assert_eq!(read_only_map.get_iter(&1).collect::<Vec<usize>>(), vec![6, 2, 0]);
            assert_eq!(read_only_map.get_iter(&2).collect::<Vec<usize>>(), vec![5, 3, 1]);
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn builds_in_parallel() {
        let maps = create_writable_join_map(10);
        for i in 0..32 {
            let mut vec = vec![];
            for j in 0..64 {
                vec.push(random::<u64>());
            }
            maps.get(0).unwrap().insert_all(vec);
        }

        let read_only_maps = maps.into_iter()
            .map(|map| spawn(map.compact()))
            .collect::<FuturesUnordered<_>>()
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
    }
}
