use crate::utils::index_lookup::IndexLookup;
use boxcar;
use dashmap;
use dashmap::{DashMap, Map, ReadOnlyView, SharedValue};
use std::hash::{BuildHasher, Hash};
use std::io::Read;
use std::mem;
use std::ops::DerefMut;
use std::sync::{Arc, Mutex};
use crate::utils::bypass_hasher::BypassHasher;

#[derive(Debug)]
struct ConcurrentJoinOverflowBuffer {
    value_count: Mutex<usize>,
    vec: boxcar::Vec<Mutex<Vec<usize>>>,
}

impl ConcurrentJoinOverflowBuffer {
    fn new() -> ConcurrentJoinOverflowBuffer {
        ConcurrentJoinOverflowBuffer {
            value_count: Mutex::new(0),
            // We need to store an initial unit vector to represent the end of each chain. All
            // indices are offset by 1, so we can differentiate between occupied and unoccupied
            // cells in the buffer. The initial unit vector ensures that nothing will actually be
            // stored at index 0 as it will never be read.
            vec: boxcar::vec![Mutex::new(vec![0usize])],
        }
    }

    fn create_new_buffer<F>(&self, buffer_size: usize, buffer_writer: F) -> usize
    where F: FnOnce(&usize, &mut [usize]) {
        // Increment the global size counter and insert the new buffer while holding the mutex
        let new_buffer = Mutex::new(vec![0; buffer_size]);
        let (buffer_index, offset) = self.append_buffer(buffer_size, new_buffer);

        {
            // We have to then immediately look up the value as the first buffer was already consumed
            let mut buffer = self.vec.get(buffer_index).unwrap().lock().unwrap();

            // Pass the locked buffer to the writer
            buffer_writer(&offset, &mut *buffer);

            // Lock is implicitly released once it goes out of scope
        }

        buffer_index
    }

    fn append_buffer(&self, buffer_size: usize, new_buffer: Mutex<Vec<usize>>) -> (usize, usize) {
        let mut total_size = self.value_count.lock().unwrap();
        let offset = *total_size;
        *total_size += buffer_size;
        (self.vec.push(new_buffer), offset)
    }

    fn compact(&self) -> Vec<usize> {
        let total_size = self.value_count.lock().unwrap();
        let mut output = Vec::<usize>::with_capacity(*total_size);

        for (_, block) in &self.vec {
            output.append(block.lock().unwrap().deref_mut());
        }

        output
    }
}

pub struct Inserter<'a> {
    offset: &'a usize,
    map: &'a DashMap<u64, usize, BypassHasher>,
    buffer: &'a mut [usize],
}

impl <'a> Inserter<'a> {
    fn new(
        offset: &'a usize,
        map: &'a DashMap<u64, usize, BypassHasher>,
        buffer: &'a mut [usize]
    ) -> Self {
        Self { offset, map, buffer }
    }

    pub fn insert(&mut self, key: u64, index: usize) {
        // Before storing the index in the map, we need to add the offset to make the index relative
        // to the global list, and an additional 1 to account for the initial zero buffer
        if let Some(existing) = self.map.insert(key, index + self.offset + 1) {
            // In the local buffer we used the original index
            self.buffer[index] = existing;
        }
    }

    pub fn insert_all(&mut self, keys: Vec<u64>) {
        let mut shard_groups = vec![vec![]; self.map._shard_count()];
        for (index, key) in keys.into_iter().enumerate() {
            shard_groups[self.map.determine_shard(key as usize)].push((key, index))
        }

        for (shard_num, shard_group) in shard_groups.into_iter().enumerate() {
            if shard_group.len() > 0 {
                let shard = &self.map.shards()[shard_num];
                let mut locked_shard = shard.write();

                // Write all the values in this group to the locked shard at once
                for (key, index) in shard_group {
                    let stored_index = index + self.offset + 1;

                    // This code is copied from the private methods of the hash map
                    match locked_shard.find_or_find_insert_slot(
                        key,
                        |(k, _v)| k == &key,
                        |(k, _v)| *k,
                    ) {
                        Ok(elem) => {
                            let existing = mem::replace(unsafe { elem.as_mut().1.get_mut() }, stored_index);
                            self.buffer[index] = existing;
                        },
                        Err(slot) => unsafe {
                            locked_shard.insert_in_slot(key, slot, (key, SharedValue::new(stored_index)));
                        },
                    }
                }
            }
        }
    }
}

#[derive(Debug)]
pub struct ConcurrentSelfHashJoinMap {
    map: DashMap<u64, usize, BypassHasher>,
    buffers: ConcurrentJoinOverflowBuffer,
}

impl ConcurrentSelfHashJoinMap {
    pub fn new() -> Self {
        Self {
            map: DashMap::with_hasher_and_shard_amount(BypassHasher, 32),
            buffers: ConcurrentJoinOverflowBuffer::new(),
        }
    }

    pub fn append_block<F>(&self, size: usize, with_inserter: F) -> usize
    where F: FnOnce(&mut Inserter) {
        self.buffers.create_new_buffer(size, |offset, buffer| {
            let mut inserter = Inserter::new(offset, &self.map, buffer);
            with_inserter(&mut inserter)
        })
    }

    pub fn compact(self) -> ReadOnlyJoinMap {
        ReadOnlyJoinMap::new(self.map.into_read_only(), self.buffers.compact())
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

#[cfg(test)]
mod tests {
    use crate::utils::concurrent_self_hash_join_map::ConcurrentSelfHashJoinMap;

    #[test]
    fn returns_empty_vecs_when_empty() {
        let join_map = ConcurrentSelfHashJoinMap::new();
        let readonly = join_map.compact();
        let expected: Vec<usize> = vec![];
        assert_eq!(readonly.get_all(&123), expected);
    }

    #[test]
    fn follows_chains_of_indexes() {
        let pairs = vec![(1u64, vec![1, 4, 3]), (2u64, vec![2, 7])];

        // Build the join map from the reversed indices
        let join_map = ConcurrentSelfHashJoinMap::new();
        join_map.append_block(10, |inserter| {
            for (key, indices) in &pairs {
                for index in indices.iter().rev() {
                    inserter.insert(*key, *index)
                }
            }
        });

        let readonly = join_map.compact();

        for (key, indices) in pairs {
            let actual: Vec<usize> = readonly.get_all(&key);
            assert_eq!(actual, indices);
        }
    }

    #[test]
    fn follows_a_chain_with_a_zero() {
        let (key, indices) = (1u64, vec![1, 0, 3]);

        // Build the join map from the reversed indices
        let join_map = ConcurrentSelfHashJoinMap::new();
        join_map.append_block(10, |inserter| {
            for index in indices.iter().rev() {
                inserter.insert(key, *index);
            }
        });

        let readonly = join_map.compact();

        let actual: Vec<usize> = readonly.get_all(&key);
        assert_eq!(actual, indices);
    }

    #[test]
    fn follows_a_chain_matching_last_element() {
        let (key, indices) = (1u64, vec![1, 9, 3]);

        // Build the join map from the reversed indices
        let join_map = ConcurrentSelfHashJoinMap::new();
        join_map.append_block(10, |inserter| {
            for index in indices.iter().rev() {
                inserter.insert(key, *index);
            }
        });

        let readonly = join_map.compact();

        let actual: Vec<usize> = readonly.get_all(&key);
        assert_eq!(actual, indices);
    }

    #[test]
    fn follows_chains_spanning_multiple_blocks() {
        let join_map = ConcurrentSelfHashJoinMap::new();

        // Create two rendezvous channels to ensure that the operations are interleaved
        let (a_done_sender, a_done_receiver) = flume::bounded::<()>(0);
        let (b_done_sender, b_done_receiver) = flume::bounded::<()>(0);

        // Build the join map in two parallel threads
        std::thread::scope(|scope| {
            scope.spawn(|| {
                join_map.append_block(10, |inserter| {
                    // Insert the first key immediately as B is already waiting
                    inserter.insert(1, 2);
                    a_done_sender.send(()).unwrap();

                    // Wait for b to insert the key
                    b_done_receiver.recv().unwrap();
                    inserter.insert(1, 4);
                    a_done_sender.send(()).unwrap();

                    b_done_receiver.recv().unwrap();
                    inserter.insert(1, 6);
                    a_done_sender.send(()).unwrap();
                });
            });

            scope.spawn(|| {
                // Wait for A to insert the first key
                a_done_receiver.recv().unwrap();

                join_map.append_block(10, |inserter| {
                    inserter.insert(1, 3);
                    b_done_sender.send(()).unwrap();

                    // Wait for A to insert the key
                    a_done_receiver.recv().unwrap();
                    inserter.insert(1, 5);
                    b_done_sender.send(()).unwrap();

                    a_done_receiver.recv().unwrap();
                    inserter.insert(1, 7);
                });
            });
        });

        let readonly = join_map.compact();

        // The result should include the indices from both threads even though they were contained
        // in different blocks. The second block values are offset by 10 due to the size of the
        // first buffer
        assert_eq!(readonly.get_all(&1), vec![17, 6, 15, 4, 13, 2]);
    }
}
