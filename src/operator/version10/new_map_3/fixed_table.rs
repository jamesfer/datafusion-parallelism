use std::alloc::{alloc_zeroed, Layout};
use std::cell::UnsafeCell;
use std::cmp::max;
use std::fmt::{Display, Formatter};
use std::mem::MaybeUninit;
use std::ptr::{slice_from_raw_parts_mut, NonNull};
use std::slice::{from_raw_parts, from_raw_parts_mut};
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicU8, Ordering};
use crate::operator::version10::new_map_3::atomic::{AsAtomic, AtomicOps};
use crate::operator::version10::new_map_3::group::{BulkGroupStrategy, BulkGroupStrategy32, BulkGroupStrategyN, GroupStrategy, IterableGroupStrategy};
use crate::operator::version10::new_map_3::probe_sequence::{ProbeSequence, ProbeSequenceBulk, ProbeSequenceBulkN};

const N: usize = 8;

// trait MaybeDisplay {
//     fn maybe_display(&self) -> String;
// }
//
// impl<T> MaybeDisplay for T {
//     default fn maybe_display(&self) -> String {
//        "_undisplayable_".to_string()
//     }
// }
//
// impl<T: Display> MaybeDisplay for T {
//     fn maybe_display(&self) -> String {
//         format!("{}", self)
//     }
// }
//
// trait CanDisplay<V> {
//     fn display(&self, v: &V) -> String;
// }
//
//
// struct S<'a, V>(&'a V);
//
// impl <'a, V: Display> S<'a, V> {
//     fn display(&self) -> String {
//         format!("{}", self.0)
//     }
// }
//
// trait NonDisplay {
//     fn display(&self) -> String;
// }
//
// impl <'a, V> NonDisplay for S<'a, V> {
//     fn display(&self) -> String {
//         "_undisplayable_".to_string()
//     }
// }

const OVERFLOW_BIT_MASK: u64 = 1 << 63;
const TOP_8_BITS_MASK: u64 = u64::MAX << 56;
const LESS_15: u64 = OVERFLOW_BIT_MASK - 15;
const IGNORE_TOP_7_MASK: u64 = 0x1FFF_FFFF_FFFF_FFFF;
const IGNORE_TOP_8_MASK: u64 = 0x0FFF_FFFF_FFFF_FFFF;
const HASH_OCCUPIED_BIT: u64 = 1 << 63;
// const HASH_OCCUPIED_BIT: u64 = 0x1000_0000_0000_0000;

struct Inner<V, G> {
    bucket_mask: usize,
    // Maybe one day this could be optimised to avoid using two separate pointers
    // data: Box<[(u64, V)]>,
    // tags: Box<[u8]>,
    memory: NonNull<u8>,
    memory_size: usize,
    tags_end: usize,
    data_start: usize,
    pub max_insert_attemps: usize,
    phantom: std::marker::PhantomData<(V, G)>,
}

impl <V, G> Drop for Inner<V, G> {
    fn drop(&mut self) {
        let slice_ptr = unsafe { slice_from_raw_parts_mut(self.memory.as_ptr(), self.memory_size) };
        drop(unsafe { Box::from_raw(slice_ptr) });
        // Pointer is now dangling, but the struct should immediately be dropped
    }
}

// impl <V, G> CanDisplay<V> for Inner<V, G> {
//     default fn display(&self, v: &V) -> String {
//         "_undisplayable_".to_string()
//     }
// }
//
// impl <V: Display, G> CanDisplay<V> for Inner<V, G> {
//     fn display(&self, v: &V) -> String {
//         format!("{}", v)
//     }
// }

// impl <V, G> Inner<V, G> {
//     default fn display(&self, v: V) -> String {
//         "_undisplayable_".to_string()
//     }
// }
//
// impl <V, G> Inner<V, G> {
//     default fn display(&self, v: V) -> String {
//         "_undisplayable_".to_string()
//     }
// }

impl <V, G> Inner<V, G>
where
    V: AsAtomic + Default + PartialEq + Clone,
    G: GroupStrategy
{
    const GROUP_MEMORY_WIDTH: usize = G::GROUP_SIZE * size_of::<u8>();

    pub fn new(capacity: usize) -> Self {
        assert!(capacity.is_power_of_two());
        assert!((capacity / G::GROUP_SIZE).is_power_of_two());

        let data_layout = Layout::new::<(u64, V)>();
        let align = max(data_layout.align(), Self::GROUP_MEMORY_WIDTH);
        let buckets_size = capacity * data_layout.size();
        let tags_size = (capacity + G::GROUP_SIZE) * size_of::<u8>();
        let data_start = tags_size.next_multiple_of(align);

        let layout = Layout::from_size_align(data_start + buckets_size, align).unwrap();
        let ptr = NonNull::new(unsafe { alloc_zeroed(layout) }).expect("Alloc failed");

        let mut inner = Self {
            bucket_mask: capacity - 1,
            memory: ptr,
            memory_size: layout.size(),
            tags_end: tags_size,
            data_start,
            phantom: std::marker::PhantomData,
            max_insert_attemps: capacity / G::GROUP_SIZE,
        };
        if G::GROUP_SIZE != 0 {
            inner.tags_mut().fill(G::EMPTY_TAG);
        }
        inner
    }

    #[inline]
    fn memory_as_slice(&self) -> &[u8] {
        unsafe { &*slice_from_raw_parts_mut(self.memory.as_ptr(), self.memory_size) }
    }

    #[inline]
    fn memory_as_mut_slice(&mut self) -> &mut [u8] {
        unsafe { &mut *slice_from_raw_parts_mut(self.memory.as_ptr(), self.memory_size) }
    }

    #[inline]
    fn tags(&self) -> &[u8] {
        &self.memory_as_slice()[..self.tags_end]
    }

    #[inline]
    fn tags_mut(&mut self) -> &mut [u8] {
        let end = self.tags_end;
        &mut self.memory_as_mut_slice()[..end]
    }

    #[inline(always)]
    fn tag_group_ptr(&self, index: usize) -> *const u8 {
        unsafe { self.memory.as_ptr().add(index) }
    }

    #[inline]
    fn data(&self) -> &[(u64, V)] {
        let slice = self.memory_as_slice();
        let ptr = slice[self.data_start..].as_ptr().cast::<(u64, V)>();
        unsafe { from_raw_parts(ptr, self.bucket_mask + 1) }
    }

    #[inline]
    fn data_mut(&mut self) -> &mut [(u64, V)] {
        let start = self.data_start;
        let slice = self.memory_as_mut_slice();
        let ptr = slice[start..].as_mut_ptr().cast::<(u64, V)>();
        unsafe { from_raw_parts_mut(ptr, self.bucket_mask + 1) }
    }

    #[inline]
    fn data_ref(&self, index: usize) -> &(u64, V) {
        let data_start_ptr = unsafe { self.memory.as_ptr().add(self.data_start) }.cast::<(u64, V)>();
        unsafe { &*data_start_ptr.add(index) }
    }

    fn cell_count(&self) -> usize {
        self.bucket_mask + 1
    }

    pub unsafe fn get_group(&self, group_start: usize) -> G::Group {
        G::load_ptr(self.tag_group_ptr(group_start))
    }

    #[inline(always)]
    pub unsafe fn get_item(&self, group_start: usize, position: usize) -> &(u64, V) {
        let item_index = (group_start + position) & self.bucket_mask;
        let item = self.data_ref(item_index);
        item
    }

    pub unsafe fn get_tag(&self, group_start: usize, position: usize) -> u8 {
        *self.tag_group_ptr(group_start + position)
    }

    #[inline(always)]
    pub unsafe fn get(&self, hash: u64) -> Option<&V> {
        // Compute starting position of probe sequence
        let (search_tag, search_hash) = Self::prepare_hash(hash);
        let (mut index, mut stride) = G::ProbeSeq::start(hash, self.bucket_mask);

        loop {
            let group_start = index;
            let group = G::load_ptr(self.tag_group_ptr(group_start));

            // Check if the search tag appears in the chunk's tags
            for position in G::match_tag(&group, search_tag) {
                let item_index = (group_start + position) & self.bucket_mask;
                let item = self.data_ref(item_index);
                if item.0 == search_hash {
                    // Found a match
                    return Some(&item.1);
                }
            }

            // Check if we are at the end of the probe chain
            if G::contains_empty_slot(&group) {
                return None;
            }

            // Probe to the next group
            index = G::ProbeSeq::next(index, search_tag, &mut stride, self.bucket_mask);
        }
    }

    #[inline]
    pub unsafe fn get_const_lookup(&self, hash: u64) -> Option<&V> {
        // Compute starting position of probe sequence
        let (search_tag, search_hash) = Self::prepare_hash(hash);
        let (mut index, mut stride) = G::ProbeSeq::start(hash, self.bucket_mask);

        loop {
            let group_start = index;
            let group = G::load_ptr(self.tag_group_ptr(group_start));

            // Check if the search tag appears in the chunk's tags
            for position in G::match_tag(&group, search_tag) {
                let item_index = (group_start + position) & self.bucket_mask;
                let item = self.data_ref(item_index);
                if item.0 == search_hash {
                    // Found a match
                    return Some(&item.1);
                }
            }

            // Check if we are at the end of the probe chain
            if G::contains_empty_slot(&group) {
                return None;
            }

            // Probe to the next group
            index = G::ProbeSeq::next(index, search_tag, &mut stride, self.bucket_mask);
        }
    }

    #[inline]
    pub unsafe fn get_in_bulk(&self, hashes: &[u64]) -> Vec<Option<&V>> {
        let indexes = hashes.iter().map(|hash| G::ProbeSeq::start_index(*hash, self.bucket_mask)).collect::<Vec<_>>();
        let groups = indexes.iter().map(|index| G::load_ptr(self.tag_group_ptr(*index))).collect::<Vec<_>>();

        let search_tags = hashes.iter().map(|hash| G::get_tag(*hash)).collect::<Vec<_>>();
        let mut iterators = groups.iter()
            .zip(search_tags.iter())
            .map(|(group, tag)| G::match_tag(group, *tag).into_iter())
            .collect::<Vec<_>>();
        let first_position = iterators.iter_mut()
            .zip(indexes.iter())
            .map(|(iterator, index)| match iterator.next() {
                None => None,
                Some(position) => Some((index + position) & self.bucket_mask),
            })
            .collect::<Vec<_>>();

        let search_hashes = hashes.iter().map(|hash| hash | HASH_OCCUPIED_BIT).collect::<Vec<_>>();
        // let stride = hashes.iter().map(|hash| G::ProbeSeq::initial_stride(*hash, self.bucket_mask)).collect::<Vec<_>>();

        search_tags.into_iter()
            .zip(search_hashes.into_iter())
            .zip(indexes.into_iter())
            // .zip(stride.into_iter())
            .zip(iterators.into_iter())
            .zip(first_position.into_iter())
            .map(|(((((search_tag, search_hash), mut index)), mut iterator), first_position)| {
                // Check the first computed entry from the iterator
                if let Some(first_item_index) = first_position {
                    let item = self.data_ref(first_item_index);
                    if item.0 == search_hash {
                        // Found a match
                        return Some(&item.1);
                    }
                }

                // Continue polling the iterator
                let group_start = index;
                for position in iterator {
                    let item_index = (group_start + position) & self.bucket_mask;
                    let item = self.data_ref(item_index);
                    if item.0 == search_hash {
                        // Found a match
                        return Some(&item.1);
                    }
                }

                // Probe to the next group
                let mut stride = G::ProbeSeq::initial_stride(search_hash, self.bucket_mask);
                index = G::ProbeSeq::next(index, search_tag, &mut stride, self.bucket_mask);

                loop {
                    let group_start = index;
                    let group = G::load_ptr(self.tag_group_ptr(group_start));

                    // Check if the search tag appears in the chunk's tags
                    for position in G::match_tag(&group, search_tag) {
                        let item_index = (group_start + position) & self.bucket_mask;
                        let item = self.data_ref(item_index);
                        if item.0 == search_hash {
                            // Found a match
                            return Some(&item.1);
                        }
                    }

                    // Check if we are at the end of the probe chain
                    if G::contains_empty_slot(&group) {
                        return None;
                    }

                    // Probe to the next group
                    index = G::ProbeSeq::next(index, search_tag, &mut stride, self.bucket_mask);
                }
            })
            .collect()
    }

    #[inline]
    pub unsafe fn get_in_bulk_static(&self, hashes: [&u64; 256]) -> Vec<Option<&V>> {
        let search_tags = hashes.iter().map(|hash| G::get_tag(**hash)).collect::<Vec<_>>();
        let search_hashes = hashes.iter().map(|hash| **hash | HASH_OCCUPIED_BIT).collect::<Vec<_>>();
        let indexes = hashes.iter().map(|hash| G::ProbeSeq::start_index(**hash, self.bucket_mask)).collect::<Vec<_>>();
        let stride = hashes.iter().map(|hash| G::ProbeSeq::initial_stride(**hash, self.bucket_mask)).collect::<Vec<_>>();

        search_tags.into_iter()
            .zip(search_hashes.into_iter())
            .zip(indexes.into_iter())
            .zip(stride.into_iter())
            .map(|(((search_tag, search_hash), mut index), mut stride)| {
                loop {
                    let group_start = index;
                    let group = G::load_ptr(self.tag_group_ptr(group_start));

                    // Check if the search tag appears in the chunk's tags
                    for position in G::match_tag(&group, search_tag) {
                        let item_index = (group_start + position) & self.bucket_mask;
                        let item = self.data_ref(item_index);
                        if item.0 == search_hash {
                            // Found a match
                            return Some(&item.1);
                        }
                    }

                    // Check if we are at the end of the probe chain
                    if G::contains_empty_slot(&group) {
                        return None;
                    }

                    // Probe to the next group
                    index = G::ProbeSeq::next(index, search_tag, &mut stride, self.bucket_mask);
                }
            })
            .collect()
    }

    #[inline]
    pub unsafe fn get_in_bulk_4(&self, hashes: [&u64; 256]) -> Vec<Option<&V>> {
        // Compute starting position of probe sequence
        // let (search_tag, search_hash) = Self::prepare_hash(hash);
        // let (mut index, mut stride) = G::ProbeSeq::start(hash, self.bucket_mask);

        let search_tags = hashes.iter().map(|hash| G::get_tag(**hash)).collect::<Vec<_>>();
        let search_hashes = hashes.iter().map(|hash| **hash | HASH_OCCUPIED_BIT).collect::<Vec<_>>();
        let indexes = hashes.iter().map(|hash| G::ProbeSeq::start_index(**hash, self.bucket_mask)).collect::<Vec<_>>();
        let stride = hashes.iter().map(|hash| G::ProbeSeq::initial_stride(**hash, self.bucket_mask)).collect::<Vec<_>>();

        search_tags.into_iter()
            .zip(search_hashes.into_iter())
            .zip(indexes.into_iter())
            .zip(stride.into_iter())
            .map(|(((search_tag, search_hash), mut index), mut stride)| {
                loop {
                    let group_start = index;
                    let group = G::load_ptr(self.tag_group_ptr(group_start));

                    // Check if the search tag appears in the chunk's tags
                    for position in G::match_tag(&group, search_tag) {
                        let item_index = (group_start + position) & self.bucket_mask;
                        let item = self.data_ref(item_index);
                        if item.0 == search_hash {
                            // Found a match
                            return Some(&item.1);
                        }
                    }

                    // Check if we are at the end of the probe chain
                    if G::contains_empty_slot(&group) {
                        return None;
                    }

                    // Probe to the next group
                    index = G::ProbeSeq::next(index, search_tag, &mut stride, self.bucket_mask);
                }
            })
            .collect()
    }

    #[inline]
    pub unsafe fn get_with_stats(&self, hash: u64) -> (Option<&V>, usize, usize) {
        // Compute starting position of probe sequence
        let (search_tag, _) = Self::prepare_hash(hash);
        let (mut index, mut stride) = G::ProbeSeq::start(hash, self.bucket_mask);

        let mut probe_length = 0;
        let mut tags_false_positives = 0;

        // let mut history = Vec::with_capacity(chunks.len());
        loop {
            let group_start = index;
            let group_end = group_start + G::GROUP_SIZE;
            let group_tags = &self.tags()[group_start..group_end];

            // history.push((chunk_index, loaded_tags.clone()));

            // Check if the search tag appears in the chunk's tags
            let group = G::load(group_tags);
            for position in G::match_tag(&group, search_tag) {
                let item_index = (group_start + position) & self.bucket_mask;
                let item = &self.data()[item_index];
                if item.0 == (hash | HASH_OCCUPIED_BIT) {
                    // Found a match
                    return (Some(&item.1), probe_length, tags_false_positives);
                } else {
                    tags_false_positives += 1;
                }
            }

            // Check if we are at the end of the probe chain
            if G::contains_empty_slot(&group) {
                return (None, probe_length, tags_false_positives);
            }

            // Chunk is full, probe to the next one
            index = G::ProbeSeq::next(index, search_tag, &mut stride, self.bucket_mask);
            probe_length += 1;
        }

        // There was not enough space in the table to store a new value
        // panic!("Table is full. Hash {hash}, value {:?}, attempts {attempts}, chunks {}, history {:?}", value, chunks.len(), history);
        // Err(())
    }


    // pub fn batch_insert_atomically(&mut self, hashes: [u64; N], values: [V; N]) -> Result<Vec<Option<V>>, ()> {
    //     let mut search_tags = [0u8; N];
    //     let mut storable_hashes = [0u64; N];
    //     let mut indexes = [0usize; N];
    //     let mut strides = [0usize; N];
    //     for (i, hash) in hashes.iter().enumerate() {
    //         let (tag, storable) = Self::prepare_hash(*hash);
    //         search_tags[i] = tag;
    //         storable_hashes[i] = storable;
    //
    //         let (index, stride) = G::ProbeSeq::start(*hash, self.bucket_mask);
    //         indexes[i] = index;
    //         strides[i] = stride;
    //     }
    //
    //     // Do the first loop explicitly
    //     let group_starts = indexes;
    //     let mut group_ends = [0usize; N];
    //     for (i, &start) in group_starts.iter().enumerate() {
    //         group_ends[i] = start + G::GROUP_SIZE;
    //     }
    //
    //     let mut slices = [(); N].map(|_| G::allocate_slice());
    //     let mut groups = [
    //         {
    //             let mut slice = G::allocate_slice();
    //             let loaded_tags = slice.as_mut();
    //             let group_tags = &mut self.tags_mut()[group_starts[0]..group_ends[0]];
    //             Self::load_tags_atomically(group_tags, loaded_tags);
    //             unsafe { G::load(loaded_tags) }
    //         },
    //         {
    //             let mut slice = G::allocate_slice();
    //             let loaded_tags = slice.as_mut();
    //             let group_tags = &mut self.tags_mut()[group_starts[1]..group_ends[1]];
    //             Self::load_tags_atomically(group_tags, loaded_tags);
    //             unsafe { G::load(loaded_tags) }
    //         },
    //         {
    //             let mut slice = G::allocate_slice();
    //             let loaded_tags = slice.as_mut();
    //             let group_tags = &mut self.tags_mut()[group_starts[2]..group_ends[2]];
    //             Self::load_tags_atomically(group_tags, loaded_tags);
    //             unsafe { G::load(loaded_tags) }
    //         },
    //         {
    //             let mut slice = G::allocate_slice();
    //             let loaded_tags = slice.as_mut();
    //             let group_tags = &mut self.tags_mut()[group_starts[3]..group_ends[3]];
    //             Self::load_tags_atomically(group_tags, loaded_tags);
    //             unsafe { G::load(loaded_tags) }
    //         },
    //         {
    //             let mut slice = G::allocate_slice();
    //             let loaded_tags = slice.as_mut();
    //             let group_tags = &mut self.tags_mut()[group_starts[4]..group_ends[4]];
    //             Self::load_tags_atomically(group_tags, loaded_tags);
    //             unsafe { G::load(loaded_tags) }
    //         },
    //         {
    //             let mut slice = G::allocate_slice();
    //             let loaded_tags = slice.as_mut();
    //             let group_tags = &mut self.tags_mut()[group_starts[5]..group_ends[5]];
    //             Self::load_tags_atomically(group_tags, loaded_tags);
    //             unsafe { G::load(loaded_tags) }
    //         },
    //         {
    //             let mut slice = G::allocate_slice();
    //             let loaded_tags = slice.as_mut();
    //             let group_tags = &mut self.tags_mut()[group_starts[6]..group_ends[6]];
    //             Self::load_tags_atomically(group_tags, loaded_tags);
    //             unsafe { G::load(loaded_tags) }
    //         },
    //         {
    //             let mut slice = G::allocate_slice();
    //             let loaded_tags = slice.as_mut();
    //             let group_tags = &mut self.tags_mut()[group_starts[7]..group_ends[7]];
    //             Self::load_tags_atomically(group_tags, loaded_tags);
    //             unsafe { G::load(loaded_tags) }
    //         }
    //     ];
    //
    //     groups.map()
    //
    //     Err(())
    // }

    #[inline(always)]
    pub fn insert_atomically(&mut self, hash: u64, value: V) -> (Result<Option<V>, ()>, usize) {
        // Compute starting position of probe sequence
        let (search_tag, storable_hash) = Self::prepare_hash(hash);
        let (mut index, mut stride) = G::ProbeSeq::start(hash, self.bucket_mask);

        // println!("Inserting {} : {hash} using search tag {search_tag}, starting at {index}", self.display(&value));

        // let mut history = Vec::with_capacity(chunks.len());
        let mut attempts = 0;
        loop {
            let group_start = index;
            let group_end = group_start + G::GROUP_SIZE;
            let group_tags = &mut self.tags_mut()[group_start..group_end];

            // Load the tags value
            let mut slice = G::allocate_slice();
            let loaded_tags = slice.as_mut();
            Self::load_tags_atomically(group_tags, loaded_tags);
            let group = unsafe { G::load(&loaded_tags) };

            // history.push((chunk_index, loaded_tags.clone()));

            // Check if the search tag appears in the chunk's tags
            for position in unsafe { G::match_tag(&group, search_tag) } {
                let item_index = (group_start + position) & self.bucket_mask;
                let item = &mut self.data_mut()[item_index];
                let atomic_hash = unsafe { Self::atomic_hash_ref(item) };
                let item_hash = atomic_hash.load(Ordering::Relaxed);
                if item_hash == storable_hash {
                    // Found a match, overwrite the value
                    let atomic_value = unsafe { Self::atomic_value_ref(item) };
                    let previous = atomic_value.swap(value, Ordering::Relaxed);

                    // println!("Overwrote value {:?} with {:?} at position {} in chunk {}, masked hash {:0b}", previous, value, position, chunk_index, masked_item_hash);

                    // Due to unpredictable orderings between atomic write, it is possible that the
                    // hash was set but no the tag, so we still need to check if the value is 0
                    return (Ok(Self::ignore_default_value(previous)), attempts);
                }

                if item_hash == 0 {
                    // Due to the unpredictable ordering of atomic writes, it is possible that this
                    // thread saw the updated tag value but not the updated hash value. In which
                    // case, we can just mark the tag value as empty so we re-check it again later.
                    loaded_tags[position] = G::EMPTY_TAG;
                }
            }

            // No existing entries matched the search
            // --------------------------------------

            // Search for empty slots
            for position in unsafe { G::match_empty(&group) } {
                // Try to write the value into the empty slot
                let item_index = (group_start + position) & self.bucket_mask;
                let item = &mut self.data_mut()[item_index];
                let atomic_hash = unsafe { Self::atomic_hash_ref(item) };

                match atomic_hash.compare_exchange(0, storable_hash, Ordering::Relaxed, Ordering::Relaxed) {
                    Ok(_) => {
                        // We write to the value with a swap operation because there is a chance
                        // that another thread tried to write to the value at the same time
                        let atomic_value = unsafe { Self::atomic_value_ref(item) };
                        let previous = atomic_value.swap(value, Ordering::Relaxed);

                        // Write to the tag to claim it as well
                        self.update_tag(item_index, search_tag, self.bucket_mask);

                        // println!("Just stored {} with value {:?} and tag {} at position {} in chunk {}, (masked hash {}, with bit {})", hash, value, search_tag, position, chunk_index, masked_stored_hash, masked_stored_hash | OVERFLOW_BIT_MASK);

                        // Due to writing the tags after the value, the previous value will always
                        // be zero here
                        // assert_eq!(previous, V::default());
                        // return Ok(None);

                        return (Ok(Self::ignore_default_value(previous)), attempts);
                    },
                    Err(previous_hash) if previous_hash == storable_hash => {
                        // Detected duplicate late. Swap the existing value with our one
                        let atomic_value = unsafe { Self::atomic_value_ref(item) };
                        let previous = atomic_value.swap(value, Ordering::Relaxed);
                        return (Ok(Self::ignore_default_value(previous)), attempts);
                    },
                    // Err(previous_hash) if previous_hash == GroupT::get_positional_overflow_bit_or_mask(position) => {
                    //     // panic if the overflow bit was already set without a value being written.
                    //     // It is possible that this is a valid but uncommon state due to the
                    //     // unpredictable ordering of atomic values, but for now we treat it as if
                    //     // it should never happen.
                    //     panic!("Overflow bit was set without a value being written. Hash {}, value {:?}, attempts {}", hash, value, attempts);
                    // }
                    // The cell was written to by another thread after we loaded the tag values
                    Err(_) => continue,
                };
            }

            attempts += 1;
            let max_attempts = self.max_insert_attemps;
            if attempts >= max_attempts {
                return (Err(()), attempts);
            }

            // if attempts == max_attempts / 2 {
            //     println!("Attempted for more than half of the table size {}", attempts);
            // }

            // Chunk is full, probe to the next one
            index = G::ProbeSeq::next(index, search_tag, &mut stride, self.bucket_mask);
        }

        // There was not enough space in the table to store a new value
        // panic!("Table is full. Hash {hash}, value {:?}, attempts {attempts}, chunks {}, history {:?}", value, chunks.len(), history);
        // Err(())
    }

    #[inline(always)]
    pub fn max_insert_attempts(&mut self) -> usize {
        self.max_insert_attemps
    }

    #[inline(always)]
    fn prepare_hash(hash: u64) -> (u8, u64) {
        let search_tag = G::get_tag(hash);
        let storable_hash = hash | HASH_OCCUPIED_BIT;
        (search_tag, storable_hash)
    }

    #[inline(always)]
    fn load_tags_atomically(group_tags: &mut [u8], destination: &mut [u8]) {
        for (tag, output) in group_tags.iter_mut().zip(destination.iter_mut()) {
            let tag_atomic = unsafe { Self::atomic_tag_ref(tag) };
            *output = tag_atomic.load(Ordering::Relaxed);
        }
        // loaded_tags
    }

    #[inline(always)]
    fn update_tag(&mut self, index: usize, tag: u8, bucket_mask: usize) {
        // The first GROUP_SIZE - 1 tags are actually duplicated at the end of the array, so both of
        // them need to be updated. The below formula can do this without a branch, as if the index
        // is greater than the GROUP_SIZE, index == index2
        let index2 = (index.wrapping_sub(G::GROUP_SIZE) & bucket_mask) + G::GROUP_SIZE;
        {
            let atomic_tag = unsafe { Self::atomic_tag_ref(&mut self.tags_mut()[index]) };
            atomic_tag.store(tag, Ordering::Relaxed);
        }
        {
            let atomic_tag2 = unsafe { Self::atomic_tag_ref(&mut self.tags_mut()[index2]) };
            atomic_tag2.store(tag, Ordering::Relaxed);
        }
    }

    #[inline(always)]
    fn ignore_default_value(value: V) -> Option<V> {
        if value == V::default() {
            None
        } else {
            Some(value)
        }
    }

    #[inline(always)]
    unsafe fn atomic_hash_ref(item: &mut (u64, V)) -> &AtomicU64 {
        let hash_ref = &mut item.0;
        let hash_ptr = core::ptr::from_mut(hash_ref);
        AtomicU64::from_ptr(hash_ptr)
    }

    #[inline(always)]
    unsafe fn atomic_value_ref(item: &mut (u64, V)) -> &V::AtomicT {
        let value_ref = &mut item.1;
        let value_ptr = core::ptr::from_mut(value_ref);
        V::AtomicT::from_ptr(value_ptr)
    }

    #[inline(always)]
    unsafe fn atomic_tag_ref(tag_ref: &mut u8) -> &AtomicU8 {
        let tag_ptr = core::ptr::from_mut(tag_ref);
        AtomicU8::from_ptr(tag_ptr)
    }
}

impl <V, G> Inner<V, G>
where
    V: AsAtomic + Default + PartialEq + Clone,
    G: BulkGroupStrategy32,
{
    #[inline(always)]
    pub unsafe fn get_in_bulk_group_32(&self, hashes: &[u64; 32]) -> [Option<&V>; 32] {
        // let capacity_mask = <G as BulkGroupStrategy32>::ProbeSeq::load_capacity_mask(self.bucket_mask);
        // let indices = <G as BulkGroupStrategy32>::ProbeSeq::start_indices(hashes, capacity_mask);
        let mut indices = [0u64; 32];
        for (hash, output) in hashes.iter()
            .zip(indices.iter_mut()) {
            *output = *hash & (self.bucket_mask as u64);
        }

        // let indices = hashes.iter().map(|hash| *hash & (self.bucket_mask as u64)).collect::<Vec<_>>();
        let mut search_tags = G::get_tags(hashes);

        // let search_hashes = hashes.iter().map(|hash| hash | HASH_OCCUPIED_BIT).collect::<Vec<_>>();

        let mut search_hashes = [0u64; 32];
        for (hash, output) in hashes.iter().zip(search_hashes.iter_mut()) {
            *output = hash | HASH_OCCUPIED_BIT
        }
        let strides = [0usize; 32];

        let mut output = [None; 32];
        for ((((search_tag, search_hash), index), mut stride), output) in search_tags.into_iter()
            .zip(search_hashes.into_iter())
            .zip(indices.into_iter())
            .zip(strides.into_iter())
            .zip(output.iter_mut()) {
            let mut index = index as usize;
            *output = self.inner_loop_32(search_tag, search_hash, &mut index, &mut stride);
        }

        output
    }

    #[inline(always)]
    unsafe fn inner_loop_32(&self, search_tag: u8, search_hash: u64, index: &mut usize, stride: &mut usize) -> Option<&V> {
        // let search_hash = hash | HASH_OCCUPIED_BIT;
        loop {
            let group = G::load_ptr(self.tag_group_ptr(*index));

            // Check if the search tag appears in the chunk's tags
            for position in G::match_tag(&group, search_tag) {
                let item_index = (*index + position) & self.bucket_mask;
                let item = self.data_ref(item_index);
                if item.0 == search_hash {
                    // Found a match
                    return Some(&item.1);
                }
            }

            // Check if we are at the end of the probe chain
            if G::contains_empty_slot(&group) {
                return None;
            }

            // Probe to the next group
            *index = <G as GroupStrategy>::ProbeSeq::next(*index, search_tag, stride, self.bucket_mask);
        }
    }
}

impl <V, G> Inner<V, G>
where
    V: AsAtomic + Default + PartialEq + Clone,
    G: BulkGroupStrategyN,
{
    #[inline(always)]
    pub unsafe fn get_in_bulk_group_n<const N: usize>(&self, hashes: &[u64; N], output: &mut [Option<V>; N]) {
        // let capacity_mask = <G as BulkGroupStrategy32>::ProbeSeq::load_capacity_mask(self.bucket_mask);
        let indices = <G as BulkGroupStrategyN>::ProbeSeq::start_indices(hashes, self.bucket_mask);

        // let indices = hashes.iter().map(|hash| *hash & (self.bucket_mask as u64)).collect::<Vec<_>>();
        let mut search_tags = G::get_tags(hashes);

        // let search_hashes = hashes.iter().map(|hash| hash | HASH_OCCUPIED_BIT).collect::<Vec<_>>();

        let mut search_hashes = [0u64; N];
        for (hash, output) in hashes.iter().zip(search_hashes.iter_mut()) {
            *output = hash | HASH_OCCUPIED_BIT
        }
        let strides = [0usize; N];

        for ((((search_tag, search_hash), index), mut stride), output) in search_tags.into_iter()
            .zip(search_hashes.into_iter())
            .zip(indices.into_iter())
            .zip(strides.into_iter())
            .zip(output.iter_mut()) {
            let mut index = index as usize;
            *output = self.inner_loop_n(search_tag, search_hash, &mut index, &mut stride).cloned();
        }
    }

    #[inline(always)]
    unsafe fn inner_loop_n(&self, search_tag: u8, search_hash: u64, index: &mut usize, stride: &mut usize) -> Option<&V> {
        // let search_hash = hash | HASH_OCCUPIED_BIT;
        loop {
            let group = G::load_ptr(self.tag_group_ptr(*index));

            // Check if the search tag appears in the chunk's tags
            for position in G::match_tag(&group, search_tag) {
                let item_index = (*index + position) & self.bucket_mask;
                let item = self.data_ref(item_index);
                if item.0 == search_hash {
                    // Found a match
                    return Some(&item.1);
                }
            }

            // Check if we are at the end of the probe chain
            if G::contains_empty_slot(&group) {
                return None;
            }

            // Probe to the next group
            *index = <G as GroupStrategy>::ProbeSeq::next(*index, search_tag, stride, self.bucket_mask);
        }
    }

    #[inline(always)]
    pub unsafe fn get_in_bulk_group_n_b<const N: usize>(&self, hashes: &[u64; N], output: &mut [Option<V>; N]) {
        let indices = <G as BulkGroupStrategyN>::ProbeSeq::start_indices(hashes, self.bucket_mask);
        let mut search_tags = G::get_tags(hashes);

        let search_hashes = hashes.map(|hash| hash | HASH_OCCUPIED_BIT);
        // let mut search_hashes = [0u64; N];
        // for (hash, output) in hashes.iter().zip(search_hashes.iter_mut()) {
        //     *output = hash | HASH_OCCUPIED_BIT
        // }
        // let strides = [0usize; N];
        let groups = indices.map(|index| self.tag_group_ptr(index as usize));
        let match_tags = G::match_tag_n(&groups, &search_tags);

        for (((((search_tag, search_hash), index), group), match_tag), output) in search_tags.into_iter()
            .zip(search_hashes.into_iter())
            .zip(indices.into_iter())
            .zip(groups.into_iter())
            .zip(match_tags.into_iter())
            .zip(output.iter_mut()) {
            *output = self.inner_loop_n_b(search_tag, search_hash, index as usize, group, match_tag).cloned();
        }
    }

    #[inline(always)]
    unsafe fn inner_loop_n_b(
        &self,
        search_tag: u8,
        search_hash: u64,
        mut index: usize,
        group_ptr: *const u8,
        mut match_tag: G::TagIt,
    ) -> Option<&V> {
        // Check if the search tag appears in the chunk's tags
        for position in match_tag {
            let item_index = (index + position) & self.bucket_mask;
            let item = self.data_ref(item_index);
            if item.0 == search_hash {
                // Found a match
                return Some(&item.1);
            }
        }

        // Check if we are at the end of the probe chain
        if G::contains_empty_slot(&G::load_ptr(group_ptr)) {
            return None;
        }

        let mut stride = 0usize;
        loop {
            // Probe to the next group
            index = <G as GroupStrategy>::ProbeSeq::next(index, search_tag, &mut stride, self.bucket_mask);

            // Check if the search tag appears in the chunk's tags
            let group = G::load_ptr(self.tag_group_ptr(index));
            for position in G::match_tag_1(&group, search_tag) {
                let item_index = (index + position) & self.bucket_mask;
                let item = self.data_ref(item_index);
                if item.0 == search_hash {
                    // Found a match
                    return Some(&item.1);
                }
            }

            // Check if we are at the end of the probe chain
            if G::contains_empty_slot(&group) {
                return None;
            }
        }
    }
}

impl <V, G> Inner<V, G>
where
    V: AsAtomic + Default + PartialEq + Clone,
    G: BulkGroupStrategy,
{
    #[inline(always)]
    pub unsafe fn get_in_bulk_group(&self, hashes: &[u64; 8]) -> [Option<&V>; 8] {
        let capacity_mask = <G as BulkGroupStrategy>::ProbeSeq::load_capacity_mask(self.bucket_mask);
        let indices = <G as BulkGroupStrategy>::ProbeSeq::start_indices(hashes, capacity_mask);
        let search_tags = G::get_tags(hashes);

        let mut search_hashes = [0u64; 8];
        // for (hash, output) in hashes.iter()
        //     .zip(search_hashes.iter_mut()) {
        //     *output = hash | HASH_OCCUPIED_BIT
        // }
        let strides = [0usize; 8];

        let mut output = [None; 8];
        for ((((search_tag, search_hash), index), mut stride), output) in search_tags.into_iter()
            .zip(hashes.into_iter())
            .zip(indices.into_iter())
            .zip(strides.into_iter())
            .zip(output.iter_mut()) {
            let mut index = index as usize;
            *output = self.inner_loop(search_tag, search_hash, &mut index, &mut stride);
        }

        output
    }

    #[inline(always)]
    unsafe fn inner_loop(&self, search_tag: u8, hash: &u64, index: &mut usize, stride: &mut usize) -> Option<&V> {
        let search_hash = hash | HASH_OCCUPIED_BIT;
        loop {
            let group = G::load_ptr(self.tag_group_ptr(*index));

            // Check if the search tag appears in the chunk's tags
            for position in G::match_tag(&group, search_tag) {
                let item_index = (*index + position) & self.bucket_mask;
                let item = self.data_ref(item_index);
                if item.0 == search_hash {
                    // Found a match
                    return Some(&item.1);
                }
            }

            // Check if we are at the end of the probe chain
            if G::contains_empty_slot(&group) {
                return None;
            }

            // Probe to the next group
            *index = <G as GroupStrategy>::ProbeSeq::next(*index, search_tag, stride, self.bucket_mask);
        }
    }
}

pub struct WritableFixedTable<V, G> {
    size: AtomicU32,
    inner: UnsafeCell<Inner<V, G>>,
    capacity: usize,
}

unsafe impl <T: Send, G> Send for WritableFixedTable<T, G> {}
unsafe impl <T: Sync, G> Sync for WritableFixedTable<T, G> {}

impl <V, G> WritableFixedTable<V, G>
where
    V: AsAtomic + Default + PartialEq + Clone,
    G: GroupStrategy
{
    pub fn with_capacity(capacity: usize) -> Self {
        let capacity_with_headroom = capacity * 8 / 7;
        let capacity = if capacity_with_headroom.is_power_of_two() {
            capacity_with_headroom
        } else {
            capacity_with_headroom.next_power_of_two()
        };
        let capacity = max(capacity, G::GROUP_SIZE);
        let additional_buffer_size = G::GROUP_SIZE;

        Self {
            // inner: UnsafeCell::new(Inner {
            //     bucket_mask: capacity - 1,
            //     data: vec![(0, V::default()); capacity].into_boxed_slice(),
            //     tags: vec![0; capacity + additional_buffer_size].into_boxed_slice(),
            // }),
            size: AtomicU32::new(0),
            capacity,
            inner: UnsafeCell::new(Inner::new(capacity)),
        }
    }

    #[inline(always)]
    pub fn insert(&self, hash: u64, value: V) -> Result<Option<V>, ()> {
        // The writable fixed table only uses insert atomically, which is safe to perform
        // concurrently on the inner table.
        // TODO move unsafe cell to inner table
        let inner = unsafe { &mut *self.inner.get() };
        let (prev, attempts) = inner.insert_atomically(hash, value);
        let prev = prev?;
        // self.size.fetch_add(1, Ordering::Relaxed);
        Ok(prev)
    }

    #[inline(always)]
    pub fn insert_with_attempts_count(&self, hash: u64, value: V) -> Result<(Option<V>, usize), ()> {
        // The writable fixed table only uses insert atomically, which is safe to perform
        // concurrently on the inner table.
        // TODO move unsafe cell to inner table
        let inner = unsafe { &mut *self.inner.get() };
        let (prev, attempts) = inner.insert_atomically(hash, value);
        let prev = prev?;
        // self.size.fetch_add(1, Ordering::Relaxed);
        Ok((prev, attempts))
    }

    #[inline(always)]
    pub fn max_insert_attempts(&self) -> usize {
        let inner = unsafe { &mut *self.inner.get() };
        inner.max_insert_attemps
    }

    pub fn entries(&self) -> WritableMapIterator<'_, V, G>
    where G: IterableGroupStrategy
    {
        WritableMapIterator::new(&self)
    }

    pub fn to_read_only(self) -> ReadOnlyFixedTable<V, G> {
        ReadOnlyFixedTable::new(self.inner)
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }
}

pub struct ReadOnlyFixedTable<V, G> {
    inner: UnsafeCell<Inner<V, G>>,
}

unsafe impl <V: Send, G> Send for ReadOnlyFixedTable<V, G> {}
unsafe impl <V: Sync, G> Sync for ReadOnlyFixedTable<V, G> {}

impl <V, G> ReadOnlyFixedTable<V, G>
where
    V: AsAtomic + Default + PartialEq + Clone,
    G: GroupStrategy,
{
    fn new(inner: UnsafeCell<Inner<V, G>>) -> Self {
        Self { inner }
    }

    #[inline(always)]
    pub fn get(&self, hash: u64) -> Option<&V> {
        // The read operation is safe to perform concurrently on the inner table since we know there
        // are no writes happening concurrently.
        unsafe {
            let inner = &*self.inner.get();
            inner.get(hash)
        }
    }

    #[inline]
    pub fn get_in_bulk(&self, hashes: &[u64]) -> Vec<Option<&V>> {
        // The read operation is safe to perform concurrently on the inner table since we know there
        // are no writes happening concurrently.
        unsafe {
            let inner = &*self.inner.get();
            inner.get_in_bulk(hashes)
        }
    }

    #[inline]
    pub fn get_in_bulk_static(&self, hashes: [&u64; 256]) -> Vec<Option<&V>> {
        // The read operation is safe to perform concurrently on the inner table since we know there
        // are no writes happening concurrently.
        unsafe {
            let inner = &*self.inner.get();
            inner.get_in_bulk_static(hashes)
        }
    }

    #[inline]
    pub fn get_const_lookup(&self, hash: u64) -> Option<&V> {
        // The read operation is safe to perform concurrently on the inner table since we know there
        // are no writes happening concurrently.
        unsafe {
            let inner = &*self.inner.get();
            inner.get_const_lookup(hash)
        }
    }

    pub fn get_with_stats(&self, hash: u64) -> (Option<&V>, usize, usize) {
        // The read operation is safe to perform concurrently on the inner table since we know there
        // are no writes happening concurrently.
        unsafe {
            let inner = &*self.inner.get();
            inner.get_with_stats(hash)
        }
    }
}

impl <V, G> ReadOnlyFixedTable<V, G>
where
    V: AsAtomic + Default + PartialEq + Clone,
    G: BulkGroupStrategy,
{
    #[inline(always)]
    pub fn get_in_bulk_static_8(&self, hashes: &[u64; 8]) -> [Option<&V>; 8] {
        // The read operation is safe to perform concurrently on the inner table since we know there
        // are no writes happening concurrently.
        unsafe {
            let inner = &*self.inner.get();
            inner.get_in_bulk_group(hashes)
        }
    }
}

impl <V, G> ReadOnlyFixedTable<V, G>
where
    V: AsAtomic + Default + PartialEq + Clone,
    G: BulkGroupStrategy32,
{
    #[inline(always)]
    pub fn get_in_bulk_static_32(&self, hashes: &[u64; 32]) -> [Option<&V>; 32] {
        // The read operation is safe to perform concurrently on the inner table since we know there
        // are no writes happening concurrently.
        unsafe {
            let inner = &*self.inner.get();
            inner.get_in_bulk_group_32(hashes)
        }
    }
}

impl <V, G> ReadOnlyFixedTable<V, G>
where
    V: AsAtomic + Default + PartialEq + Clone,
    G: BulkGroupStrategyN,
{
    #[inline(always)]
    pub fn get_in_bulk_static_n<const N: usize>(&self, hashes: &[u64; N], output: &mut [Option<V>; N]) {
        // The read operation is safe to perform concurrently on the inner table since we know there
        // are no writes happening concurrently.
        unsafe {
            let inner = &*self.inner.get();
            inner.get_in_bulk_group_n(hashes, output)
        }
    }

    #[inline(always)]
    pub fn get_in_bulk_static_n_b<const N: usize>(&self, hashes: &[u64; N], output: &mut [Option<V>; N]) {
        // The read operation is safe to perform concurrently on the inner table since we know there
        // are no writes happening concurrently.
        unsafe {
            let inner = &*self.inner.get();
            inner.get_in_bulk_group_n_b(hashes, output)
        }
    }
}

pub struct WritableMapIterator<'a, V, G>
where
    G: GroupStrategy + IterableGroupStrategy + 'static,
    <G as IterableGroupStrategy>::It: 'static,
    <<G as IterableGroupStrategy>::It as IntoIterator>::IntoIter: 'static,
{
    cells: usize,
    group_start: usize,
    // group: Box<dyn Iterator<Item=usize>>,
    // group: <G as IterableGroupStrategy>::It::IntoIter,
    index: usize,
    positions: Vec<usize>,
    table: &'a WritableFixedTable<V, G>,
}

impl<'a, V, G> WritableMapIterator<'a, V, G>
where
    G: GroupStrategy + IterableGroupStrategy + 'static,
    <G as IterableGroupStrategy>::It: 'static,
    <<G as IterableGroupStrategy>::It as IntoIterator>::IntoIter: 'static,
{
    fn new(table: &'a WritableFixedTable<V, G>) -> WritableMapIterator<'a, V, G>
    where
        V: AsAtomic + Clone + Default + PartialEq,
    {
        // TODO I think this will fail when size = 0
        let first_group = unsafe { (&*table.inner.get()).get_group(0) };
        let group_iterator = unsafe { G::match_non_empty(&first_group) };

        let mut positions = Vec::with_capacity(G::GROUP_SIZE);
        for position in group_iterator {
            positions.push(position);
        }

        Self {
            cells: unsafe { (&*table.inner.get()).cell_count() },
            group_start: 0,
            // group: group_iterator.into_iter(),
            positions,
            index: 0,
            table,
        }
    }
}

impl <'a, V, G> Iterator for WritableMapIterator<'a, V, G>
where
    G: GroupStrategy + IterableGroupStrategy + 'static,
    <G as IterableGroupStrategy>::It: 'static,
    <<G as IterableGroupStrategy>::It as IntoIterator>::IntoIter: 'static,
    V: AsAtomic + Clone + Default + PartialEq,
{
    type Item = (u64, &'a V);

    fn next(&mut self) -> Option<Self::Item> {
        if self.group_start >= self.cells {
            return None;
        }

        loop {
            // if let Some(position) = self.group.next() {
            if self.index < self.positions.len() {
                let position = self.positions[self.index];
                self.index += 1;
                if (self.group_start + position) < self.cells {
                    // return item at group index n
                    let inner = unsafe { &*self.table.inner.get() };
                    let pair = unsafe { inner.get_item(self.group_start, position) };
                    let tag = unsafe { inner.get_tag(self.group_start, position) };

                    // println!("Entries iterator : {} hash : {} tag : {}", inner.display(&pair.1), pair.0, tag);

                    // The first bit of the stored hash and the last bit of the tag are reserved,
                    // is used for other purposes, so we need to combine them to rebuild the 
                    // original hash
                    let hash = ((tag & 0b11111110) as u64) << 56 | (pair.0 & 0x01FF_FFFF_FFFF_FFFF);
                    return Some((hash, &pair.1));
                }
            }

            // The current group iterator was exhausted, move to the next one
            self.group_start += G::GROUP_SIZE;
            if self.group_start >= self.cells {
                return None;
            }

            let group_iterator = unsafe {
                let g = (&*self.table.inner.get()).get_group(0);
                G::match_non_empty(&g)
            };

            self.index = 0;
            self.positions.clear();
            for position in group_iterator {
                self.positions.push(position);
            }
            // self.group = group_iterator.into_iter();
        }
    }
}

#[cfg(test)]
mod tests {
    use rand::rngs::StdRng;
    use rand::seq::SliceRandom;
    use rand::{Rng, SeedableRng};
    use std::collections::HashMap;
    use std::sync::Arc;
    use crate::operator::version10::new_map_3::fixed_table::WritableFixedTable;
    use crate::operator::version10::new_map_3::group::Group16;

    #[test]
    fn create_empty_table() {
        let table = WritableFixedTable::<usize, Group16>::with_capacity(20);
        // assert_eq!(table.size, 32);

        let read_table = table.to_read_only();
        assert_eq!(read_table.get(0), None);
        assert_eq!(read_table.get(123), None);
        assert_eq!(read_table.get(u64::MAX), None);
    }

    #[test]
    fn can_read_values() {
        let table = WritableFixedTable::<usize, Group16>::with_capacity(20);
        table.insert(0, 10).unwrap();
        table.insert(123, 1230).unwrap();
        table.insert(u64::MAX, usize::MAX).unwrap();

        let read_table = table.to_read_only();
        assert_eq!(read_table.get(0).copied(), Some(10));
        assert_eq!(read_table.get(123).copied(), Some(1230));
        assert_eq!(read_table.get(u64::MAX).copied(), Some(usize::MAX));

        assert_eq!(read_table.get(456), None);
        assert_eq!(read_table.get(u64::MAX - 1), None);
    }

    #[test]
    fn can_read_overflowed_values() {
        let table = WritableFixedTable::<usize, Group16>::with_capacity(20);
        // Store 17 numbers which all belong to the same initial chunk
        for i in 0..17u64 {
            table.insert(i << 32, i as usize).unwrap();
        }

        let read_table = table.to_read_only();
        for i in 0..17u64 {
            assert_eq!(read_table.get(i << 32).copied(), Some(i as usize));
        }
    }

    #[test]
    fn can_read_from_a_full_table() {
        let table = WritableFixedTable::<usize, Group16>::with_capacity(64);
        let mut hashes = [0u64; 64];
        let mut rng = StdRng::seed_from_u64(123);
        rng.fill(&mut hashes[..]);

        for hash in hashes {
            table.insert(hash, hash as usize).unwrap();
        }

        let read_table = table.to_read_only();
        for hash in hashes {
            assert_eq!(read_table.get(hash).copied(), Some(hash as usize));
        }
    }

    #[test]
    fn write_returns_err_when_table_is_full() {
        let table = WritableFixedTable::<usize, Group16>::with_capacity(64);
        let mut hashes = [0u64; 64];
        let mut rng = StdRng::seed_from_u64(123);
        rng.fill(&mut hashes[..]);

        for hash in hashes {
            table.insert(hash, hash as usize).unwrap();
        }

        assert_eq!(table.insert(1, 1), Err(()));
    }

    #[test]
    fn can_store_all_special_values() {
        let pairs: Vec<_> = (56..64).map(|shift| 1u64 << shift).enumerate().collect();

        // Check that each of the special tag values can be stored in the same chunk without
        // colliding
        let table = WritableFixedTable::<usize, Group16>::with_capacity(16);
        for (value, hash) in pairs.iter() {
            table.insert(*hash, *value).unwrap();
        }

        let read_table = table.to_read_only();
        for (value, hash) in pairs.iter() {
            assert_eq!(read_table.get(*hash), Some(value));
        }
    }

    #[test]
    fn can_store_zero_hash() {
        let table = WritableFixedTable::<usize, Group16>::with_capacity(64);
        assert_eq!(table.insert(0, 100), Ok(None));
        for i in 1..64 {
            assert_eq!(table.insert(i, i as usize), Ok(None));
        }

        let read_table = table.to_read_only();
        assert_eq!(read_table.get(0).copied(), Some(100));
    }

    #[test]
    fn can_detect_duplicates() {
        let table = WritableFixedTable::<usize, Group16>::with_capacity(64);
        assert_eq!(table.insert(1, 1), Ok(None));
        assert_eq!(table.insert(1, 2), Ok(Some(1)));
        assert_eq!(table.insert(4023, 4), Ok(None));
        assert_eq!(table.insert(1, 5), Ok(Some(2)));
        assert_eq!(table.insert(4023, 6), Ok(Some(4)));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn multithreaded_writing_test() {
        let batch_size = 8192;
        let thread_count = 128;
        let mut data = vec![0u64; batch_size * thread_count];
        let mut rng = StdRng::seed_from_u64(123);
        rng.fill(&mut data[..]);

        let pairs: Vec<_> = data.into_iter().enumerate().collect();
        let table = Arc::new(WritableFixedTable::<usize, Group16>::with_capacity(batch_size * thread_count));

        // Start all the threads in parallel
        let batches: Vec<_> = pairs.chunks(batch_size).collect();
        let handles: Vec<_> = batches.into_iter().map(|batch| {
            let table = Arc::clone(&table);
            let batch = Vec::from(batch);
            tokio::spawn(async move {
                for (i, hash) in batch {
                    table.insert(hash, i).unwrap();
                }
            })
        }).collect();

        for handle in handles {
            handle.await.unwrap();
        }

        // Check that all the values are readable in the table
        let read_table = Arc::into_inner(table).unwrap().to_read_only();
        for (i, hash) in pairs {
            assert_eq!(read_table.get(hash).copied(), Some(i));
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn multithreaded_writing_with_duplicates() {
        let duplicates_count = 8;
        let batch_size = 10;
        let thread_count = 128;
        let mut data = vec![0u64; batch_size * thread_count];
        let mut rng = StdRng::seed_from_u64(456);
        rng.fill(&mut data[..]);

        let mut pairs: Vec<_> = (1usize..duplicates_count + 1)
            .map(|v| data.iter().map(|hash| (*hash, v)).collect::<Vec<_>>())
            .flatten()
            .collect();
        pairs.shuffle(&mut rng);
        let table = Arc::new(WritableFixedTable::<usize, Group16>::with_capacity(batch_size * thread_count));

        // Start all the threads in parallel
        // let all_duplicates: Arc<Mutex<HashMap<u64, Vec<usize>>>> = Arc::new(Mutex::new(HashMap::new()));
        let batches: Vec<_> = pairs.chunks(batch_size).collect();
        let handles: Vec<_> = batches.into_iter().map(|batch| {
            let table = Arc::clone(&table);
            let batch = Vec::from(batch);
            tokio::spawn(async move {
                let mut local_duplicates: HashMap<u64, Vec<usize>> = HashMap::new();

                for (hash, v) in batch {
                    match table.insert(hash, v).unwrap() {
                        None => {},
                        Some(previous) => {
                            local_duplicates.entry(hash)
                                .and_modify(|vec| vec.push(previous))
                                .or_insert_with(|| vec![previous]);
                        },
                    }
                }

                local_duplicates

                // // Copy local duplicates to global duplicates
                // let all_duplicates = all_duplicates.lock().await.(local_duplicates);
            })
        }).collect();

        let mut outputs = Vec::new();
        for handle in handles {
            outputs.push(handle.await.unwrap());
        }

        // Check that all the values are readable in the table and contain all the correct values
        let read_table = Arc::into_inner(table).unwrap().to_read_only();
        let mut values = Vec::with_capacity(duplicates_count);
        for hash in data {
            values.clear();

            let final_value = read_table.get(hash).copied();
            assert!(final_value.is_some());
            values.push(final_value.unwrap());

            for output in &outputs {
                if let Some(duplicates) = output.get(&hash) {
                    values.extend_from_slice(duplicates);
                }
            }

            values.sort_unstable();
            assert_eq!(values, (1usize..duplicates_count + 1).collect::<Vec<_>>());
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn supports_writing_zero_hash_in_parallel() {
        let mut rng = StdRng::seed_from_u64(123);
        let table_count = 10usize;
        let thread_count = 16usize;

        // For each table create a shuffled vec of the numbers from 0 to 16, excluding 1 since it
        // is handled the same as the hash 0
        let values: Arc<Vec<_>> = Arc::new((0..table_count).map(|_| {
            let mut block = (1..thread_count as u64 + 1).into_iter().collect::<Vec<_>>();
            block[0] = 0;
            block.shuffle(&mut rng);
            block
        }).collect());

        // Run the test in a single thread, which should always succeed
        let tables = (0..table_count).map(|_| WritableFixedTable::<usize, Group16>::with_capacity(thread_count)).collect::<Vec<_>>();
        for thread_index in 0..thread_count {
            for ((table_index, table), values_block) in tables.iter().enumerate().zip(values.iter()) {
                let hash = values_block[thread_index];
                table.insert(hash, 100 + thread_index).unwrap();
            }
        }

        // Assert that every table has the correct values
        for (table_index, (table, values_block)) in tables.into_iter().zip(values.iter()).enumerate() {
            let read_table = table.to_read_only();
            for (index, hash) in values_block.iter().enumerate() {
                assert_eq!(
                    read_table.get(*hash).copied(), Some(100 + index),
                    "Attempting to read hash {} from table number {}",
                    hash,
                    table_index
                );
            }
        }

        // Then run the test again in parallel, which should fail
        let tables = Arc::new((0..table_count).map(|_| WritableFixedTable::<usize, Group16>::with_capacity(thread_count)).collect::<Vec<_>>());
        let barrier = Arc::new(tokio::sync::Barrier::new(thread_count));
        let handles: Vec<_> = (0..thread_count).map(|thread_index| {
            let barrier = Arc::clone(&barrier);
            let tables = Arc::clone(&tables);
            let values = Arc::clone(&values);
            tokio::spawn(async move {
                for ((table_index, table), values_block) in tables.iter().enumerate().zip(values.iter()) {
                    let hash = values_block[thread_index];
                    barrier.wait().await;
                    table.insert(hash, 100 + thread_index).unwrap();
                }
            })
        }).collect();

        for handle in handles {
            handle.await.unwrap();
        }

        // Assert that every table has the correct values
        for (table_index, (table, values_block)) in Arc::into_inner(tables).unwrap().into_iter().zip(values.iter()).enumerate() {
            let read_table = table.to_read_only();
            for (index, hash) in values_block.iter().enumerate() {
                assert_eq!(
                    read_table.get(*hash).copied(), Some(100 + index),
                    "Attempting to read hash {} from table number {}",
                    hash,
                    table_index
                );
            }
        }
    }

    #[test]
    fn can_read_overflowed_values_in_bulk() {
        let table = WritableFixedTable::<usize, Group16>::with_capacity(20);
        // Store 17 numbers which all belong to the same initial chunk
        let pairs: Vec<_> = (0..17u64).map(|i| (i << 32, i as usize)).collect();
        for (hash, value) in &pairs {
            table.insert(*hash, *value).unwrap();
        }

        let read_table = table.to_read_only();
        let values = read_table.get_in_bulk(&pairs.iter().map(|(hash, _)| *hash).collect::<Vec<_>>());
        for (found, (_, value)) in values.into_iter().zip(pairs.iter()) {
            assert_eq!(found, Some(value));
        }
    }
}

#[cfg(test)]
mod tests8 {
    use rand::rngs::StdRng;
    use rand::seq::SliceRandom;
    use rand::{Rng, SeedableRng};
    use std::collections::HashMap;
    use std::sync::Arc;
    use crate::operator::version10::new_map_3::fixed_table::WritableFixedTable;
    use crate::operator::version10::new_map_3::group::Group8;

    #[test]
    fn create_empty_table() {
        let table = WritableFixedTable::<usize, Group8>::with_capacity(20);
        // assert_eq!(table.size, 32);

        let read_table = table.to_read_only();
        assert_eq!(read_table.get(0), None);
        assert_eq!(read_table.get(123), None);
        assert_eq!(read_table.get(u64::MAX), None);
    }

    #[test]
    fn can_read_values() {
        let table = WritableFixedTable::<usize, Group8>::with_capacity(20);
        table.insert(0, 10).unwrap();
        table.insert(123, 1230).unwrap();
        table.insert(u64::MAX, usize::MAX).unwrap();

        let read_table = table.to_read_only();
        assert_eq!(read_table.get(0).copied(), Some(10));
        assert_eq!(read_table.get(123).copied(), Some(1230));
        assert_eq!(read_table.get(u64::MAX).copied(), Some(usize::MAX));

        assert_eq!(read_table.get(456), None);
        assert_eq!(read_table.get(u64::MAX - 1), None);
    }

    #[test]
    fn can_read_overflowed_values() {
        let table = WritableFixedTable::<usize, Group8>::with_capacity(20);
        // Store 17 numbers which all belong to the same initial chunk
        for i in 0..17u64 {
            table.insert(i << 32, i as usize).unwrap();
        }

        let read_table = table.to_read_only();
        for i in 0..17u64 {
            assert_eq!(read_table.get(i << 32).copied(), Some(i as usize));
        }
    }

    #[test]
    fn can_read_from_a_full_table() {
        let table = WritableFixedTable::<usize, Group8>::with_capacity(64);
        let mut hashes = [0u64; 64];
        let mut rng = StdRng::seed_from_u64(123);
        rng.fill(&mut hashes[..]);

        for hash in hashes {
            table.insert(hash, hash as usize).unwrap();
        }

        let read_table = table.to_read_only();
        for hash in hashes {
            assert_eq!(read_table.get(hash).copied(), Some(hash as usize));
        }
    }

    #[test]
    fn write_returns_err_when_table_is_full() {
        let table = WritableFixedTable::<usize, Group8>::with_capacity(64);
        let mut hashes = [0u64; 64];
        let mut rng = StdRng::seed_from_u64(123);
        rng.fill(&mut hashes[..]);

        for hash in hashes {
            table.insert(hash, hash as usize).unwrap();
        }

        assert_eq!(table.insert(1, 1), Err(()));
    }

    #[test]
    fn can_store_all_special_values() {
        let pairs: Vec<_> = (56..64).map(|shift| 1u64 << shift).enumerate().collect();

        // Check that each of the special tag values can be stored in the same chunk without
        // colliding
        let table = WritableFixedTable::<usize, Group8>::with_capacity(16);
        for (value, hash) in pairs.iter() {
            table.insert(*hash, *value).unwrap();
        }

        let read_table = table.to_read_only();
        for (value, hash) in pairs.iter() {
            assert_eq!(read_table.get(*hash), Some(value));
        }
    }

    #[test]
    fn can_store_zero_hash() {
        let table = WritableFixedTable::<usize, Group8>::with_capacity(64);
        assert_eq!(table.insert(0, 100), Ok(None));
        for i in 1..64 {
            assert_eq!(table.insert(i, i as usize), Ok(None));
        }

        let read_table = table.to_read_only();
        assert_eq!(read_table.get(0).copied(), Some(100));
    }

    #[test]
    fn can_detect_duplicates() {
        let table = WritableFixedTable::<usize, Group8>::with_capacity(64);
        assert_eq!(table.insert(1, 1), Ok(None));
        assert_eq!(table.insert(1, 2), Ok(Some(1)));
        assert_eq!(table.insert(4023, 4), Ok(None));
        assert_eq!(table.insert(1, 5), Ok(Some(2)));
        assert_eq!(table.insert(4023, 6), Ok(Some(4)));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn multithreaded_writing_test() {
        let batch_size = 8192;
        let thread_count = 128;
        let mut data = vec![0u64; batch_size * thread_count];
        let mut rng = StdRng::seed_from_u64(123);
        rng.fill(&mut data[..]);

        let pairs: Vec<_> = data.into_iter().enumerate().collect();
        let table = Arc::new(WritableFixedTable::<usize, Group8>::with_capacity(batch_size * thread_count));

        // Start all the threads in parallel
        let batches: Vec<_> = pairs.chunks(batch_size).collect();
        let handles: Vec<_> = batches.into_iter().map(|batch| {
            let table = Arc::clone(&table);
            let batch = Vec::from(batch);
            tokio::spawn(async move {
                for (i, hash) in batch {
                    table.insert(hash, i).unwrap();
                }
            })
        }).collect();

        for handle in handles {
            handle.await.unwrap();
        }

        // Check that all the values are readable in the table
        let read_table = Arc::into_inner(table).unwrap().to_read_only();
        for (i, hash) in pairs {
            assert_eq!(read_table.get(hash).copied(), Some(i));
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn multithreaded_writing_with_duplicates() {
        let duplicates_count = 8;
        let batch_size = 10;
        let thread_count = 128;
        let mut data = vec![0u64; batch_size * thread_count];
        let mut rng = StdRng::seed_from_u64(456);
        rng.fill(&mut data[..]);

        let mut pairs: Vec<_> = (1usize..duplicates_count + 1)
            .map(|v| data.iter().map(|hash| (*hash, v)).collect::<Vec<_>>())
            .flatten()
            .collect();
        pairs.shuffle(&mut rng);
        let table = Arc::new(WritableFixedTable::<usize, Group8>::with_capacity(batch_size * thread_count));

        // Start all the threads in parallel
        // let all_duplicates: Arc<Mutex<HashMap<u64, Vec<usize>>>> = Arc::new(Mutex::new(HashMap::new()));
        let batches: Vec<_> = pairs.chunks(batch_size).collect();
        let handles: Vec<_> = batches.into_iter().map(|batch| {
            let table = Arc::clone(&table);
            let batch = Vec::from(batch);
            tokio::spawn(async move {
                let mut local_duplicates: HashMap<u64, Vec<usize>> = HashMap::new();

                for (hash, v) in batch {
                    match table.insert(hash, v).unwrap() {
                        None => {},
                        Some(previous) => {
                            local_duplicates.entry(hash)
                                .and_modify(|vec| vec.push(previous))
                                .or_insert_with(|| vec![previous]);
                        },
                    }
                }

                local_duplicates

                // // Copy local duplicates to global duplicates
                // let all_duplicates = all_duplicates.lock().await.(local_duplicates);
            })
        }).collect();

        let mut outputs = Vec::new();
        for handle in handles {
            outputs.push(handle.await.unwrap());
        }

        // Check that all the values are readable in the table and contain all the correct values
        let read_table = Arc::into_inner(table).unwrap().to_read_only();
        let mut values = Vec::with_capacity(duplicates_count);
        for hash in data {
            values.clear();

            let final_value = read_table.get(hash).copied();
            assert!(final_value.is_some());
            values.push(final_value.unwrap());

            for output in &outputs {
                if let Some(duplicates) = output.get(&hash) {
                    values.extend_from_slice(duplicates);
                }
            }

            values.sort_unstable();
            assert_eq!(values, (1usize..duplicates_count + 1).collect::<Vec<_>>());
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn supports_writing_zero_hash_in_parallel() {
        let mut rng = StdRng::seed_from_u64(123);
        let table_count = 10usize;
        let thread_count = 16usize;

        // For each table create a shuffled vec of the numbers from 0 to 16, excluding 1 since it
        // is handled the same as the hash 0
        let values: Arc<Vec<_>> = Arc::new((0..table_count).map(|_| {
            let mut block = (1..thread_count as u64 + 1).into_iter().collect::<Vec<_>>();
            block[0] = 0;
            block.shuffle(&mut rng);
            block
        }).collect());

        // Run the test in a single thread, which should always succeed
        let tables = (0..table_count).map(|_| WritableFixedTable::<usize, Group8>::with_capacity(thread_count)).collect::<Vec<_>>();
        for thread_index in 0..thread_count {
            for ((table_index, table), values_block) in tables.iter().enumerate().zip(values.iter()) {
                let hash = values_block[thread_index];
                table.insert(hash, 100 + thread_index).unwrap();
            }
        }

        // Assert that every table has the correct values
        for (table_index, (table, values_block)) in tables.into_iter().zip(values.iter()).enumerate() {
            let read_table = table.to_read_only();
            for (index, hash) in values_block.iter().enumerate() {
                assert_eq!(
                    read_table.get(*hash).copied(), Some(100 + index),
                    "Attempting to read hash {} from table number {}",
                    hash,
                    table_index
                );
            }
        }

        // Then run the test again in parallel, which should fail
        let tables = Arc::new((0..table_count).map(|_| WritableFixedTable::<usize, Group8>::with_capacity(thread_count)).collect::<Vec<_>>());
        let barrier = Arc::new(tokio::sync::Barrier::new(thread_count));
        let handles: Vec<_> = (0..thread_count).map(|thread_index| {
            let barrier = Arc::clone(&barrier);
            let tables = Arc::clone(&tables);
            let values = Arc::clone(&values);
            tokio::spawn(async move {
                for ((table_index, table), values_block) in tables.iter().enumerate().zip(values.iter()) {
                    let hash = values_block[thread_index];
                    barrier.wait().await;
                    table.insert(hash, 100 + thread_index).unwrap();
                }
            })
        }).collect();

        for handle in handles {
            handle.await.unwrap();
        }

        // Assert that every table has the correct values
        for (table_index, (table, values_block)) in Arc::into_inner(tables).unwrap().into_iter().zip(values.iter()).enumerate() {
            let read_table = table.to_read_only();
            for (index, hash) in values_block.iter().enumerate() {
                assert_eq!(
                    read_table.get(*hash).copied(), Some(100 + index),
                    "Attempting to read hash {} from table number {}",
                    hash,
                    table_index
                );
            }
        }
    }
}
