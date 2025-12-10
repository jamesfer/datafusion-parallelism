use std::cell::UnsafeCell;
use std::sync::Arc;
use crate::operator::version10::new_map_3::new_map_3::ReadOnlyTable;
use crate::utils::index_lookup::IndexLookup;

pub struct Version10Lookup {
    read_only_table: Arc<ReadOnlyTable<usize>>,
    overflow_buffer: Arc<UnsafeCell<Vec<usize>>>,
}

unsafe impl Send for Version10Lookup {}
unsafe impl Sync for Version10Lookup {}

impl Version10Lookup {
    pub fn new(
        read_only_table: Arc<ReadOnlyTable<usize>>,
        overflow_buffer: Arc<UnsafeCell<Vec<usize>>>,
    ) -> Self {
        Self { read_only_table, overflow_buffer }
    }
}

pub struct Version10LookupIterator<'a> {
    lookup: &'a Version10Lookup,
    next_index: usize,
}

impl<'a> Iterator for Version10LookupIterator<'a> {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        if self.next_index == 0 {
            return None;
        }

        let current_index = self.next_index;
        // We know that this is safe because the read-only section is never mutated
        let overflow_buffer = unsafe { &*self.lookup.overflow_buffer.get() };
        self.next_index = overflow_buffer[current_index];

        // Need to subtract 1 because all indices are incremented by 1, since 0 is a placeholder
        Some(current_index - 1)
    }
}

impl IndexLookup<u64> for Version10Lookup {
    type It<'a> = Version10LookupIterator<'a>
    where
        Self: 'a;

    fn get_iter<'a>(&'a self, key: &'a u64) -> Self::It<'a> {
        let first_index = self.read_only_table.get(*key).copied();
        let next_index = first_index.unwrap_or(0);

        Version10LookupIterator {
            lookup: self,
            next_index,
        }
    }
}
