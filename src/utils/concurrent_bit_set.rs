use std::cmp::min;
use std::iter;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use datafusion::arrow::array::{BooleanArray, BooleanBufferBuilder, UInt32Array, UInt32Builder};
use datafusion::arrow::buffer::MutableBuffer;
use datafusion::arrow::util::bit_util::ceil;

#[derive(Debug, Clone)]
pub struct ConcurrentBitSet {
    length: usize,
    set_count: Arc<AtomicUsize>,
    contents: Arc<Vec<AtomicU64>>,
}

impl ConcurrentBitSet {
    pub fn with_capacity(length: usize) -> ConcurrentBitSet {
        // Round the capacity up to the nearest multiple of 64
        let capacity = ceil(length, 64);
        ConcurrentBitSet {
            length,
            set_count: Arc::new(AtomicUsize::new(0)),
            contents: Arc::new(iter::repeat_with(|| AtomicU64::new(0)).take(capacity).collect()),
        }
    }

    pub fn set_ones(&self, indices: Vec<usize>) {
        if let Some(last_index) = indices.last() {
            if last_index >= &self.length {
                panic!("Index out of bounds");
            }
        }

        let mut current_offset: usize = 0;
        let mut current_bits: u64 = 0;
        for index in indices {

            // Apply the current bits when needed and reset the current bits
            let next_offset = index / 64;
            if next_offset != current_offset && current_bits != 0 {
                self.write_bits_at(current_offset, current_bits);
                current_offset = next_offset;
                current_bits = 0;
            }

            current_bits |= (1u64) << index % 64;
        }

        // Apply the last bits
        self.write_bits_at(current_offset, current_bits);
    }

    fn write_bits_at(&self, offset: usize, bits: u64) {
        let previous_bits = self.contents[offset].fetch_or(bits, Ordering::Relaxed);

        // Increment the number of set bits
        let newly_set_bits = bits & !previous_bits;
        self.set_count.fetch_add(newly_set_bits.count_ones() as usize, Ordering::Relaxed);
    }

    pub fn get_set_count(&self) -> usize {
        self.set_count.load(Ordering::Relaxed)
    }

    // pub fn get_indices(&self) {
    //     let mut x = arrow::array::BooleanBufferBuilder::new_from_buffer();
    //     let z: arrow::array::UInt64Builder;
    //     let f = z.finish();
    //
    //     let y: arrow::buffer::MutableBuffer;
    //
    //     for (offset, block) in self.contents.iter().enumerate() {
    //         let i = offset * 64;
    //         let mut block_value = block.load(Ordering::Relaxed);
    //         while block_value != 0 {
    //             if block_value & 1 {
    //                 x.set_bit(i, true)
    //             }
    //         }
    //     }
    // }

    pub fn get_set_indices(&self) -> BooleanArray {
        // Create a buffer large enough to fit every whole u64, so larger than self.length
        let mut mutable_buffer = MutableBuffer::with_capacity(self.contents.len() * 64);
        mutable_buffer.extend(self.contents.iter().map(|atomic64| atomic64.load(Ordering::Relaxed)));

        // Convert the mutable buffer into a boolean buffer of size self.length
        BooleanArray::from(BooleanBufferBuilder::new_from_buffer(mutable_buffer, self.length).finish())
    }

    pub fn get_unset_indices(&self) -> BooleanArray {
        // Create a buffer large enough to fit every whole u64, so larger than self.length
        let mut mutable_buffer = MutableBuffer::with_capacity(self.contents.len() * 64);
        mutable_buffer.extend(self.contents.iter().map(|atomic64| !atomic64.load(Ordering::Relaxed)));

        // Convert the mutable buffer into a boolean buffer of size self.length
        BooleanArray::from(BooleanBufferBuilder::new_from_buffer(mutable_buffer, self.length).finish())
    }

    pub fn get_unset_indices_array(&self) -> UInt32Array {
        let mut array = UInt32Builder::with_capacity(self.length - self.get_set_count());

        for (atomic64, offset) in self.contents.iter().zip((0..self.length).step_by(64)) {
            let bits = atomic64.load(Ordering::Relaxed);
            for i in 0..min(64, self.length - offset) {
                if bits & (1u64 << i) == 0 {
                    array.append_value((offset + i) as u32);
                }
            }
        }

        array.finish()
    }
}

#[cfg(test)]
mod tests {
    use crate::utils::concurrent_bit_set::ConcurrentBitSet;

    #[test]
    fn converts_single_u64_to_boolean_array() {
        let bit_set = ConcurrentBitSet::with_capacity(64);
        let indices = vec![0, 1, 2, 5, 10, 31, 32, 63];
        bit_set.set_ones(indices.clone());
        let actual_values = bit_set.get_set_indices().iter().collect::<Vec<_>>();
        let expected_values = (0..64).map(|i| Some(indices.contains(&i))).collect::<Vec<_>>();

        assert_eq!(actual_values, expected_values);
    }

    #[test]
    fn converts_multiple_u64_to_boolean_array() {
        let bit_set = ConcurrentBitSet::with_capacity(256);
        let indices = vec![0, 1, 32, 64, 112, 196, 197, 255];
        bit_set.set_ones(indices.clone());
        let actual_values = bit_set.get_set_indices().iter().collect::<Vec<_>>();
        let expected_values = (0..256).map(|i| Some(indices.contains(&i))).collect::<Vec<_>>();

        assert_eq!(actual_values, expected_values);
    }

    #[test]
    fn converts_non_aligned_size_to_boolean_array() {
        let bit_set = ConcurrentBitSet::with_capacity(83);
        let indices = vec![0, 1, 32, 64, 82];
        bit_set.set_ones(indices.clone());
        let actual_values = bit_set.get_set_indices().iter().collect::<Vec<_>>();
        let expected_values = (0..83).map(|i| Some(indices.contains(&i))).collect::<Vec<_>>();

        assert_eq!(actual_values.len(), 83);
        assert_eq!(actual_values, expected_values);
    }

    #[test]
    fn counts_number_of_set_bits() {
        let bit_set = ConcurrentBitSet::with_capacity(83);
        bit_set.set_ones(vec![0, 1, 32, 64, 82]);
        bit_set.set_ones(vec![0, 10, 32, 74, 82]);
        assert_eq!(bit_set.get_set_count(), 7);
    }

    #[test]
    fn get_unset_bits_array() {
        let bit_set = ConcurrentBitSet::with_capacity(83);
        let excluded_indices = vec![0, 1, 4, 12, 54, 63, 64, 81, 82];

        bit_set.set_ones((0..83).into_iter().filter(|n| !excluded_indices.contains(n)).collect());
        assert_eq!(
            bit_set.get_unset_indices_array().iter().collect::<Vec<_>>(),
            excluded_indices.into_iter().map(|i| Some(i as u32)).collect::<Vec<_>>(),
        );
    }
}
