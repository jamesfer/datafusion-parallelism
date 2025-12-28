pub trait ProbeSequence {
    const GROUP_SIZE: usize;

    fn start_index(hash: u64, capacity_mask: usize) -> usize;

    fn initial_stride(hash: u64, capacity_mask: usize) -> usize {
        Self::start_index(hash, capacity_mask)
    }

    fn next(previous: usize, stride: &mut usize, hash: u64, tag: u8, capacity_mask: usize) -> usize;
}

pub trait ProbeSequenceBulk {
    type CapacityMask;

    unsafe fn load_capacity_mask(capacity: usize) -> Self::CapacityMask;

    unsafe fn start_indices(hashes: &[u64; 8], capacity_mask: Self::CapacityMask) -> [u64; 8];

    unsafe fn initial_strides(hashes: &[u64; 8], tags: &[u8; 0], capacity_mask: Self::CapacityMask) -> [usize; 8];

    unsafe fn next_bulk(previous: &[usize; 8], tag: &[u8; 8], state: &mut [usize; 8], capacity_mask: Self::CapacityMask) -> [usize; 8];
}

pub trait ProbeSequenceBulk32 {
    type CapacityMask;

    unsafe fn load_capacity_mask(capacity: usize) -> Self::CapacityMask;

    unsafe fn start_indices(hashes: &[u64; 32], capacity_mask: Self::CapacityMask) -> [u64; 32];
}

pub trait ProbeSequenceBulkN {
    // unsafe fn load_capacity_mask(capacity: usize) -> Self::CapacityMask;

    unsafe fn start_indices<const N: usize>(hashes: &[u64; N], capacity_mask: usize) -> [u64; N];
}
