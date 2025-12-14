pub trait ProbeSequence {
    #[inline(always)]
    fn start(hash: u64, capacity_mask: usize) -> (usize, usize);

    #[inline(always)]
    fn start_index(hash: u64, capacity_mask: usize) -> usize {
        Self::start(hash, capacity_mask).0
    }

    #[inline(always)]
    fn initial_stride(hash: u64, capacity_mask: usize) -> usize {
        Self::start(hash, capacity_mask).1
    }

    #[inline(always)]
    fn next(previous: usize, tag: u8, state: &mut usize, capacity_mask: usize) -> usize;
}

pub trait ProbeSequenceBulk {
    type CapacityMask;

    #[inline(always)]
    unsafe fn load_capacity_mask(capacity: usize) -> Self::CapacityMask;

    #[inline(always)]
    unsafe fn start_indices(hashes: &[u64; 8], capacity_mask: Self::CapacityMask) -> [u64; 8];

    #[inline(always)]
    unsafe fn initial_strides(hashes: &[u64; 8], tags: &[u8; 0], capacity_mask: Self::CapacityMask) -> [usize; 8];

    #[inline(always)]
    unsafe fn next_bulk(previous: &[usize; 8], tag: &[u8; 8], state: &mut [usize; 8], capacity_mask: Self::CapacityMask) -> [usize; 8];
}

pub trait ProbeSequenceBulk32 {
    type CapacityMask;

    #[inline(always)]
    unsafe fn load_capacity_mask(capacity: usize) -> Self::CapacityMask;

    #[inline(always)]
    unsafe fn start_indices(hashes: &[u64; 32], capacity_mask: Self::CapacityMask) -> [u64; 32];
}

pub trait ProbeSequenceBulkN {

    // #[inline(always)]
    // unsafe fn load_capacity_mask(capacity: usize) -> Self::CapacityMask;

    #[inline(always)]
    unsafe fn start_indices<const N: usize>(hashes: &[u64; N], capacity_mask: usize) -> [u64; N];
}

