use std::arch::aarch64;

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

pub struct HybridProbeSequence<const GROUP_SIZE: usize>;

impl <const GROUP_SIZE: usize> HybridProbeSequence<GROUP_SIZE> {
    #[inline(always)]
    pub fn start(hash: u64, capacity_mask: usize) -> usize {
        // debug_assert!((capacity_mask + 1).is_power_of_two());

        hash as usize & capacity_mask
    }

    #[inline(always)]
    pub fn next(previous: usize, tag: u8, capacity_mask: usize) -> usize {
        // debug_assert!((capacity_mask + 1).is_power_of_two());

        let stride = (tag as usize * 2 + 1) * GROUP_SIZE;
        (previous + stride) & capacity_mask
    }
}

impl <const GROUP_SIZE: usize> ProbeSequence for HybridProbeSequence<GROUP_SIZE> {
    #[inline(always)]
    fn start(hash: u64, capacity_mask: usize) -> (usize, usize) {
        (HybridProbeSequence::<GROUP_SIZE>::start(hash, capacity_mask), 0)
    }

    #[inline(always)]
    fn next(previous: usize, tag: u8, _: &mut usize, capacity_mask: usize) -> usize {
        HybridProbeSequence::<GROUP_SIZE>::next(previous, tag, capacity_mask)
    }
}

impl ProbeSequenceBulk for HybridProbeSequence<8> {

    type CapacityMask = aarch64::uint64x2_t;

    #[inline(always)]
    unsafe fn load_capacity_mask(capacity: usize) -> Self::CapacityMask {
        aarch64::vdupq_n_u64(capacity as u64)
    }

    #[inline(always)]
    unsafe fn start_indices(hashes: &[u64; 8], capacity_mask: Self::CapacityMask) -> [u64; 8] {
        let mut output = [0u64; 8];
        for i in [0, 2, 4, 6] {
            let hashes_register = aarch64::vld1q_u64(hashes.as_ptr().add(i));
            let result = aarch64::vandq_u64(hashes_register, capacity_mask);
            aarch64::vst1q_u64(output.as_mut_ptr().add(i), result);
        }

        output
    }

    #[inline(always)]
    unsafe fn initial_strides(hashes: &[u64; 8], tags: &[u8; 0], capacity_mask: Self::CapacityMask) -> [usize; 8] {
        let one_register = aarch64::vdup_n_u8(1);
        let group_size_register = aarch64::vdup_n_u8(8);

        let strides = aarch64::vand_u8(aarch64::vld1_u8(tags.as_ptr()), one_register);

        let mut output = [0usize; 8];
        // aarch64::vst1_u8(output.as_mut_ptr(), strides);

        output
    }

    #[inline(always)]
    unsafe fn next_bulk(previous: &[usize; 8], tag: &[u8; 8], state: &mut [usize; 8], capacity_mask: Self::CapacityMask) -> [usize; 8] {
        todo!()
    }
}

impl ProbeSequenceBulk32 for HybridProbeSequence<8> {

    type CapacityMask = aarch64::uint64x2_t;

    #[inline(always)]
    unsafe fn load_capacity_mask(capacity: usize) -> Self::CapacityMask {
        aarch64::vdupq_n_u64(capacity as u64)
    }

    #[inline(always)]
    unsafe fn start_indices(hashes: &[u64; 32], capacity_mask: Self::CapacityMask) -> [u64; 32] {
        let mut output = [0u64; 32];
        for (output, hashes) in output.array_chunks_mut::<2>()
            .zip(hashes.array_chunks::<2>()) {
            let hashes_register = aarch64::vld1q_u64(hashes.as_ptr());
            let result = aarch64::vandq_u64(hashes_register, capacity_mask);
            aarch64::vst1q_u64(output.as_mut_ptr(), result);
        }
        // for i in [0, 2, 4, 6] {
        //     let hashes_register = aarch64::vld1q_u64(hashes.as_ptr().add(i));
        //     let result = aarch64::vandq_u64(hashes_register, capacity_mask);
        //     aarch64::vst1q_u64(output.as_mut_ptr().add(i), result);
        // }

        output
    }
}

impl <const S: usize> ProbeSequenceBulkN for HybridProbeSequence<S> {
    #[inline(always)]
    unsafe fn start_indices<const N: usize>(hashes: &[u64; N], capacity_mask: usize) -> [u64; N] {
        let mut output = [0u64; N];
        for (output, hash) in output.iter_mut().zip(hashes.iter()) {
            *output = hash & (capacity_mask as u64);
        }

        output
    }
}

pub struct SwissTableProbeSeq<const GROUP_SIZE: usize>;

impl <const GROUP_SIZE: usize> ProbeSequence for SwissTableProbeSeq<GROUP_SIZE> {
    #[inline(always)]
    fn start(hash: u64, capacity_mask: usize) -> (usize, usize) {
        (hash as usize & capacity_mask, 0)
    }

    #[inline(always)]
    fn next(previous: usize, _: u8, stride: &mut usize, capacity_mask: usize) -> usize {
        *stride += GROUP_SIZE;
        (previous + *stride) & capacity_mask
    }
}
