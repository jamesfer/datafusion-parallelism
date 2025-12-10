use std::arch::aarch64;
use std::cmp::max;
use std::ops::DerefMut;
use crate::operator::version10::new_map_3::iterable_bit_mask::{IterableBitMaskIntrinsics16x4, IterableBitMaskIntrinsics8x4, IterableBitMaskIntrinsics8x8};
use crate::operator::version10::new_map_3::probe_sequence::{HybridProbeSequence, ProbeSequence, ProbeSequenceBulk, ProbeSequenceBulk32, ProbeSequenceBulkN, SwissTableProbeSeq};

// unsafe fn x() {
//     const MASK: u64 = 0b1000_0000_0100_0000_0010_0000_0001_0000_0000_1000_0000_0100_0000_0010_0000_0001;
//
//     let loaded_mask = aarch64::vreinterpret_u8_u64(aarch64::vld1_dup_u64(&MASK));
//
//     let masked_output = aarch64::vand_u8(123, loaded_mask);
//
//     let mut final_result = 0u8;
//
//     for i in 0..8 {
//         final_result |= aarch64::vget_lane_u8::<i>(masked_output);
//     }
//
//     let match_result: aarch64::uint8x8_t;
//
//     // aarch64::vshrn_n_u16::<4>(aarch64::vreinterpret_u16_u8(output));
//
//     // Only the lower 64 bits are really used
//     let larger_register = aarch64::vld1q_dup_u64(&aarch64::vget_lane_u64::<0>(aarch64::vreinterpret_u64_u8(match_result)));
//     // Halve the width of each match result, to 4 bits each. Only the lower 32 bits matter now,
//     // the upper bits are just duplicated
//     let halved = aarch64::vqshrn_n_u16::<7>(aarch64::vreinterpretq_u16_u64(larger_register));
//
//     let extracted = aarch64::vget_lane_u32::<0>(aarch64::vreinterpret_u32_u8(halved));
//
//     let mut result = 0u8;
//     for i in 0..8 {
//         result |= (extracted >> (i * 4)) as u8 & 1;
//     }
//     result
//
//     ()
// }


// Pretty good option
#[no_mangle]
pub fn load_ptr_halved(search: u8, values: *const u8) -> u8 {
    use std::arch::aarch64;

    unsafe {
        // Replicate the search value 16 times into a 128-bit register
        let search_register = aarch64::vld1_dup_u8(&search);
        let values_register = aarch64::vld1_u8(values);

        // Compare the registers together. For each u8 value in the 128-bit register, if the values
        // match, the output will have all 1s, otherwise all 0s.
        let output = aarch64::vceq_u8(values_register, search_register);

        // Only the lower 64 bits are really used
        let larger_register = aarch64::vld1q_dup_u64(&aarch64::vget_lane_u64::<0>(aarch64::vreinterpret_u64_u8(output)));
        // Halve the width of each match result, to 4 bits each. Only the lower 32 bits matter now,
        // the upper bits are just duplicated
        let halved = aarch64::vqshrn_n_u16::<7>(aarch64::vreinterpretq_u16_u64(larger_register));

        let extracted = aarch64::vget_lane_u32::<0>(aarch64::vreinterpret_u32_u8(halved));

        let mut result = 0u8;
        for i in 0..4 {
            // Compiles to assembly really efficiently
            result |= ((extracted & (0b11 << (i * 8)) >> (i * 8)) as u8) << (i * 2);
        }
        result
    }
}

#[inline]
unsafe fn compress_match_result(match_result: aarch64::uint8x8_t) -> u8 {
    // Load the 64 bits from the match result into a 128 bit register
    let larger_register = aarch64::vld1q_dup_u64(&aarch64::vget_lane_u64::<0>(aarch64::vreinterpret_u64_u8(match_result)));
    // Halve the width of each match result, to 4 bits each. Only the lower 32 bits matter now,
    // the upper bits are just duplicated
    let halved = aarch64::vqshrn_n_u16::<7>(aarch64::vreinterpretq_u16_u64(larger_register));

    let extracted = aarch64::vget_lane_u32::<0>(aarch64::vreinterpret_u32_u8(halved));

    // Extract the rest of the bits in loop
    let mut result = 0u8;
    for i in 0..4 {
        // Compiles to assembly really efficiently
        result |= ((extracted & (0b11 << (i * 8)) >> (i * 8)) as u8) << (i * 2);
    }
    result
}

// trait DerefsToSlice {
//
// }
//
// impl <T> DerefsToSlice for T
// where for<'a> &'a T: Into<&'a [u8]>
// {
//
// }

pub trait GroupStrategy {
    const GROUP_SIZE: usize;
    const EMPTY_TAG: u8;

    type Group;
    type ProbeSeq: ProbeSequence;
    type SliceType: AsMut<[u8]>;

    #[inline(always)]
    fn get_tag(hash: u64) -> u8;

    #[inline(always)]
    unsafe fn load(tags: &[u8]) -> Self::Group;

    #[inline(always)]
    unsafe fn load_ptr(tags: *const u8) -> Self::Group;

    #[inline(always)]
    unsafe fn match_tag(group: &Self::Group, search_tag: u8) -> impl IntoIterator<Item=usize>;

    #[inline(always)]
    unsafe fn match_tag_as_u8(group: &Self::Group, search_tag: u8) -> u8;

    #[inline(always)]
    unsafe fn match_empty(group: &Self::Group) -> impl IntoIterator<Item=usize>;

    #[inline(always)]
    unsafe fn contains_empty_slot(group: &Self::Group) -> bool;

    #[inline(always)]
    fn allocate_slice() -> Self::SliceType;
}

pub trait IterableGroupStrategy: GroupStrategy {
    type It: IntoIterator<Item=usize>;

    unsafe fn match_non_empty(group: &Self::Group) -> Self::It;
}

pub trait BulkGroupStrategy: GroupStrategy {
    type ProbeSeq: ProbeSequenceBulk;

    #[inline(always)]
    unsafe fn get_tags(hashes: &[u64; 8]) -> [u8; 8];
}

pub trait BulkGroupStrategy32: GroupStrategy {
    type ProbeSeq: ProbeSequenceBulk32;

    #[inline(always)]
    unsafe fn get_tags(hashes: &[u64; 32]) -> [u8; 32];
}

pub trait BulkGroupStrategyN: GroupStrategy {
    type ProbeSeq: ProbeSequenceBulkN;
    type TagIt: IntoIterator<Item=usize>;

    #[inline(always)]
    unsafe fn get_tags<const N: usize>(hashes: &[u64; N]) -> [u8; N];

    #[inline(always)]
    unsafe fn match_tag_n<const N: usize>(group: &[*const u8; N], search_tag: &[u8; N]) -> [Self::TagIt; N];

    #[inline(always)]
    unsafe fn match_tag_1(group: &Self::Group, search_tag: u8) -> Self::TagIt;
}

#[derive(Copy, Clone)]
pub struct Group4;

impl GroupStrategy for Group4 {
    const GROUP_SIZE: usize = 4;
    const EMPTY_TAG: u8 = 0;
    type Group = aarch64::uint8x8_t;
    type ProbeSeq = HybridProbeSequence<4>;
    type SliceType = [u8; 4];

    #[inline(always)]
    fn get_tag(hash: u64) -> u8 {
        max((hash >> 56) as u8, 1)
    }

    #[inline(always)]
    unsafe fn load(tags: &[u8]) -> Self::Group {
        debug_assert!(tags.len() >= Self::GROUP_SIZE);
        aarch64::vcreate_u8(u32::from_le_bytes([
            tags[0],
            tags[1],
            tags[2],
            tags[3],
        ]) as u64)
    }

    #[inline(always)]
    unsafe fn load_ptr(tags: *const u8) -> Self::Group {
        aarch64::vcreate_u8(u32::from_le_bytes([
            *tags.add(0),
            *tags.add(1),
            *tags.add(2),
            *tags.add(3),
        ]) as u64)
    }

    #[inline(always)]
    unsafe fn match_tag(group: &Self::Group, search_tag: u8) -> IterableBitMaskIntrinsics8x4 {
        // Replicate the search value 8 times into a 64-bit register
        let search_register = aarch64::vld1_dup_u8(&search_tag);

        // Compare the registers together. For each u8 value in the 64-bit register, if the values
        // match, the output will have all 1s, otherwise all 0s.
        let match_result = aarch64::vceq_u8(*group, search_register);

        let output = aarch64::vget_lane_u32::<0>(aarch64::vreinterpret_u32_u8(match_result));

        IterableBitMaskIntrinsics8x4::new(output)
    }

    unsafe fn match_tag_as_u8(group: &Self::Group, search_tag: u8) -> u8 {
        // Replicate the search value 8 times into a 64-bit register
        let search_register = aarch64::vld1_dup_u8(&search_tag);

        // Compare the registers together. For each u8 value in the 64-bit register, if the values
        // match, the output will have all 1s, otherwise all 0s.
        let match_result = aarch64::vceq_u8(*group, search_register);

        compress_match_result(match_result)
    }

    #[inline(always)]
    unsafe fn match_empty(group: &Self::Group) -> IterableBitMaskIntrinsics8x4 {
        Self::match_tag(group, Self::EMPTY_TAG)
    }

    #[inline(always)]
    unsafe fn contains_empty_slot(group: &Self::Group) -> bool {
        Self::match_empty(group).any_bit_set()
    }

    #[inline(always)]
    fn allocate_slice() -> Self::SliceType {
        [0u8; 4]
    }
}

impl BulkGroupStrategyN for Group4 {
    type ProbeSeq = HybridProbeSequence<4>;
    type TagIt = IterableBitMaskIntrinsics8x4;

    unsafe fn get_tags<const N: usize>(hashes: &[u64; N]) -> [u8; N] {
        hashes.map(|hash| max((hash >> 56) as u8, 1))
    }

    unsafe fn match_tag_n<const N: usize>(group: &[*const u8; N], search_tag: &[u8; N]) -> [Self::TagIt; N] {
        assert_eq!(N % 2, 0);

        let mut output_data = [IterableBitMaskIntrinsics8x4::new_unchecked(0); N];
        let (search_chunks, _) = search_tag.as_chunks::<4>();
        let (group_chunks, _) = group.as_chunks::<4>();
        let (output_chunks, _) = output_data.as_chunks_mut::<4>();
        for ((search_tags, group), output_data) in search_chunks.iter()
            .zip(group_chunks.iter())
            .zip(output_chunks.iter_mut()) {

            // Loads 4 u8 values into 4 64 bit registers by duplicating all the values
            let search_tags_x4 = interleave_x4_d(search_tags);

            let groups_x4 = load_x4_d(group);

            // Compare the registers together. For each u8 value in the 64-bit register, if the values
            // match, the output will have all 1s, otherwise all 0s.
            let match_result = aarch64::vceqq_u8(search_tags_x4, groups_x4);

            // Mask the result to only keep the first bit per byte, as this is what the iterator
            // type needs
            let masked_result = aarch64::vandq_u8(match_result, aarch64::vld1q_dup_u8(&1));

            aarch64::vst1q_u32(
                IterableBitMaskIntrinsics8x4::reinterpret_as_u32s(output_data).as_mut_ptr(),
                aarch64::vreinterpretq_u32_u8(masked_result),
            );
        }

        output_data
    }

    unsafe fn match_tag_1(group: &Self::Group, search_tag: u8) -> Self::TagIt {
        Self::match_tag(group, search_tag)
    }
}

#[inline(always)]
unsafe fn interleave_x4(search_tags: [u8; 4]) -> aarch64::uint8x16_t {
    let many_u8_register = aarch64::vld4_dup_u8(search_tags.as_ptr());
    let left = aarch64::vreinterpret_u8_u32(aarch64::vzip1_u32(aarch64::vreinterpret_u32_u8(many_u8_register.0), aarch64::vreinterpret_u32_u8(many_u8_register.1)));
    let right = aarch64::vreinterpret_u8_u32(aarch64::vzip1_u32(aarch64::vreinterpret_u32_u8(many_u8_register.2), aarch64::vreinterpret_u32_u8(many_u8_register.3)));
    let search_tags_x4 = aarch64::vcombine_u8(left, right);
    search_tags_x4
}

#[inline(always)]
unsafe fn interleave_x4_b(search_tags: &[u8; 4]) -> aarch64::uint8x16_t {
    let u32 = u32::from_le_bytes(*search_tags);
    let u8buf = aarch64::vcreate_u8(u32 as u64);
    let x8buf_x2 = aarch64::vzip1_u8(u8buf, u8buf);
    let x8buf_x2 = aarch64::vzip_u8(x8buf_x2, x8buf_x2);
    let r = aarch64::vcombine_u8(x8buf_x2.0, x8buf_x2.1);
    r
}

#[inline(always)]
unsafe fn interleave_x4_d(search_tags: &[u8; 4]) -> aarch64::uint8x16_t {
    let b = [
        search_tags[0],
        search_tags[0],
        search_tags[0],
        search_tags[0],
        search_tags[1],
        search_tags[1],
        search_tags[1],
        search_tags[1],
        search_tags[2],
        search_tags[2],
        search_tags[2],
        search_tags[2],
        search_tags[3],
        search_tags[3],
        search_tags[3],
        search_tags[3],
    ];
    aarch64::vld1q_u8(b.as_ptr())
}

#[inline(always)]
unsafe fn interleave_x4_c(search_tags: [u8; 4]) -> aarch64::uint8x16_t {
    // let a = aarch64::vcreate_u8(search_tags[0] as u64);
    // let b = aarch64::vcreate_u8(search_tags[1] as u64);
    // let c = aarch64::vcreate_u8(search_tags[2] as u64);
    // let d = aarch64::vcreate_u8(search_tags[3] as u64);
    let v = aarch64::vcreate_u8(0);
    let v = aarch64::vset_lane_u8::<0>(search_tags[0], v);
    let v = aarch64::vset_lane_u8::<1>(search_tags[1], v);
    let v = aarch64::vset_lane_u8::<2>(search_tags[2], v);
    let v = aarch64::vset_lane_u8::<3>(search_tags[3], v);

    let v2 = aarch64::vzip1_u8(v, v);
    let v4 = aarch64::vzip_u8(v2, v2);
    let r = aarch64::vcombine_u8(v4.0, v4.1);


    // let ac = aarch64::vzip1_u8(a, c);
    // let bd = aarch64::vzip1_u8(b, d);
    // let abcd = aarch64::vzip1_u8(ac, bd);
    // let abcd_x2 = aarch64::vzip1_u8(abcd, abcd);
    // let abcd_x4 = aarch64::vzip_u8(abcd_x2, abcd_x2);
    // let r = aarch64::vcombine_u8(abcd_x4.0, abcd_x4.1);
    r
}

#[cfg(test)]
mod interleave_x4_tests {
    use std::arch::aarch64;

    #[test]
    fn test_a() {
        let search_tags = [1, 2, 3, 4];
        let interleaved = unsafe { super::interleave_x4(search_tags) };

        let values = as_vec(interleaved);
        assert_eq!(values, [
            1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3, 4, 4, 4, 4,
        ]);
    }

    #[test]
    fn test_b() {
        let search_tags = [1, 2, 3, 4];
        let interleaved = unsafe { super::interleave_x4_b(&search_tags) };

        let values = as_vec(interleaved);
        assert_eq!(values, [
            1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3, 4, 4, 4, 4,
        ]);
    }

    #[test]
    fn test_c() {
        let search_tags = [1, 2, 3, 4];
        let interleaved = unsafe { super::interleave_x4_c(search_tags) };

        let values = as_vec(interleaved);
        assert_eq!(values, [
            1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3, 4, 4, 4, 4,
        ]);
    }

    #[test]
    fn test_d() {
        let search_tags = [1, 2, 3, 4];
        let interleaved = unsafe { super::interleave_x4_d(&search_tags) };

        let values = as_vec(interleaved);
        assert_eq!(values, [
            1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3, 4, 4, 4, 4,
        ]);
    }

    fn as_vec(v: aarch64::uint8x16_t) -> [u8; 16] {
        unsafe {
            [
                aarch64::vgetq_lane_u8::<0>(v),
                aarch64::vgetq_lane_u8::<1>(v),
                aarch64::vgetq_lane_u8::<2>(v),
                aarch64::vgetq_lane_u8::<3>(v),
                aarch64::vgetq_lane_u8::<4>(v),
                aarch64::vgetq_lane_u8::<5>(v),
                aarch64::vgetq_lane_u8::<6>(v),
                aarch64::vgetq_lane_u8::<7>(v),
                aarch64::vgetq_lane_u8::<8>(v),
                aarch64::vgetq_lane_u8::<9>(v),
                aarch64::vgetq_lane_u8::<10>(v),
                aarch64::vgetq_lane_u8::<11>(v),
                aarch64::vgetq_lane_u8::<12>(v),
                aarch64::vgetq_lane_u8::<13>(v),
                aarch64::vgetq_lane_u8::<14>(v),
                aarch64::vgetq_lane_u8::<15>(v),
            ]
        }
    }
}

#[inline(always)]
unsafe fn load_x4_d(groups: &[*const u8; 4]) -> aarch64::uint8x16_t {
    let b = [
        *groups[0].add(0),
        *groups[0].add(1),
        *groups[0].add(2),
        *groups[0].add(3),
        *groups[1].add(0),
        *groups[1].add(1),
        *groups[1].add(2),
        *groups[1].add(3),
        *groups[2].add(0),
        *groups[2].add(1),
        *groups[2].add(2),
        *groups[2].add(3),
        *groups[3].add(0),
        *groups[3].add(1),
        *groups[3].add(2),
        *groups[3].add(3),
    ];
    aarch64::vld1q_u8(b.as_ptr())
}




#[derive(Copy, Clone)]
pub struct Group8(aarch64::uint8x8_t);

impl Group8 {
    #[inline(always)]
    pub unsafe fn load(tags: &[u8]) -> Self {
        debug_assert!(tags.len() >= 8);
        Self(aarch64::vld1_u8(&tags[0]))
    }

    #[inline(always)]
    pub unsafe fn load_ptr(tags: *const u8) -> Self {
        let x = [0u8; 8];
        Self(aarch64::vld1_u8(tags))
    }

    #[inline(always)]
    pub unsafe fn find(&self, search_tag: u8) -> IterableBitMaskIntrinsics8x8 {
        // Replicate the search value 8 times into a 64-bit register
        let search_register = aarch64::vld1_dup_u8(&search_tag);

        // Compare the registers together. For each u8 value in the 64-bit register, if the values
        // match, the output will have all 1s, otherwise all 0s.
        let match_result = aarch64::vceq_u8(self.0, search_register);

        let output = aarch64::vget_lane_u64::<0>(aarch64::vreinterpret_u64_u8(match_result));

        IterableBitMaskIntrinsics8x8::new(output)
    }

    // #[inline(always)]
    // pub unsafe fn findx2(&self, search_tag: &[u8; 2]) -> IterableBitMaskIntrinsics8x8 {
    //     // Replicate the search value 8 times into a 64-bit register
    //     let search_register = aarch64::vld1q_dup_u8(&search_tag[0]);
    //     aarch64::vld1q_lane_u8::<1>(&search_tag[1], search_register);
    //
    //     // Compare the registers together. For each u8 value in the 64-bit register, if the values
    //     // match, the output will have all 1s, otherwise all 0s.
    //     let match_result = aarch64::vceq_u8(self.0, search_register);
    //
    //     let output = aarch64::vget_lane_u64::<0>(aarch64::vreinterpret_u64_u8(match_result));
    //
    //     IterableBitMaskIntrinsics8x8::new(output)
    // }

    #[inline(always)]
    pub unsafe fn match_tag_as_u8(&self, search_tag: u8) -> u8 {
        // Replicate the search value 8 times into a 64-bit register
        let search_register = aarch64::vld1_dup_u8(&search_tag);

        // Compare the registers together. For each u8 value in the 64-bit register, if the values
        // match, the output will have all 1s, otherwise all 0s.
        let match_result = aarch64::vceq_u8(self.0, search_register);

        compress_match_result(match_result)
    }

    #[inline(always)]
    pub unsafe fn match_empty(&self) -> IterableBitMaskIntrinsics8x8 {
        self.find(0)
    }
}

impl GroupStrategy for Group8 {
    const GROUP_SIZE: usize = 8;
    const EMPTY_TAG: u8 = 0;
    type Group = Self;
    type ProbeSeq = HybridProbeSequence<8>;
    type SliceType = [u8; 8];

    #[inline(always)]
    fn get_tag(hash: u64) -> u8 {
        max((hash >> 56) as u8, 1)
    }

    #[inline(always)]
    unsafe fn load(tags: &[u8]) -> Self::Group {
        Self::load(tags)
    }

    #[inline(always)]
    unsafe fn load_ptr(tags: *const u8) -> Self::Group {
        Self::load_ptr(tags)
    }

    #[inline(always)]
    unsafe fn match_tag(group: &Self::Group, search_tag: u8) -> impl IntoIterator<Item=usize> {
        group.find(search_tag)
    }

    unsafe fn match_tag_as_u8(group: &Self::Group, search_tag: u8) -> u8 {
        group.match_tag_as_u8(search_tag)
    }

    #[inline(always)]
    unsafe fn match_empty(group: &Self::Group) -> impl IntoIterator<Item=usize> {
        group.match_empty()
    }

    #[inline(always)]
    unsafe fn contains_empty_slot(group: &Self::Group) -> bool {
        group.match_empty().any_bit_set()
    }

    #[inline(always)]
    fn allocate_slice() -> Self::SliceType {
        [0u8; 8]
    }
}

impl IterableGroupStrategy for Group8 {
    type It = IterableBitMaskIntrinsics8x8;
    
    unsafe fn match_non_empty(group: &Self::Group) -> Self::It {
        // Replicate the search value 8 times into a 64-bit register
        let search_register = aarch64::vld1_dup_u8(&0);

        // Compare the registers together. For each u8 value in the 64-bit register, if the values
        // match, the output will have all 1s, otherwise all 0s.
        let match_result = aarch64::vceq_u8(group.0, search_register);

        let output = aarch64::vget_lane_u64::<0>(aarch64::vreinterpret_u64_u8(match_result));

        IterableBitMaskIntrinsics8x8::new(!output)
    }
}

impl BulkGroupStrategyN for Group8 {
    type ProbeSeq = HybridProbeSequence<8>;
    type TagIt = IterableBitMaskIntrinsics8x8;

    #[inline(always)]
    unsafe fn get_tags<const N: usize>(hashes: &[u64; N]) -> [u8; N] {
        let mut output = [0u8; N];
        // let one_register = aarch64::vld1_dup_u8(&1);

        // for (output, hashes) in output.array_chunks_mut::<8>()
        //     .zip(hashes.array_chunks::<8>()){
        //     // Perform an interlaced load
        //     let aarch64::uint8x16x4_t(_, _, _, b) = aarch64::vld4q_u8(hashes.as_ptr().cast());
        //     // Skip every second byte
        //     let lower_bits = aarch64::vshrn_n_u16::<8>(aarch64::vreinterpretq_u16_u8(b));
        //     let max = aarch64::vmax_u8(lower_bits, one_register);
        //
        //     aarch64::vst1_u8(output.as_mut_ptr(), max);
        // }

        for (output, hash) in output.iter_mut()
            .zip(hashes.iter()){
            *output = max((hash >> 56) as u8, 1u8);
        }

        output
    }

    #[inline(always)]
    unsafe fn match_tag_n<const N: usize>(group: &[*const u8; N], search_tag: &[u8; N]) -> [Self::TagIt; N] {
        assert_eq!(N % 2, 0);

        let mut output_data = [IterableBitMaskIntrinsics8x8::new_unchecked(0); N];
        let (search_chunks, _) = search_tag.as_chunks::<2>();
        let (group_chunks, _) = group.as_chunks::<2>();
        let (output_chunks, _) = output_data.as_chunks_mut::<2>();
        for ((search_tags, group), output_data) in search_chunks.iter()
            .zip(group_chunks.iter())
            .zip(output_chunks.iter_mut()) {
            // Replicate the search value 8 times into a 64-bit register
            // let search_register = aarch64::vld1_dup_u8(&search_tag[left]);

            // Loads 2 u8 values into 2 64 bit registers by duplicating all the values
            let double_u8_register = aarch64::vld2_dup_u8(search_tags.as_ptr());
            let search_tags_x8 = aarch64::vcombine_u8(double_u8_register.0, double_u8_register.1);
            // let search_tags_x8 = aarch64::vreinterpretq_u8_u64(aarch64::vcombine_u64(
            //     aarch64::vreinterpret_u64_u8(double_u8_register.0),
            //     aarch64::vreinterpret_u64_u8(double_u8_register.1),
            // ));
            // let u64_store = [
            //     aarch64::vget_lane_u64::<0>(aarch64::vreinterpret_u64_u8(double_u8_register.0)),
            //     aarch64::vget_lane_u64::<0>(aarch64::vreinterpret_u64_u8(double_u8_register.1)),
            // ];
            // let search_tags_x8 = aarch64::vreinterpretq_u8_u64(aarch64::vld1q_u64(u64_store.as_ptr()));

            let groups_x8 = aarch64::vcombine_u8(aarch64::vld1_u8(group[0]), aarch64::vld1_u8(group[1]));
            // let groups_x8 = aarch64::vreinterpretq_u8_u64(aarch64::vcombine_u64(
            //     aarch64::vreinterpret_u64_u8(aarch64::vld1_u8(group[0])),
            //     aarch64::vreinterpret_u64_u8(aarch64::vld1_u8(group[1])),
            // ));
            // let u64_store = [
            //     aarch64::vget_lane_u64::<0>(aarch64::vreinterpret_u64_u8(aarch64::vld1_u8(group[0]))),
            //     aarch64::vget_lane_u64::<0>(aarch64::vreinterpret_u64_u8(aarch64::vld1_u8(group[1]))),
            // ];
            // let groups_x8 = aarch64::vreinterpretq_u8_u64(aarch64::vld1q_u64(u64_store.as_ptr()));

            // Compare the registers together. For each u8 value in the 64-bit register, if the values
            // match, the output will have all 1s, otherwise all 0s.
            let match_result = aarch64::vceqq_u8(groups_x8, search_tags_x8);

            // Mask the result to only keep the first bit per byte, as this is what the iterator
            // type needs
            let masked_result = aarch64::vandq_u8(match_result, aarch64::vld1q_dup_u8(&1));

            aarch64::vst1q_u64(
                IterableBitMaskIntrinsics8x8::reinterpret_as_u64s(output_data).as_mut_ptr(),
                aarch64::vreinterpretq_u64_u8(masked_result),
            );
        }

        output_data
    }

    #[inline(always)]
    unsafe fn match_tag_1(group: &Self::Group, search_tag: u8) -> Self::TagIt {
        group.find(search_tag)
    }
}

#[cfg(test)]
mod group8_bulk_n_tests {
    use crate::operator::version10::new_map_3::group::{BulkGroupStrategyN, Group8};

    #[test]
    pub fn test() {
        let search_tags = [
            0b00000001,
            0b00000010,
            0b00000100,
            0b00001000,
            0b00010000,
            0b00100000,
            0b01000000,
            0b10000000
        ];

        let data = [
            0b00000001,
            0b00000010,
            0b00000100,
            0b00001000,
            0b00001000,
            0b00000100,
            0b00000010,
            0b00000001
        ];
        let groups: [*const u8; 8] = [data.as_ptr(); 8];

        let iterators = unsafe { Group8::match_tag_n::<8>(&groups, &search_tags) };
        let indices = iterators.map(|it| it.into_iter().collect::<Vec<_>>());
        assert_eq!(indices, [
            vec![0, 7],
            vec![1, 6],
            vec![2, 5],
            vec![3, 4],
            vec![],
            vec![],
            vec![],
            vec![],
        ])
    }
}

impl BulkGroupStrategy32 for Group8 {
    type ProbeSeq = HybridProbeSequence<8>;

    #[inline(always)]
    unsafe fn get_tags(hashes: &[u64; 32]) -> [u8; 32] {
        let mut output = [0u8; 32];
        let one_register = aarch64::vld1_dup_u8(&1);

        // for (output, hashes) in output.array_chunks_mut::<8>()
        //     .zip(hashes.array_chunks::<8>()){
        //     // Perform an interlaced load
        //     let aarch64::uint8x16x4_t(_, _, _, b) = aarch64::vld4q_u8(hashes.as_ptr().cast());
        //     // Skip every second byte
        //     let lower_bits = aarch64::vshrn_n_u16::<8>(aarch64::vreinterpretq_u16_u8(b));
        //     let max = aarch64::vmax_u8(lower_bits, one_register);
        //
        //     aarch64::vst1_u8(output.as_mut_ptr(), max);
        // }

        for (output, hash) in output.iter_mut()
            .zip(hashes.iter()){
            *output = max((hash >> 56) as u8, 1u8);
        }

        output
    }
}

impl BulkGroupStrategy for Group8 {
    type ProbeSeq = HybridProbeSequence<8>;

    // unsafe fn get_tags_3(hashes: &[u64; 8]) -> [u8; 8] {
    //     let ones_register = aarch64::vld1_dup_u32(&1);
    //     let mut output = [0u8; 8];
    //
    //     for i in 0..4 {
    //         let hashes_register = aarch64::vld1q_u64(hashes.as_ptr().add(i * 2));
    //         let shifted = aarch64::vshrq_n_u64::<56>(hashes_register);
    //         let u8_register = aarch64::vreinterpretq_u8_u64(shifted);
    //
    //         let shrunk = aarch64::vmovn_u64(shifted);
    //         let max = aarch64::vmax_u32(shrunk, ones_register);
    //         let u8_register = aarch64::vreinterpret_u8_u32(max);
    //         output[i * 2] = aarch64::vget_lane_u8::<0>(u8_register);
    //         output[i * 2 + 1] = aarch64::vget_lane_u8::<4>(u8_register);
    //     }
    //
    //     output
    // }
    //
    // unsafe fn get_tags_2(hashes: &[u64; 8]) -> [u8; 8] {
    //     let ones_register = aarch64::vld1_dup_u32(&1);
    //     let mut output = [0u8; 8];
    //
    //     for i in 0..4 {
    //         let hashes_register = aarch64::vld1q_u64(hashes.as_ptr().add(i * 2));
    //         let shifted = aarch64::vshrq_n_u64::<56>(hashes_register);
    //         let shrunk = aarch64::vmovn_u64(shifted);
    //         let max = aarch64::vmax_u32(shrunk, ones_register);
    //         let u8_register = aarch64::vreinterpret_u8_u32(max);
    //         output[i * 2] = aarch64::vget_lane_u8::<0>(u8_register);
    //         output[i * 2 + 1] = aarch64::vget_lane_u8::<4>(u8_register);
    //     }
    //
    //     output
    // }
    //
    // #[inline(always)]
    // unsafe fn get_tags_1b(hashes: &[u64; 8]) -> [u8; 8] {
    //     let mut output = [0u8; 8];
    //
    //     for i in 0..8 {
    //         output[i] = hashes[i].to_be_bytes()[0];
    //     }
    //
    //     let tags_register = aarch64::vld1_u8(output.as_ptr());
    //
    //     let one_register = aarch64::vld1_dup_u8(&1);
    //     let max = aarch64::vmax_u8(tags_register, one_register);
    //
    //     aarch64::vst1_u8(output.as_mut_ptr(), max);
    //
    //     output
    // }


    #[inline(always)]
    unsafe fn get_tags(hashes: &[u64; 8]) -> [u8; 8] {
        // Perform an interlaced load
        let aarch64::uint8x16x4_t(_, _, _, b) = aarch64::vld4q_u8(hashes.as_ptr().cast());

        // Skip every second byte
        let lower_bits = aarch64::vshrn_n_u16::<8>(aarch64::vreinterpretq_u16_u8(b));


        let one_register = aarch64::vld1_dup_u8(&1);
        let max = aarch64::vmax_u8(lower_bits, one_register);

        let mut output = [0u8; 8];
        aarch64::vst1_u8(output.as_mut_ptr(), max);

        output
    }


    // #[inline(always)]
    // unsafe fn get_tags(hashes: &[u64; 8]) -> [u8; 8] {
    //     let mut tags_register = aarch64::vld1_u8(&0);
    //     tags_register = aarch64::vld1_lane_u8::<0>(&hashes[0].to_be_bytes()[0], tags_register);
    //     tags_register = aarch64::vld1_lane_u8::<1>(&hashes[1].to_be_bytes()[0], tags_register);
    //     tags_register = aarch64::vld1_lane_u8::<2>(&hashes[2].to_be_bytes()[0], tags_register);
    //     tags_register = aarch64::vld1_lane_u8::<3>(&hashes[3].to_be_bytes()[0], tags_register);
    //     tags_register = aarch64::vld1_lane_u8::<4>(&hashes[4].to_be_bytes()[0], tags_register);
    //     tags_register = aarch64::vld1_lane_u8::<5>(&hashes[5].to_be_bytes()[0], tags_register);
    //     tags_register = aarch64::vld1_lane_u8::<6>(&hashes[6].to_be_bytes()[0], tags_register);
    //     tags_register = aarch64::vld1_lane_u8::<7>(&hashes[7].to_be_bytes()[0], tags_register);
    //
    //
    //
    //     // let mut top_bits = [0u8; 8];
    //     //
    //     // let aarch64::uint8x8x4_t(x, y, a, b) = aarch64::vld4_u8(hashes.as_ptr().cast());
    //     // let aarch64::uint8x8x4_t(w, z, c, d) = aarch64::vld4_u8(hashes.as_ptr().add(4).cast());
    //     //
    //     // // b and d contain the 4th and 8th bytes of each of the hashes. b[0] is the 4th byte of
    //     // // first hash, and b[1] is the 8th byte of the first hash (the highest byte), etc.
    //     // println!("Registers after load");
    //     // for r in [x, y, a, b, w, z, c, d].iter() {
    //     //     println!("{:0>2x?}", r);
    //     // }
    //     //
    //     // let mut all_top_bits = [0u8; 16];
    //     // aarch64::vget_lane_u8()
    //     // aarch64::vst2_u8(all_top_bits.as_mut_ptr(), aarch64::uint8x8x2_t(b, d));
    //     // aarch64::vst2_u8(top_bits.as_mut_ptr().add(4), aarch64::uint8x8x2_t(c, d));
    //
    //     // println!("Top bits after load");
    //     // for (index, byte) in all_top_bits.iter().enumerate() {
    //     //     println!("{}: 0b{:0>2x?}", index, byte);
    //     // }
    //
    //     // aarch64::vqshrn_n_u16()
    //     // aarch64::uint64x2x4_t
    //     // let aarch64::uint64x2x4_t(a, b, c, d) = aarch64::vld1q_u64_x4(hashes.as_ptr());
    //     // aarch64::vshrn_n_u64()
    //
    //     // for i in 0..4 {
    //     //     let buffer = aarch64::vld1q_u64(hashes.as_ptr().add(i * 2));
    //     //     let shifted = aarch64::vshrq_n_u64::<56>(buffer);
    //     //     let shifted_as_u8 = aarch64::vreinterpretq_u8_u64(shifted);
    //     //     top_bits[i * 2] = aarch64::vgetq_lane_u8::<0>(shifted_as_u8);
    //     //     top_bits[i * 2 + 1] = aarch64::vgetq_lane_u8::<8>(shifted_as_u8);
    //     // }
    //
    //     // println!("{:0>2x?}", top_bits);
    //     // let tag_register = aarch64::vld1_u8(top_bits.as_ptr());
    //
    //     let one_register = aarch64::vld1_dup_u8(&1);
    //     let max = aarch64::vmax_u8(tags_register, one_register);
    //
    //     let mut output = [0u8; 8];
    //     // output[0] = aarch64::vget_lane_u8::<0>(max);
    //     // output[1] = aarch64::vget_lane_u8::<1>(max);
    //     // output[2] = aarch64::vget_lane_u8::<2>(max);
    //     // output[3] = aarch64::vget_lane_u8::<3>(max);
    //     // output[4] = aarch64::vget_lane_u8::<4>(max);
    //     // output[5] = aarch64::vget_lane_u8::<5>(max);
    //     // output[6] = aarch64::vget_lane_u8::<6>(max);
    //     // output[7] = aarch64::vget_lane_u8::<7>(max);
    //
    //     aarch64::vst1_u8(output.as_mut_ptr(), max);
    //
    //     // aarch64::vst1_u8(output.as_mut_ptr(), max);
    //     // aarch64::vget_lane_u64::<0>(aarch64::vreinterpret_u64_u8(max)).to_be_bytes()
    //     output
    // }
}



#[cfg(test)]
mod group8_tests {
    use crate::operator::version10::new_map_3::group::{BulkGroupStrategy, Group8};

    #[test]
    fn test_get_tags() {
        let mut hashes = [0u64; 8];
        for (index, hash) in hashes.iter_mut().enumerate() {
            *hash = 1 << (55 + index);
        }

        println!("Hashes as numbers {:?}", hashes);
        for hash in hashes {
            println!("0b{:0>64b} : {}", hash, hash >> 56);
        }

        let tags = unsafe { Group8::get_tags(&hashes) };
        assert_eq!(tags, [1, 1, 2, 4, 8, 16, 32, 64]);
    }
}

#[derive(Copy, Clone)]
pub struct Group16(aarch64::uint8x16_t);

impl Group16 {
    #[inline(always)]
    pub unsafe fn load(tags: &[u8]) -> Self {
        debug_assert!(tags.len() >= 16);
        Self(aarch64::vld1q_u8(&tags[0]))
    }

    #[inline(always)]
    pub unsafe fn load_ptr(tags: *const u8) -> Self {
        Self(aarch64::vld1q_u8(tags))
    }

    #[inline(always)]
    pub unsafe fn find(&self, search_tag: u8) -> IterableBitMaskIntrinsics16x4 {
        // Replicate the search value 16 times into a 128-bit register
        let search_register = aarch64::vld1q_dup_u8(&search_tag);

        // Compare the registers together. For each u8 value in the 128-bit register, if the values
        // match, the output will have all 1s, otherwise all 0s.
        let output = aarch64::vceqq_u8(self.0, search_register);

        // Shrink the 128 bit result into a 64 bit value, where the result of each comparison is 4
        // bits wide
        let shifted = aarch64::vshrn_n_u16::<4>(aarch64::vreinterpretq_u16_u8(output));

        let output = aarch64::vget_lane_u64::<0>(aarch64::vreinterpret_u64_u8(shifted));

        IterableBitMaskIntrinsics16x4::new(output)
    }

    #[inline(always)]
    pub unsafe fn match_empty(&self) -> IterableBitMaskIntrinsics16x4 {
        self.find(0)
    }
}

impl GroupStrategy for Group16 {
    const GROUP_SIZE: usize = 16;
    const EMPTY_TAG: u8 = 0;
    type Group = Group16;
    type ProbeSeq = HybridProbeSequence<16>;
    type SliceType = [u8; 16];

    #[inline(always)]
    fn get_tag(hash: u64) -> u8 {
        max((hash >> 56) as u8, 1)
    }

    #[inline(always)]
    unsafe fn load(tags: &[u8]) -> Self::Group {
        Self::load(tags)
    }

    #[inline(always)]
    unsafe fn load_ptr(tags: *const u8) -> Self::Group {
        Self::load_ptr(tags)
    }

    #[inline(always)]
    unsafe fn match_tag(group: &Self::Group, search_tag: u8) -> impl IntoIterator<Item=usize> {
        group.find(search_tag)
    }

    unsafe fn match_tag_as_u8(group: &Self::Group, search_tag: u8) -> u8 {
        todo!()
    }

    #[inline(always)]
    unsafe fn match_empty(group: &Self::Group) -> impl IntoIterator<Item=usize> {
        group.match_empty()
    }

    #[inline(always)]
    unsafe fn contains_empty_slot(group: &Self::Group) -> bool {
        group.match_empty().any_bit_set()
    }

    #[inline(always)]
    fn allocate_slice() -> Self::SliceType {
        [0u8; 16]
    }
}

#[derive(Copy, Clone)]
pub struct Group16Swiss(aarch64::uint8x16_t);

impl Group16Swiss {
    #[inline(always)]
    pub unsafe fn load(tags: &[u8]) -> Self {
        debug_assert!(tags.len() >= 16);
        Self(aarch64::vld1q_u8(&tags[0]))
    }

    #[inline(always)]
    pub unsafe fn load_ptr(tags: *const u8) -> Self {
        Self(aarch64::vld1q_u8(tags))
    }

    #[inline(always)]
    pub unsafe fn find(&self, search_tag: u8) -> IterableBitMaskIntrinsics16x4 {
        // Replicate the search value 8 times into a 64-bit register
        let search_register = aarch64::vld1q_dup_u8(&search_tag);

        // Compare the registers together. For each u8 value in the 64-bit register, if the values
        // match, the output will have all 1s, otherwise all 0s.
        let output = aarch64::vceqq_u8(self.0, search_register);

        let shrunk = aarch64::vshrn_n_u16::<4>(aarch64::vreinterpretq_u16_u8(output));

        // Reinterpret the 16-lane u8 type into a 2-lane 64-bit type, and return it as a regular
        // rust type
        let output = aarch64::vget_lane_u64::<0>(aarch64::vreinterpret_u64_u8(shrunk));

        IterableBitMaskIntrinsics16x4::new(output)
    }

    #[inline(always)]
    pub unsafe fn match_empty(&self) -> IterableBitMaskIntrinsics16x4 {
        self.find(0b1111_1111)
    }
}

// impl GroupStrategy for Group16Swiss {
//     const GROUP_SIZE: usize = 16;
//     type Group = Group16Swiss;
//     type ProbeSeq = SwissTableProbeSeq<16>;
//     type SliceType = [u8; 16];
//
//     const EMPTY_TAG: u8 = 0b1111_1111;
//
//     #[inline(always)]
//     fn get_tag(hash: u64) -> u8 {
//         Tag::full(hash).0
//     }
//
//     #[inline(always)]
//     unsafe fn load(tags: &[u8]) -> Self::Group {
//         Self::load(tags)
//     }
//
//     #[inline(always)]
//     unsafe fn load_ptr(tags: *const u8) -> Self::Group {
//         Self::load_ptr(tags)
//     }
//
//     #[inline(always)]
//     unsafe fn match_tag(group: &Self::Group, search_tag: u8) -> impl IntoIterator<Item=usize> {
//         group.find(search_tag)
//     }
//
//     #[inline(always)]
//     unsafe fn match_empty(group: &Self::Group) -> impl IntoIterator<Item=usize> {
//         group.match_empty()
//     }
//
//     #[inline(always)]
//     unsafe fn contains_empty_slot(group: &Self::Group) -> bool {
//         group.match_empty().any_bit_set()
//     }
//
//     #[inline(always)]
//     fn allocate_slice() -> Self::SliceType {
//         [0u8; 16]
//     }
//
//     unsafe fn match_tag_as_u8(group: &Self::Group, search_tag: u8) -> u8 {
//         todo!()
//     }
// }

pub struct GroupType8SwissTable;

// impl GroupStrategy for GroupType8SwissTable {
//     const GROUP_SIZE: usize = 8;
//     type Group = HashbrownGroup;
//     type ProbeSeq = SwissTableProbeSeq<8>;
//     type SliceType = [u8; 8];
//
//     const EMPTY_TAG: u8 = 0b1111_1111;
//
//     #[inline(always)]
//     fn get_tag(hash: u64) -> u8 {
//         Tag::full(hash).0
//     }
//
//     #[inline(always)]
//     unsafe fn load(tags: &[u8]) -> Self::Group {
//         Self::Group::load(tags.as_ptr().cast())
//     }
//
//     #[inline(always)]
//     unsafe fn load_ptr(tags: *const u8) -> Self::Group {
//         Self::Group::load(tags.cast())
//     }
//
//     #[inline(always)]
//     unsafe fn match_tag(group: &Self::Group, search_tag: u8) -> impl IntoIterator<Item=usize> {
//         group.match_tag(Tag(search_tag))
//     }
//
//     #[inline(always)]
//     unsafe fn match_empty(group: &Self::Group) -> impl IntoIterator<Item=usize> {
//         group.match_empty()
//     }
//
//     #[inline(always)]
//     unsafe fn contains_empty_slot(group: &Self::Group) -> bool {
//         group.match_empty().any_bit_set()
//     }
//
//     #[inline(always)]
//     fn allocate_slice() -> Self::SliceType {
//         [0u8; 8]
//     }
//
//     unsafe fn match_tag_as_u8(group: &Self::Group, search_tag: u8) -> u8 {
//         todo!()
//     }
// }

#[derive(Copy, Clone)]
pub struct Group8SwissPlus(aarch64::uint8x8_t);

impl Group8SwissPlus {
    pub(crate) const EMPTY_TAG: u8 = 0b1000_0000;

    #[inline(always)]
    pub fn make_tag(hash: u64) -> u8 {
        (hash >> 57) as u8
    }

    #[inline(always)]
    pub unsafe fn load(tags: &[u8]) -> Self {
        debug_assert!(tags.len() >= 8);
        Self(aarch64::vld1_u8(&tags[0]))
    }

    #[inline(always)]
    pub unsafe fn load_ptr(tags: *const u8) -> Self {
        Self(aarch64::vld1_u8(tags))
    }

    #[inline(always)]
    pub unsafe fn find(&self, search_tag: u8) -> IterableBitMaskIntrinsics8x8 {
        // Replicate the search value 8 times into a 64-bit register
        let search_register = aarch64::vld1_dup_u8(&search_tag);

        // Compare the registers together. For each u8 value in the 64-bit register, if the values
        // match, the output will have all 1s, otherwise all 0s.
        let output = aarch64::vceq_u8(self.0, search_register);

        // Reinterpret the 16-lane u8 type into a 2-lane 64-bit type, and return it as a regular
        // rust type
        let output = aarch64::vget_lane_u64::<0>(aarch64::vreinterpret_u64_u8(output));

        IterableBitMaskIntrinsics8x8::new(output)
    }

    #[inline(always)]
    pub unsafe fn find_as_u8(&self, search_tag: u8) -> u8 {
        // Replicate the search value 8 times into a 64-bit register
        let search_register = aarch64::vld1_dup_u8(&search_tag);

        // Compare the registers together. For each u8 value in the 64-bit register, if the values
        // match, the output will have all 1s, otherwise all 0s.
        let output = aarch64::vceq_u8(self.0, search_register);

        compress_match_result(output)
    }

    #[inline(always)]
    pub unsafe fn match_empty(&self) -> IterableBitMaskIntrinsics8x8 {
        let output = aarch64::vcltz_s8(aarch64::vreinterpret_s8_u8(self.0));
        let output = aarch64::vget_lane_u64::<0>(aarch64::vreinterpret_u64_u8(output));
        IterableBitMaskIntrinsics8x8::new(output)
    }
}


impl GroupStrategy for Group8SwissPlus {
    const GROUP_SIZE: usize = 8;
    type Group = Group8SwissPlus;
    type ProbeSeq = SwissTableProbeSeq<8>;
    type SliceType = [u8; 8];

    const EMPTY_TAG: u8 = Group8SwissPlus::EMPTY_TAG;

    #[inline(always)]
    fn get_tag(hash: u64) -> u8 {
        Group8SwissPlus::make_tag(hash)
    }

    #[inline(always)]
    unsafe fn load(tags: &[u8]) -> Self::Group {
        Self::load(tags)
    }

    #[inline(always)]
    unsafe fn load_ptr(tags: *const u8) -> Self::Group {
        Self::load_ptr(tags)
    }

    #[inline(always)]
    unsafe fn match_tag(group: &Self::Group, search_tag: u8) -> impl IntoIterator<Item=usize> {
        group.find(search_tag)
    }

    #[inline(always)]
    unsafe fn match_empty(group: &Self::Group) -> impl IntoIterator<Item=usize> {
        group.match_empty()
    }

    #[inline(always)]
    unsafe fn contains_empty_slot(group: &Self::Group) -> bool {
        group.match_empty().any_bit_set()
    }

    #[inline(always)]
    fn allocate_slice() -> Self::SliceType {
        [0u8; 8]
    }

    unsafe fn match_tag_as_u8(group: &Self::Group, search_tag: u8) -> u8 {
        group.find_as_u8(search_tag)
    }
}
