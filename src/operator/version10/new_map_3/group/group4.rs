use std::arch::aarch64;
use std::cmp::max;
use crate::operator::version10::new_map_3::group::utils;
use crate::operator::version10::new_map_3::group::group_strategy::{BulkGroupStrategyN, GroupStrategy};
use crate::operator::version10::new_map_3::group::probe_hybrid::HybridProbeSequence;
use crate::operator::version10::new_map_3::group::utils::{compress_match_result, interleave_x4_d, load_x4_d};
use crate::operator::version10::new_map_3::group::iterable_bit_mask::IterableBitMaskIntrinsics8x4;

#[derive(Copy, Clone)]
pub struct Group4;

impl GroupStrategy for Group4 {
    const GROUP_SIZE: usize = 4;
    const EMPTY_TAG: u8 = 0;
    type Group = aarch64::uint8x8_t;
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
