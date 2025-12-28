use std::arch::aarch64;
use std::cmp::max;
use crate::operator::version10::new_map_3::group::group_strategy::{BulkGroupStrategy, BulkGroupStrategy32, BulkGroupStrategyN, GroupStrategy, IterableGroupStrategy};
use crate::operator::version10::new_map_3::group::probe_hybrid::HybridProbeSequence;
use crate::operator::version10::new_map_3::group::utils::compress_match_result;
use crate::operator::version10::new_map_3::group::iterable_bit_mask::IterableBitMaskIntrinsics8x8;

// Uses top 8 bits of the hash, but reserves 0b1111_1111 as the empty tag, using a modulo operation
// to avoid collisions with the empty tag.
#[derive(Copy, Clone)]
pub struct Group8SpreadEmpty(aarch64::uint8x8_t);

impl GroupStrategy for Group8SpreadEmpty {
    const GROUP_SIZE: usize = 8;
    const EMPTY_TAG: u8 = 0b1111_1111;
    type Group = Self;
    type SliceType = [u8; 8];

    #[inline(always)]
    unsafe fn load(tags: &[u8]) -> Self::Group {
        debug_assert!(tags.len() >= 8);
        Self(aarch64::vld1_u8(&tags[0]))
    }

    #[inline(always)]
    unsafe fn load_ptr(tags: *const u8) -> Self::Group {
        Self(aarch64::vld1_u8(tags))
    }

    #[inline(always)]
    fn get_tag(hash: u64) -> u8 {
        let top_bits = (hash >> 46) as u16;                 // take 16 bits
        let raw = top_bits % 255;
        raw as u8
    }

    #[inline(always)]
    unsafe fn match_tag(group: &Self::Group, search_tag: u8) -> impl IntoIterator<Item=usize> {
        group.find(search_tag)
    }

    #[inline(always)]
    unsafe fn match_empty(group: &Self::Group) -> impl IntoIterator<Item=usize> {
        group.find(Self::EMPTY_TAG)
    }

    #[inline(always)]
    unsafe fn contains_empty_slot(group: &Self::Group) -> bool {
        group.find(Self::EMPTY_TAG).any_bit_set()
    }

    #[inline(always)]
    fn allocate_slice() -> Self::SliceType {
        [0u8; 8]
    }
}

impl Group8SpreadEmpty {
    #[inline(always)]
    pub unsafe fn find(&self, search_tag: u8) -> IterableBitMaskIntrinsics8x8 {
        let output = self.find_raw(&search_tag);
        IterableBitMaskIntrinsics8x8::new(output)
    }

    #[inline(always)]
    unsafe fn find_raw(&self, search_tag: &u8) -> u64 {
        // Replicate the search value 8 times into a 64-bit register
        let search_register = aarch64::vld1_dup_u8(search_tag);

        // Compare the registers together. For each u8 value in the 64-bit register, if the values
        // match, the output will have all 1s, otherwise all 0s.
        let match_result = aarch64::vceq_u8(self.0, search_register);

        aarch64::vget_lane_u64::<0>(aarch64::vreinterpret_u64_u8(match_result))
    }
}

impl IterableGroupStrategy for Group8SpreadEmpty {
    type It = IterableBitMaskIntrinsics8x8;

    unsafe fn match_non_empty(group: &Self::Group) -> Self::It {
        let output = group.find_raw(&Self::EMPTY_TAG);
        // Invert the output to find non-empty slots
        IterableBitMaskIntrinsics8x8::new(!output)
    }
}
