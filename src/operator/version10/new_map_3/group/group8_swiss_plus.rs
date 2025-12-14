use std::arch::aarch64;
use crate::operator::version10::new_map_3::group::group_strategy::GroupStrategy;
use crate::operator::version10::new_map_3::group::probe_swiss::SwissTableProbeSeq;
use crate::operator::version10::new_map_3::group::utils::compress_match_result;
use crate::operator::version10::new_map_3::group::iterable_bit_mask::IterableBitMaskIntrinsics8x8;

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
