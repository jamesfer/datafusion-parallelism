use std::arch::aarch64;
use crate::operator::version10::new_map_3::group::iterable_bit_mask::IterableBitMaskIntrinsics16x4;

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
