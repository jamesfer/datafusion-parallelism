use std::arch::aarch64;
use std::cmp::max;
use crate::operator::version10::new_map_3::group::group_strategy::{BulkGroupStrategy, BulkGroupStrategy32, BulkGroupStrategyN, GroupStrategy, IterableGroupStrategy};
use crate::operator::version10::new_map_3::group::probe_hybrid::HybridProbeSequence;
use crate::operator::version10::new_map_3::group::iterable_bit_mask::IterableBitMaskIntrinsics8x8;

// Takes the top 8 bits of the hash, reserving 0 as the empty tag
#[derive(Copy, Clone)]
pub struct Group8ReserveZero(aarch64::uint8x8_t);

impl GroupStrategy for Group8ReserveZero {
    const GROUP_SIZE: usize = 8;
    const EMPTY_TAG: u8 = 0;
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
        // Takes the top 8 bits
        max((hash >> 56) as u8, 1)
    }

    #[inline(always)]
    unsafe fn match_tag(group: &Self::Group, search_tag: u8) -> impl IntoIterator<Item=usize> + use<> {
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
}

impl Group8ReserveZero {
    #[inline(always)]
    pub unsafe fn find(&self, search_tag: u8) -> IterableBitMaskIntrinsics8x8 {
        let output = self.find_raw(&search_tag);
        IterableBitMaskIntrinsics8x8::new(output)
    }

    #[inline(always)]
    pub unsafe fn match_empty(&self) -> IterableBitMaskIntrinsics8x8 {
        self.find(Self::EMPTY_TAG)
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

impl IterableGroupStrategy for Group8ReserveZero {
    type It = IterableBitMaskIntrinsics8x8;

    #[inline(always)]
    unsafe fn match_non_empty(group: &Self::Group) -> Self::It {
        let output = group.find_raw(&Self::EMPTY_TAG);
        // Invert the output to find non-empty slots
        IterableBitMaskIntrinsics8x8::new(!output)
    }
}

impl BulkGroupStrategyN for Group8ReserveZero {
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

impl BulkGroupStrategy32 for Group8ReserveZero {
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

impl BulkGroupStrategy for Group8ReserveZero {
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
mod group8_bulk_n_tests {
    use crate::operator::version10::new_map_3::group::group8_reserve_zero::Group8ReserveZero;
    use crate::operator::version10::new_map_3::group::group_strategy::BulkGroupStrategyN;

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

        let iterators = unsafe { Group8ReserveZero::match_tag_n::<8>(&groups, &search_tags) };
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


#[cfg(test)]
mod group8_tests {
    use crate::operator::version10::new_map_3::group::group8_reserve_zero::Group8ReserveZero;
    use crate::operator::version10::new_map_3::group::group_strategy::BulkGroupStrategy;

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

        let tags = unsafe { Group8ReserveZero::get_tags(&hashes) };
        assert_eq!(tags, [1, 1, 2, 4, 8, 16, 32, 64]);
    }
}
