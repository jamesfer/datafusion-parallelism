use std::arch::aarch64;
use std::ops::DerefMut;
use crate::operator::version10::new_map_3::group::group_strategy::GroupStrategy;

// Pretty good option
// #[no_mangle]
// pub fn load_ptr_halved(search: u8, values: *const u8) -> u8 {
//     use std::arch::aarch64;
//
//     unsafe {
//         // Replicate the search value 16 times into a 128-bit register
//         let search_register = aarch64::vld1_dup_u8(&search);
//         let values_register = aarch64::vld1_u8(values);
//
//         // Compare the registers together. For each u8 value in the 128-bit register, if the values
//         // match, the output will have all 1s, otherwise all 0s.
//         let output = aarch64::vceq_u8(values_register, search_register);
//
//         // Only the lower 64 bits are really used
//         let larger_register = aarch64::vld1q_dup_u64(&aarch64::vget_lane_u64::<0>(aarch64::vreinterpret_u64_u8(output)));
//         // Halve the width of each match result, to 4 bits each. Only the lower 32 bits matter now,
//         // the upper bits are just duplicated
//         let halved = aarch64::vqshrn_n_u16::<7>(aarch64::vreinterpretq_u16_u64(larger_register));
//
//         let extracted = aarch64::vget_lane_u32::<0>(aarch64::vreinterpret_u32_u8(halved));
//
//         let mut result = 0u8;
//         for i in 0..4 {
//             // Compiles to assembly really efficiently
//             result |= ((extracted & (0b11 << (i * 8)) >> (i * 8)) as u8) << (i * 2);
//         }
//         result
//     }
// }

#[inline(always)]
pub unsafe fn compress_match_result(match_result: aarch64::uint8x8_t) -> u8 {
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

#[inline(always)]
pub unsafe fn interleave_x4(search_tags: [u8; 4]) -> aarch64::uint8x16_t {
    let many_u8_register = aarch64::vld4_dup_u8(search_tags.as_ptr());
    let left = aarch64::vreinterpret_u8_u32(aarch64::vzip1_u32(aarch64::vreinterpret_u32_u8(many_u8_register.0), aarch64::vreinterpret_u32_u8(many_u8_register.1)));
    let right = aarch64::vreinterpret_u8_u32(aarch64::vzip1_u32(aarch64::vreinterpret_u32_u8(many_u8_register.2), aarch64::vreinterpret_u32_u8(many_u8_register.3)));
    let search_tags_x4 = aarch64::vcombine_u8(left, right);
    search_tags_x4
}

#[inline(always)]
pub unsafe fn interleave_x4_b(search_tags: &[u8; 4]) -> aarch64::uint8x16_t {
    let u32 = u32::from_le_bytes(*search_tags);
    let u8buf = aarch64::vcreate_u8(u32 as u64);
    let x8buf_x2 = aarch64::vzip1_u8(u8buf, u8buf);
    let x8buf_x2 = aarch64::vzip_u8(x8buf_x2, x8buf_x2);
    let r = aarch64::vcombine_u8(x8buf_x2.0, x8buf_x2.1);
    r
}

// This is the only variation used
#[inline(always)]
pub unsafe fn interleave_x4_d(search_tags: &[u8; 4]) -> aarch64::uint8x16_t {
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
pub unsafe fn interleave_x4_c(search_tags: [u8; 4]) -> aarch64::uint8x16_t {
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
pub unsafe fn load_x4_d(groups: &[*const u8; 4]) -> aarch64::uint8x16_t {
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
