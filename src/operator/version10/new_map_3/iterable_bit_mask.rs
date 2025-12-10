use std::fmt::{Debug, Formatter};
use std::num::{NonZeroU128, NonZeroU32, NonZeroU64};

pub trait IterableBitMaskT {
    fn next_set_index(&mut self) -> Option<usize>;
}

pub struct IterableBitMask {
    value: u128,
}

impl IterableBitMask {
    pub fn new(bytes: [u8; 16]) -> Self {
        // Each of the bytes should only ever be a 0 or a 1. We left shift the final value to push
        // the ones to the most significant bit of their byte.
        let value = u128::from_be_bytes(bytes) << 7;
        Self { value }
    }

    pub fn next_set_index(&mut self) -> Option<usize> {
        let value = NonZeroU128::new(self.value)?;

        let leading_zeros = value.leading_zeros();

        // Divide the bit set index by 8 to get a regular index value
        let index = leading_zeros / 8;

        // Remove the top bit from the value
        self.value = self.value & !(1 << (127 - leading_zeros));

        Some(index as usize)
    }
}

impl IterableBitMaskT for IterableBitMask {
    fn next_set_index(&mut self) -> Option<usize> {
        self.next_set_index()
    }
}

#[cfg(test)]
mod tests {
    use crate::operator::version10::new_map_3::iterable_bit_mask::IterableBitMask;

    #[test]
    pub fn single_valued_bit_mask() {
        for i in 0..16 {
            let mut data = [0u8; 16];
            data[i] = 1;

            let mut mask = IterableBitMask::new(data);

            assert_eq!(mask.next_set_index(), Some(i), "Failed for index {}", i);
            assert_eq!(mask.next_set_index(), None);
            assert_eq!(mask.next_set_index(), None);
            assert_eq!(mask.next_set_index(), None);
        }
    }

    #[test]
    pub fn multi_valued_bit_mask() {
        let mut data = [0u8; 16];
        data[0] = 1;
        data[3] = 1;
        data[8] = 1;
        data[15] = 1;

        let mut mask = IterableBitMask::new(data);
        assert_eq!(mask.next_set_index(), Some(0));
        assert_eq!(mask.next_set_index(), Some(3));
        assert_eq!(mask.next_set_index(), Some(8));
        assert_eq!(mask.next_set_index(), Some(15));
        assert_eq!(mask.next_set_index(), None);
        assert_eq!(mask.next_set_index(), None);
        assert_eq!(mask.next_set_index(), None);
    }
}

pub struct IterableBitMaskIntrinsics {
    value: u128,
}

impl IterableBitMaskIntrinsics {
    pub fn new(value: u128) -> Self {
        // Each of the bytes is either all 1s or all zeros, so we mask the value to only keep the
        // lowest bit per byte. TODO this could be done inside the intrinsics
        Self { value: value & 0x0101_0101_0101_0101_0101_0101_0101_0101 }
    }

    pub fn next_set_index(&mut self) -> Option<usize> {
        let value = NonZeroU128::new(self.value)?;
        let zeros = value.trailing_zeros();

        // Remove the lowest bit from the value
        self.value = self.value & (self.value - 1);

        // Divide the bit set index by 8 to get a regular index value
        Some((zeros / 8) as usize)
    }
}

impl IterableBitMaskT for IterableBitMaskIntrinsics {
    fn next_set_index(&mut self) -> Option<usize> {
        self.next_set_index()
    }
}

#[cfg(test)]
mod intrinsics_tests {
    // #[test]
    // #[cfg(target_arch = "aarch64")]
    // pub fn works_with_intrinsics() {
    //     let search_tag = 0b1010_1010;
    //     let mut values = [0u8; 16];
    //     values[0] = 0b1111_0000;
    //     values[1] = search_tag;
    //     values[2] = search_tag;
    //     values[4] = 0b0000_1111;
    //     values[6] = 0b0011_1111;
    //     values[10] = 0b1111_1111;
    //     values[14] = 0b1000_1000;
    //     values[15] = search_tag;
    //
    //     let num = u8x16_compare_eq_aarch64(search_tag, &values);
    //     let mut mask = IterableBitMaskIntrinsics::new(num.clone());
    //
    //     assert_eq!(mask.next_set_index(), Some(1), "Intrinsic output: {:032X?}", num);
    //     assert_eq!(mask.next_set_index(), Some(2), "Intrinsic output: {:032X?}", num);
    //     assert_eq!(mask.next_set_index(), Some(15), "Intrinsic output: {:032X?}", num);
    //     assert_eq!(mask.next_set_index(), None, "Intrinsic output: {:032X?}", num);
    //     assert_eq!(mask.next_set_index(), None, "Intrinsic output: {:032X?}", num);
    // }
    // #[test]
    // pub fn single_valued_bit_mask() {
    //     for i in 0..15 {
    //         let mut data = [0u8; 16];
    //         data[i] = 1;
    //
    //         let mut mask = IterableBitMask::new(data);
    //
    //         assert_eq!(mask.next_set_index(), Some(i), "Failed for index {}", i);
    //         assert_eq!(mask.next_set_index(), None);
    //         assert_eq!(mask.next_set_index(), None);
    //         assert_eq!(mask.next_set_index(), None);
    //     }
    // }
    //
    // #[test]
    // pub fn multi_valued_bit_mask() {
    //     let mut data = [0u8; 16];
    //     data[0] = 1;
    //     data[3] = 1;
    //     data[8] = 1;
    //     data[15] = 1;
    //
    //     let mut mask = super::IterableBitMask::new(data);
    //     assert_eq!(mask.next_set_index(), Some(0));
    //     assert_eq!(mask.next_set_index(), Some(3));
    //     assert_eq!(mask.next_set_index(), Some(8));
    //     assert_eq!(mask.next_set_index(), Some(15));
    //     assert_eq!(mask.next_set_index(), None);
    //     assert_eq!(mask.next_set_index(), None);
    //     assert_eq!(mask.next_set_index(), None);
    // }
}

#[repr(transparent)]
#[derive(Clone, Copy)]
pub struct IterableBitMaskIntrinsics16x4 {
    value: u64,
}

impl IterableBitMaskIntrinsics16x4 {
    #[inline]
    pub fn new(value: u64) -> Self {
        // Each of the 4 bits is either all 1s or all zeros, so we mask the value to only keep the
        // lowest bit per byte.

        // Hexadecimal mask of 0b0001_0001....
        const MASK: u64 = 0x1111_1111_1111_1111;
        Self { value: value & MASK }
    }

    #[inline]
    pub fn next_set_index(&mut self) -> Option<usize> {
        let value = NonZeroU64::new(self.value)?;
        let zeros = value.trailing_zeros();

        // Remove the lowest bit from the value
        self.value = self.value & (self.value - 1);

        // Divide the bit set index by 8 to get a regular index value
        Some((zeros / 4) as usize)
    }
     #[inline]
     pub fn any_bit_set(&self) -> bool {
         self.value != 0
     }
}

impl IterableBitMaskT for IterableBitMaskIntrinsics16x4 {
    fn next_set_index(&mut self) -> Option<usize> {
        self.next_set_index()
    }
}

impl IntoIterator for IterableBitMaskIntrinsics16x4 {
    type Item = usize;
    type IntoIter = IterableBitMaskIntrinsics16x4Iterator;

    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        IterableBitMaskIntrinsics16x4Iterator { value: self.value }
    }
}

pub struct IterableBitMaskIntrinsics16x4Iterator {
    value: u64,
}

impl Iterator for IterableBitMaskIntrinsics16x4Iterator {
    type Item = usize;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        let value = NonZeroU64::new(self.value)?;
        let zeros = value.trailing_zeros();

        // Remove the lowest bit from the value
        self.value = self.value & (self.value - 1);

        // Divide the bit set index by 8 to get a regular index value
        Some((zeros / 4) as usize)
    }
}

#[cfg(test)]
mod intrinsics_tests_64 {
    // #[test]
    // #[cfg(target_arch = "aarch64")]
    // pub fn works_with_intrinsics() {
    //     let search_tag = 0b1010_1010;
    //     let mut values = [0u8; 16];
    //     values[0] = 0b1111_0000;
    //     values[1] = search_tag;
    //     values[2] = search_tag;
    //     values[4] = 0b0000_1111;
    //     values[6] = 0b0011_1111;
    //     values[10] = 0b1111_1111;
    //     values[14] = 0b1000_1000;
    //     values[15] = search_tag;
    // 
    //     let num = u8x16_compare_eq_aarch64_shrink(search_tag, &values);
    //     let mut mask = IterableBitMaskIntrinsics16x4::new(num.clone());
    // 
    //     assert_eq!(mask.next_set_index(), Some(1), "Intrinsic output: {:032X?}", num);
    //     assert_eq!(mask.next_set_index(), Some(2), "Intrinsic output: {:032X?}", num);
    //     assert_eq!(mask.next_set_index(), Some(15), "Intrinsic output: {:032X?}", num);
    //     assert_eq!(mask.next_set_index(), None, "Intrinsic output: {:032X?}", num);
    //     assert_eq!(mask.next_set_index(), None, "Intrinsic output: {:032X?}", num);
    // }
}

#[repr(transparent)]
#[derive(Clone, Copy)]
pub struct IterableBitMaskIntrinsics8x8 {
    value: u64,
}

impl IterableBitMaskIntrinsics8x8 {
    #[inline]
    pub fn new(value: u64) -> Self {
        // Each of the 4 bits is either all 1s or all zeros, so we mask the value to only keep the
        // lowest bit per byte.

        // Hexadecimal mask of 0b0000_0001....
        const MASK: u64 = 0x0101_0101_0101_0101;
        Self { value: value & MASK }
    }

    #[inline]
    pub unsafe fn new_unchecked(value: u64) -> Self {
        Self { value }
    }

    #[inline]
    pub fn reinterpret_as_u64s(slice: &mut [Self]) -> &mut [u64] {
        // Constant assertion to ensure that the pointer cast is going to succeed
        //noinspection RsAssertEqual
        const SIZE_OK: () = assert!(size_of::<IterableBitMaskIntrinsics8x8>() == size_of::<u64>());
        unsafe { &mut *(slice as *mut [Self] as *mut [u64]) }
    }

    #[inline]
    pub fn next_set_index(&mut self) -> Option<usize> {
        let value = NonZeroU64::new(self.value)?;
        let zeros = value.trailing_zeros();

        // Remove the lowest bit from the value
        self.value = self.value & (self.value - 1);

        // Divide the bit set index by 8 to get a regular index value
        Some((zeros / 8) as usize)
    }
    #[inline]
    pub fn any_bit_set(&self) -> bool {
        self.value != 0
    }
}

impl IterableBitMaskT for IterableBitMaskIntrinsics8x8 {
    fn next_set_index(&mut self) -> Option<usize> {
        self.next_set_index()
    }
}

impl IntoIterator for IterableBitMaskIntrinsics8x8 {
    type Item = usize;
    type IntoIter = IterableBitMaskIntrinsics8x8Iterator;

    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        IterableBitMaskIntrinsics8x8Iterator { value: self.value }
    }
}

pub struct IterableBitMaskIntrinsics8x8Iterator {
    value: u64,
}

impl Iterator for IterableBitMaskIntrinsics8x8Iterator {
    type Item = usize;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        let value = NonZeroU64::new(self.value)?;
        let zeros = value.trailing_zeros();

        // Remove the lowest bit from the value
        self.value = self.value & (self.value - 1);

        // Divide the bit set index by 8 to get a regular index value
        Some((zeros / 8) as usize)
    }
}

#[repr(transparent)]
#[derive(Clone, Copy)]
pub struct IterableBitMaskIntrinsics8x4 {
    value: u32,
}

impl IterableBitMaskIntrinsics8x4 {
    #[inline]
    pub fn new(value: u32) -> Self {
        // Each of the 4 bits is either all 1s or all zeros, so we mask the value to only keep the
        // lowest bit per byte.

        // Hexadecimal mask of 0b0000_0001....
        const MASK: u32 = 0x0101_0101;
        Self { value: value & MASK }
    }

    #[inline]
    pub unsafe fn new_unchecked(value: u32) -> Self {
        Self { value }
    }

    #[inline]
    pub fn reinterpret_as_u32s(slice: &mut [Self]) -> &mut [u32] {
        // Constant assertion to ensure that the pointer cast is going to succeed
        //noinspection RsAssertEqual
        const SIZE_OK: () = assert!(size_of::<IterableBitMaskIntrinsics8x4>() == size_of::<u32>());
        unsafe { &mut *(slice as *mut [Self] as *mut [u32]) }
    }

    // #[inline]
    // pub fn next_set_index(&mut self) -> Option<usize> {
    //     let value = NonZeroU32::new(self.value)?;
    //     let zeros = value.trailing_zeros();
    //
    //     // Remove the lowest bit from the value
    //     self.value = self.value & (self.value - 1);
    //
    //     // Divide the bit set index by 8 to get a regular index value
    //     Some((zeros / 8) as usize)
    // }

    #[inline]
    pub fn any_bit_set(&self) -> bool {
        self.value != 0
    }
}

impl IntoIterator for IterableBitMaskIntrinsics8x4 {
    type Item = usize;
    type IntoIter = IterableBitMaskIntrinsics8x4Iterator;

    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        IterableBitMaskIntrinsics8x4Iterator { value: self.value }
    }
}

pub struct IterableBitMaskIntrinsics8x4Iterator {
    value: u32,
}

impl Iterator for IterableBitMaskIntrinsics8x4Iterator {
    type Item = usize;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        let value = NonZeroU32::new(self.value)?;
        let zeros = value.trailing_zeros();

        // Remove the lowest bit from the value
        self.value = self.value & (self.value - 1);

        // Divide the bit set index by 4 to get a regular index value
        Some((zeros / 8) as usize)
    }
}
