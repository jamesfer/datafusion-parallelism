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
