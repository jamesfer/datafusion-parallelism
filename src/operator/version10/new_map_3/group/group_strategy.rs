use crate::operator::version10::new_map_3::group::probe_sequence::{ProbeSequence, ProbeSequenceBulk, ProbeSequenceBulk32, ProbeSequenceBulkN};

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

