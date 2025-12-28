use crate::operator::version10::new_map_3::group::probe_sequence::ProbeSequence;

pub struct SwissTableProbeSeq<const GROUP_SIZE: usize>;

impl <const GROUP_SIZE: usize> ProbeSequence for SwissTableProbeSeq<GROUP_SIZE> {
    const GROUP_SIZE: usize = GROUP_SIZE;

    #[inline(always)]
    fn start_index(hash: u64, capacity_mask: usize) -> usize {
        hash as usize & capacity_mask
    }

    #[inline(always)]
    fn next(previous: usize, stride: &mut usize, _hash: u64, _tag: u8, capacity_mask: usize) -> usize {
        *stride += GROUP_SIZE;
        (previous + *stride) & capacity_mask
    }
}
