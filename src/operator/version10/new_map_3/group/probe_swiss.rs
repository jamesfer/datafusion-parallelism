use crate::operator::version10::new_map_3::group::probe_sequence::ProbeSequence;

pub struct SwissTableProbeSeq<const GROUP_SIZE: usize>;

impl <const GROUP_SIZE: usize> ProbeSequence for SwissTableProbeSeq<GROUP_SIZE> {
    #[inline(always)]
    fn start(hash: u64, capacity_mask: usize) -> (usize, usize) {
        (hash as usize & capacity_mask, 0)
    }

    #[inline(always)]
    fn next(previous: usize, _: u8, stride: &mut usize, capacity_mask: usize) -> usize {
        *stride += GROUP_SIZE;
        (previous + *stride) & capacity_mask
    }
}
