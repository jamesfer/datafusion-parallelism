use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use crossbeam::atomic::AtomicCell;
use datafusion::arrow::array::RecordBatch;
use crate::utils::concurrent_queued_self_hash_join_map::{make_concurrent_queued_self_hash_join_map, ConcurrentQueuedSelfHashJoinMapInstance};
use crate::utils::limited_rc::LimitedRc;

pub struct ParallelJoinExecutionStateInstance {
    pub join_map: ConcurrentQueuedSelfHashJoinMapInstance,
    pub batch_list: LimitedRc<boxcar::Vec<(usize, RecordBatch)>>,
    pub compacted_join_map_sender: Arc<AtomicCell<Option<tokio::sync::broadcast::Sender<RecordBatch>>>>,
    pub compacted_join_map_receiver: tokio::sync::broadcast::Receiver<RecordBatch>,
}

impl Debug for ParallelJoinExecutionStateInstance {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("ParallelJoinExecutionStateInstance {...}")
    }
}

pub struct ParallelJoinExecutionState {
    states: Vec<AtomicCell<Option<ParallelJoinExecutionStateInstance>>>,
}

impl ParallelJoinExecutionState {
    pub fn new(parallelism: usize) -> Self {
        let join_maps = make_concurrent_queued_self_hash_join_map(parallelism);
        let batch_list = boxcar::Vec::new();
        let broadcast_sender = tokio::sync::broadcast::Sender::<RecordBatch>::new(1);
        let broadcast_sender_cell = Arc::new(AtomicCell::new(Some(broadcast_sender.clone())));

        let states = join_maps.into_iter()
            .zip(LimitedRc::new_copies(batch_list, parallelism).into_iter())
            .map(|(join_map, batch_list)| AtomicCell::new(Some(ParallelJoinExecutionStateInstance {
                join_map,
                batch_list,
                compacted_join_map_sender: broadcast_sender_cell.clone(),
                compacted_join_map_receiver: broadcast_sender.subscribe(),
            })))
            .collect::<Vec<_>>();
        Self { states }
    }

    pub fn take(&self, index: usize) -> Option<ParallelJoinExecutionStateInstance> {
        self.states.get(index).map(|cell| cell.take()).flatten()
    }
}

impl Debug for ParallelJoinExecutionState {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("ParallelJoinExecutionState {...}")
    }
}
