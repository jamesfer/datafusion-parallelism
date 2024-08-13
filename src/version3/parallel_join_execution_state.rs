use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use crossbeam::atomic::AtomicCell;
use datafusion::arrow::array::RecordBatch;
use datafusion_common::DataFusionError;

use crate::utils::limited_rc::LimitedRc;
use crate::utils::partitioned_concurrent_join_map::{create_writable_join_map, WritablePartitionedConcurrentJoinMap};
use crate::utils::perform_once::PerformOnce;

pub struct ParallelJoinExecutionStateInstance {
    pub join_map: WritablePartitionedConcurrentJoinMap,
    pub batch_list: LimitedRc<boxcar::Vec<(usize, RecordBatch)>>,
    pub compute_compacted_batch_list: Arc<PerformOnce<Result<RecordBatch, DataFusionError>>>,
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
        let compute_compacted_batch_list = Arc::new(PerformOnce::new());
        let states = create_writable_join_map(parallelism)
            .into_iter()
            .zip(LimitedRc::new_copies(boxcar::Vec::new(), parallelism).into_iter())
            .map(|(join_map, batch_list)| AtomicCell::new(Some(ParallelJoinExecutionStateInstance {
                join_map,
                batch_list,
                compute_compacted_batch_list: compute_compacted_batch_list.clone(),
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
