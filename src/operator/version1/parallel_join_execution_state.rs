use std::fmt::{Debug, Formatter};
use std::future::{ready, Ready};
use std::sync::Arc;

use crossbeam::atomic::AtomicCell;
use datafusion::arrow::datatypes::SchemaRef;
use tokio::sync::OnceCell;
use crate::utils::concurrent_self_hash_join_map::{ConcurrentSelfHashJoinMap, ReadOnlyJoinMap};
use crate::utils::limited_rc::LimitedRc;
use crate::utils::parallel_compaction_batch_list::ParallelCompactionBatchList;

pub struct ParallelJoinExecutionStateInstance {
    pub join_map: LimitedRc<ConcurrentSelfHashJoinMap>,
    // pub batch_list: LimitedRc<boxcar::Vec<(usize, RecordBatch)>>,
    pub batch_list: ParallelCompactionBatchList,
    pub compacted_join_map_sender: Arc<AtomicCell<Option<tokio::sync::broadcast::Sender<Arc<ReadOnlyJoinMap>>>>>,
    pub compacted_join_map_receiver: tokio::sync::broadcast::Receiver<Arc<ReadOnlyJoinMap>>,
}

impl Debug for ParallelJoinExecutionStateInstance {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("ParallelJoinExecutionStateInstance {...}")
    }
}

pub struct ParallelJoinExecutionState {
    parallelism: usize,
    states: OnceCell<Vec<AtomicCell<Option<ParallelJoinExecutionStateInstance>>>>,
}

impl ParallelJoinExecutionState {
    pub fn new(parallelism: usize) -> Self {
        Self { parallelism, states: OnceCell::new() }
    }

    pub async fn take(&self, schema: SchemaRef, index: usize) -> Option<ParallelJoinExecutionStateInstance> {
        let parallelism = self.parallelism;
        let states = self.states.get_or_init(|| ready(Self::initialize_states(parallelism, schema))).await;
        states.get(index).map(|cell| cell.take()).flatten()
    }

    fn initialize_states(parallelism: usize, schema: SchemaRef) -> Vec<AtomicCell<Option<ParallelJoinExecutionStateInstance>>> {
        let join_map = ConcurrentSelfHashJoinMap::new();
        let broadcast_sender = tokio::sync::broadcast::Sender::<Arc<ReadOnlyJoinMap>>::new(1);
        let broadcast_sender_cell = Arc::new(AtomicCell::new(Some(broadcast_sender.clone())));

        LimitedRc::new_copies(join_map, parallelism)
            .into_iter()
            .zip(ParallelCompactionBatchList::new_copies(schema, parallelism))
            .map(|(join_map, batch_list)| AtomicCell::new(Some(ParallelJoinExecutionStateInstance {
                join_map,
                batch_list,
                compacted_join_map_sender: broadcast_sender_cell.clone(),
                compacted_join_map_receiver: broadcast_sender.subscribe(),
            })))
            .collect()
    }
}

impl Debug for ParallelJoinExecutionState {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("ParallelJoinExecutionState {...}")
    }
}
