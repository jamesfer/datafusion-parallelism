use datafusion::arrow::array::RecordBatch;
use std::sync::{Arc, OnceLock, Weak};
use crossbeam::atomic::AtomicCell;
use std::fmt::{Debug, Formatter};
use crate::utils::concurrent_join_map::{ConcurrentJoinMap, ReadOnlyJoinMap};
use crate::utils::limited_rc::LimitedRc;

struct JoinStateInstance {
    concurrent_join_map: Arc<ConcurrentJoinMap<u64>>,
    // A sender that should only be written to by a single task, once the build side is complete
    compacted_join_map_sender: Arc<AtomicCell<Option<tokio::sync::broadcast::Sender<(RecordBatch, ReadOnlyJoinMap<u64>)>>>>,
    // A receiver that will be filled with the completed build side once the final task is complete
    compacted_join_map_receiver: tokio::sync::broadcast::Receiver<(RecordBatch, ReadOnlyJoinMap<u64>)>
}

#[derive(Clone)]
struct ParallelJoinState {
    weak_join_map: OnceLock<Weak<ConcurrentJoinMap<u64>>>,
    compacted_join_map_sender: tokio::sync::broadcast::Sender<(RecordBatch, ReadOnlyJoinMap<u64>)>,
    atomic_cell_sender: Arc<AtomicCell<Option<tokio::sync::broadcast::Sender<(RecordBatch, ReadOnlyJoinMap<u64>)>>>>,
}

impl Debug for ParallelJoinState {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("ParallelJoinState {...}")
    }
}

impl ParallelJoinState {
    pub fn new() -> Self {
        let broadcast_sender = tokio::sync::broadcast::Sender::<(RecordBatch, ReadOnlyJoinMap<u64>)>::new(1);
        Self {
            weak_join_map: OnceLock::new(),
            atomic_cell_sender: Arc::new(AtomicCell::new(Some(broadcast_sender.clone()))),
            compacted_join_map_sender: broadcast_sender,
        }
    }

    pub fn create_instance(&self) -> Result<JoinStateInstance, String> {
        // Get a copy of the join map arc
        let arc_join_map = match self.weak_join_map.get() {
            // Need to initialize the state
            None => {
                let arc_join_map = Arc::new(ConcurrentJoinMap::new());
                let weak_arc = self.weak_join_map.get_or_init(|| Arc::downgrade(&arc_join_map));
                // When we call .get_or_init, there is no guarantee that the returned value is the
                // same arc value in the closure since the join map could have already been
                // initialised. Therefore, we always need to attempt to upgrade the join map from
                // the Weak pointer
                match weak_arc.upgrade() {
                    None => Err("Failed to upgrade weak pointer after initialization".to_string()),
                    Some(arc_join_map) => Ok(arc_join_map),
                }
            }
            Some(weak_join_map) => match weak_join_map.upgrade() {
                // Join map was already dropped
                None => Err("Failed to upgrade weak pointer".to_string()),
                Some(arc_join_map) => Ok(arc_join_map),
            }
        }?;

        Ok(JoinStateInstance {
            concurrent_join_map: arc_join_map,
            compacted_join_map_sender: Arc::clone(&self.atomic_cell_sender),
            compacted_join_map_receiver: self.compacted_join_map_sender.subscribe(),
        })
    }
}

struct ParallelJoinExecutionStateInstance {
    join_map: LimitedRc<ConcurrentJoinMap<u64>>,
    batch_list: LimitedRc<boxcar::Vec<(usize, RecordBatch)>>,
    compacted_join_map_sender: Arc<AtomicCell<Option<tokio::sync::broadcast::Sender<(RecordBatch, Arc<ReadOnlyJoinMap<u64>>)>>>>,
    compacted_join_map_receiver: tokio::sync::broadcast::Receiver<(RecordBatch, Arc<ReadOnlyJoinMap<u64>>)>,
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
    pub(crate) fn new(parallelism: usize) -> Self {
        let join_map = ConcurrentJoinMap::new();
        let batch_list = boxcar::Vec::new();
        let broadcast_sender = tokio::sync::broadcast::Sender::<(RecordBatch, Arc<ReadOnlyJoinMap<u64>>)>::new(1);
        let broadcast_sender_cell = Arc::new(AtomicCell::new(Some(broadcast_sender.clone())));

        let states = LimitedRc::new_copies(join_map, parallelism)
            .into_iter()
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

    fn take(&self, index: usize) -> Option<ParallelJoinExecutionStateInstance> {
        self.states.get(index).map(|cell| cell.take()).flatten()
    }
}

impl Debug for ParallelJoinExecutionState {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("ParallelJoinExecutionState {...}")
    }
}
