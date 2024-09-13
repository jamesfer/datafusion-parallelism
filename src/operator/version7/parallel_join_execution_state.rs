use std::fmt::{Debug, Formatter};

use crossbeam::atomic::AtomicCell;

use crate::operator::version7::hash_lookup_builder::{create_local_accumulators, LocalAccumulator};

pub struct ParallelJoinExecutionStateInstance {
    pub accumulator: LocalAccumulator,
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
        let states = create_local_accumulators(parallelism).into_iter()
            .map(|accumulator| AtomicCell::new(Some(ParallelJoinExecutionStateInstance {
                accumulator,
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
