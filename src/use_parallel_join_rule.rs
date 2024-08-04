use std::any::Any;
use std::cell::OnceCell;
use std::fmt::{Debug, Formatter, Write};
use std::sync::{Arc, OnceLock, Weak};
use crossbeam::atomic::AtomicCell;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion_common::config::ConfigOptions;
use datafusion_common::{DataFusionError, JoinType};
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_physical_expr::{Distribution, EquivalenceProperties, Partitioning, PhysicalExprRef};
use datafusion_physical_plan::{DisplayAs, DisplayFormatType, ExecutionMode, ExecutionPlan, ExecutionPlanProperties, PlanProperties};
use datafusion_physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion_physical_plan::joins::utils::build_join_schema;
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use futures::TryFutureExt;
use crate::inner_hash_join::inner_join_stream;
use crate::utils::concurrent_join_map::{ConcurrentJoinMap, ReadOnlyJoinMap};
use crate::utils::limited_rc::LimitedRc;

pub struct UseParallelJoinRule;

impl PhysicalOptimizerRule for UseParallelJoinRule {
    fn optimize(&self, plan: Arc<dyn ExecutionPlan>, config: &ConfigOptions) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        Ok(plan.transform(transform)?.data)
    }

    fn name(&self) -> &str {
        "UseParallelJoin"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

fn transform(plan: Arc<dyn ExecutionPlan>) -> Result<Transformed<Arc<dyn ExecutionPlan>>, DataFusionError> {
    match plan.as_any().downcast_ref::<HashJoinExec>() {
        Some(hash_join_exec) => {
            match ParallelJoin::try_from(hash_join_exec) {
                Ok(new_operator) => {
                    Ok(Transformed::yes(Arc::new(new_operator)))
                },
                Err(error) => {
                    println!("Failed to apply ParallelJoin operator due to: {}", error);
                    Ok(Transformed::no(plan))
                }
            }

        },
        None => Ok(Transformed::no(plan)),
    }
}

struct JoinStateInstance {
    // TODO use limited RC to prevent cloning
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

// compacted_join_map_sender: AtomicCell<Option<Box<dyn FnOnce(RecordBatch, ReadOnlyJoinMap<u64>)>>>,
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

struct ParallelJoinExecutionState {
    states: Vec<AtomicCell<Option<ParallelJoinExecutionStateInstance>>>,
}

impl ParallelJoinExecutionState {
    fn new(parallelism: usize) -> Self {
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

#[derive(Debug)]
struct ParallelJoin {
    properties: PlanProperties,

    left: Arc<dyn ExecutionPlan>,
    right: Arc<dyn ExecutionPlan>,
    on: Vec<(PhysicalExprRef, PhysicalExprRef)>,
    join_type: JoinType,

    execution_state: Arc<OnceLock<ParallelJoinExecutionState>>,
}

impl ParallelJoin {
    fn compute_properties(eq: EquivalenceProperties, left: &Arc<dyn ExecutionPlan>) -> PlanProperties {
        PlanProperties::new(
            eq,
            Partitioning::RoundRobinBatch(left.output_partitioning().partition_count()),
            ExecutionMode::Bounded,
        )
    }
}

impl TryFrom<&HashJoinExec> for ParallelJoin {
    type Error = String;

    fn try_from(value: &HashJoinExec) -> Result<Self, Self::Error> {
        if value.join_type != JoinType::Inner {
            return Err(format!("Unsupported join type {}", value.join_type));
        }
        if value.projection.is_some() {
            return Err(format!("Projection not supported {:?}", value.projection));
        }
        if value.filter.is_some() {
            return Err(format!("Filter not supported {:?}", value.filter));
        }
        if value.mode != PartitionMode::Partitioned {
            return Err(format!("Mode not supported {:?}", value.mode));
        }

        let eq_from_hash_join = value.properties().eq_properties.clone();
        let properties = ParallelJoin::compute_properties(eq_from_hash_join.clone(), &value.left);

        Ok(ParallelJoin {
            properties,

            left: value.left.clone(),
            right: value.right.clone(),
            on: value.on.clone(),
            join_type: value.join_type.clone(),

            execution_state: Arc::new(OnceLock::new()),
            // join_state: ParallelJoinState::new(),
            // batch_list: Arc::new(boxcar::Vec::new()),
        })
    }
}

impl DisplayAs for ParallelJoin {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        f.write_str("ParallelJoin")
    }
}

impl ExecutionPlan for ParallelJoin {
    fn name(&self) -> &str {
        "ParallelJoin"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![
            Distribution::UnspecifiedDistribution,
            Distribution::UnspecifiedDistribution,
        ]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.left, &self.right]
    }

    fn with_new_children(self: Arc<Self>, children: Vec<Arc<dyn ExecutionPlan>>) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        let left = Arc::clone(&children[0]);
        Ok(Arc::new(ParallelJoin {
            properties: ParallelJoin::compute_properties(self.properties.eq_properties.clone(), &left),

            left,
            right: Arc::clone(&children[1]),
            join_type: self.join_type.clone(),
            on: self.on.clone(),

            execution_state: self.execution_state.clone(),
        }))
    }

    fn execute(&self, partition: usize, context: Arc<TaskContext>) -> datafusion_common::Result<SendableRecordBatchStream> {
        let parallelism = self.properties.output_partitioning().partition_count();
        let state = self.execution_state.get_or_init(|| ParallelJoinExecutionState::new(parallelism))
            .take(partition)
            .ok_or(DataFusionError::Internal(format!("State already consumed for partition {}", partition)))?;


        let (join_schema, column_indices) =
            build_join_schema(&self.left.schema(), &self.right.schema(), &self.join_type);

        let join_schema = Arc::new(join_schema);

        let left_c = Arc::clone(&self.left);
        let right_c = Arc::clone(&self.right);

        let left_stream = left_c.execute(partition, Arc::clone(&context))?;
        let right_stream = right_c.execute(partition, context)?;

        let schema = Arc::clone(&self.schema());
        let on = self.on.clone();


        match self.join_type {
            JoinType::Inner => {
                // TODO check if we really need to async move to prevent inner_join_stream from evaluating things immediately
                let s = async move {
                    inner_join_stream(
                        join_schema,
                        left_stream,
                        right_stream,
                        on,
                        state.join_map,
                        state.batch_list,
                        state.compacted_join_map_sender,
                        state.compacted_join_map_receiver,
                    ).await
                };
                Ok(Box::pin(RecordBatchStreamAdapter::new(schema, s.try_flatten_stream())))
            }
            _ => panic!("Unsupported join type {}", self.join_type)
            // JoinType::Left => {}
            // JoinType::Right => {}
            // JoinType::Full => {}
            // JoinType::LeftSemi => {}
            // JoinType::RightSemi => {}
            // JoinType::LeftAnti => {}
            // JoinType::RightAnti => {}
        }
    }
}
