use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::{Arc, OnceLock};

use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion_common::{DataFusionError, JoinType};
use datafusion_physical_expr::{Distribution, EquivalenceProperties, Partitioning, PhysicalExprRef};
use datafusion_physical_plan::{DisplayAs, DisplayFormatType, ExecutionMode, ExecutionPlan, ExecutionPlanProperties, PlanProperties};
use datafusion_physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion_physical_plan::joins::utils::build_join_schema;
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use futures::TryFutureExt;
use crate::version1::inner_hash_join::inner_join_stream;
use crate::version1::parallel_join_execution_state::ParallelJoinExecutionState;


#[derive(Debug)]
pub struct ParallelJoin {
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
