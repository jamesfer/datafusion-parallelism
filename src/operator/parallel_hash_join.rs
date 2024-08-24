use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::{Arc, OnceLock};

use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion_common::{DataFusionError, JoinType};
use datafusion_physical_expr::{Distribution, EquivalenceProperties, Partitioning, PhysicalExprRef};
use datafusion_physical_plan::{DisplayAs, DisplayFormatType, ExecutionMode, ExecutionPlan, ExecutionPlanProperties, PlanProperties};
use datafusion_physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion_physical_plan::joins::utils::build_join_schema;

use crate::operator::parallel_hash_join_stream::ParallelHashJoinStream;
use crate::parse_sql::JoinReplacement;

#[derive(Debug)]
pub struct ParallelHashJoin {
    properties: PlanProperties,
    left: Arc<dyn ExecutionPlan>,
    right: Arc<dyn ExecutionPlan>,
    on: Vec<(PhysicalExprRef, PhysicalExprRef)>,
    join_type: JoinType,
    replacement: JoinReplacement,
    execution_instance: Arc<OnceLock<ParallelHashJoinStream>>,
}

impl ParallelHashJoin {
    fn compute_properties(eq: EquivalenceProperties, left: &Arc<dyn ExecutionPlan>) -> PlanProperties {
        PlanProperties::new(
            eq,
            Partitioning::RoundRobinBatch(left.output_partitioning().partition_count()),
            ExecutionMode::Bounded,
        )
    }

    pub fn convert_hash_join(value: &HashJoinExec, replacement: JoinReplacement) -> Result<Self, String> {
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
        let properties = ParallelHashJoin::compute_properties(eq_from_hash_join.clone(), &value.left);

        Ok(ParallelHashJoin {
            properties,
            left: value.left.clone(),
            right: value.right.clone(),
            on: value.on.clone(),
            join_type: value.join_type.clone(),
            replacement,
            execution_instance: Arc::new(OnceLock::new()),
        })
    }
}

impl DisplayAs for ParallelHashJoin {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        f.write_str("ParallelJoin")
    }
}

impl ExecutionPlan for ParallelHashJoin {
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
        Ok(Arc::new(ParallelHashJoin {
            properties: ParallelHashJoin::compute_properties(self.properties.eq_properties.clone(), &left),
            left,
            right: Arc::clone(&children[1]),
            join_type: self.join_type.clone(),
            on: self.on.clone(),
            replacement: self.replacement.clone(),
            // TODO this might break the data fusion contract
            execution_instance: self.execution_instance.clone(),
        }))
    }

    fn execute(&self, partition: usize, context: Arc<TaskContext>) -> datafusion_common::Result<SendableRecordBatchStream> {
        let parallelism = self.properties.output_partitioning().partition_count();
        let execution_instance = self.execution_instance.get_or_init(|| ParallelHashJoinStream::new(parallelism, self.replacement.clone(), self.join_type.clone()));

        let left_c = Arc::clone(&self.left);
        let right_c = Arc::clone(&self.right);

        let left_stream = left_c.execute(partition, Arc::clone(&context))?;
        let right_stream = right_c.execute(partition, context)?;

        let schema = Arc::clone(&self.schema());
        let on = self.on.clone();

        Ok(execution_instance.perform_streaming_join(
            partition,
            left_stream,
            right_stream,
            on,
        ))
    }
}
