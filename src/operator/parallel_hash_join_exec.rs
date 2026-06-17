use crate::operator::parallel_hash_join_executor::ParallelHashJoinExecutor;
use crate::operator::work_stealing_repartition_exec::WorkStealingRepartitionExec;
use crate::parse_sql::JoinReplacement;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::error::Result;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion_common::JoinType;
use datafusion_physical_expr::{Distribution, EquivalenceProperties, Partitioning, PhysicalExprRef};
use datafusion_physical_plan::joins::utils::{build_join_schema, JoinFilter};
use datafusion_physical_plan::{DisplayAs, DisplayFormatType, ExecutionMode, ExecutionPlan, ExecutionPlanProperties, PlanProperties};
use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::{Arc, OnceLock};

#[derive(Debug)]
pub struct ParallelHashJoinExec {
    properties: PlanProperties,
    left: Arc<dyn ExecutionPlan>,
    right: Arc<dyn ExecutionPlan>,
    on: Vec<(PhysicalExprRef, PhysicalExprRef)>,
    join_type: JoinType,
    filter: Option<JoinFilter>,
    build_implementation_version: JoinReplacement,
    executor_instance: Arc<OnceLock<ParallelHashJoinExecutor>>,
}

impl ParallelHashJoinExec {
    pub fn new(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        on: Vec<(PhysicalExprRef, PhysicalExprRef)>,
        join_type: JoinType,
        filter: Option<JoinFilter>,
        build_implementation_version: JoinReplacement,
    ) -> Self {
        let (schema, _) = build_join_schema(&left.schema(), &right.schema(), &join_type);
        Self {
            properties: Self::compute_properties(Arc::new(schema), &left),
            left,
            right,
            on,
            join_type,
            filter,
            build_implementation_version,
            executor_instance: Arc::new(OnceLock::new()),
        }
    }

    pub fn join_type(&self) -> &JoinType {
        &self.join_type
    }

    pub fn compute_properties(schema: SchemaRef, left: &Arc<dyn ExecutionPlan>) -> PlanProperties {
        PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::RoundRobinBatch(left.output_partitioning().partition_count()),
            ExecutionMode::Bounded,
        )
    }

}

impl DisplayAs for ParallelHashJoinExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        let schema = self.schema();
        let columns = schema.fields().iter().map(|f| f.name()).collect::<Vec<_>>();
        f.write_fmt(format_args!("ParallelHashJoin: join_type={:?} columns={:?}", self.join_type, columns))
    }
}

impl ExecutionPlan for ParallelHashJoinExec {
    fn name(&self) -> &str {
        "ParallelHashJoinExec"
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

    fn with_new_children(self: Arc<Self>, children: Vec<Arc<dyn ExecutionPlan>>) -> Result<Arc<dyn ExecutionPlan>> {
        let left = Arc::clone(&children[0]);
        Ok(Arc::new(ParallelHashJoinExec {
            properties: ParallelHashJoinExec::compute_properties(self.schema().clone(), &left),
            left,
            right: Arc::clone(&children[1]),
            on: self.on.clone(),
            join_type: self.join_type.clone(),
            filter: self.filter.clone(),
            build_implementation_version: self.build_implementation_version.clone(),
            // TODO this might break the data fusion contract
            executor_instance: self.executor_instance.clone(),
        }))
    }

    fn execute(&self, partition: usize, context: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
        let parallelism = self.properties.output_partitioning().partition_count();

        // Initialize the executor instance so all partitions can share state
        let execution_instance = self.executor_instance.get_or_init(||
            ParallelHashJoinExecutor::new(
                parallelism,
                self.build_implementation_version.clone(),
                self.join_type.clone(),
                self.left.schema(),
            )
        );

        // Convert each side of the join into a stream to consume
        let left_stream = self.left.execute(partition, Arc::clone(&context))?;
        let right_id = self.right.as_any().downcast_ref::<WorkStealingRepartitionExec>().map(|w| w.id());
        let right_stream = self.right.execute(partition, context)?;

        // In DataFusion, the left stream is the build side
        Ok(execution_instance.execute_streaming_join(
            partition,
            left_stream,
            right_stream,
            right_id,
            self.on.clone(),
            self.filter.clone(),
        ))
    }
}
