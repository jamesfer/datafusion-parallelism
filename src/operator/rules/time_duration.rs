use std::any::Any;
use std::fmt;
use std::sync::Arc;
use std::time::Instant;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion_common::config::ConfigOptions;
use datafusion_common::DataFusionError;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_physical_plan::{ExecutionPlan, PlanProperties, DisplayAs, DisplayFormatType};
use datafusion_physical_plan::joins::HashJoinExec;
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use futures::stream::StreamExt;

pub struct TimeBuildDuration;

impl TimeBuildDuration {
    fn transform(
        plan: Arc<dyn ExecutionPlan>,
    ) -> Result<Transformed<Arc<dyn ExecutionPlan>>, DataFusionError> {
        match plan.as_any().downcast_ref::<HashJoinExec>() {
            Some(hash_join_exec) => {
                let children = hash_join_exec.children();
                let build = children[0].clone();
                let new_build = Arc::new(TimeDurationExec { input: build });
                let probe = children[1].clone();

                let new_hash_join = plan.with_new_children(vec![new_build, probe])?;
                Ok(Transformed::yes(new_hash_join))
            },
            None => Ok(Transformed::no(plan)),
        }
    }
}

impl PhysicalOptimizerRule for TimeBuildDuration {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        Ok(plan.transform(|plan| Self::transform(plan))?.data)
    }

    fn name(&self) -> &str {
        "TimeDuration"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

#[derive(Debug)]
pub struct TimeDurationExec {
    input: Arc<dyn ExecutionPlan>,
}

impl DisplayAs for TimeDurationExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "TimeDurationExec")
    }
}

impl ExecutionPlan for TimeDurationExec {
    fn name(&self) -> &str {
        "TimeDurationExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        self.input.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(self: Arc<Self>, children: Vec<Arc<dyn ExecutionPlan>>) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        let input = children[0].clone();
        Ok(Arc::new(TimeDurationExec { input }))
    }

    fn execute(&self, partition: usize, context: Arc<TaskContext>) -> datafusion_common::Result<SendableRecordBatchStream> {
        let input = self.input.execute(partition, context)?;
        let schema = input.schema();
        let cols = Arc::new(schema.fields().iter().map(|f| f.name()).cloned().collect::<Vec<_>>());
        let start_time = Instant::now();

        let stream = futures::stream::unfold(
            (input, start_time, false),
            move |(mut input, start_time, completed)| {
                let cols = cols.clone();
                async move {
                    if completed {
                        return None;
                    }

                    match input.next().await {
                        Some(result) => Some((result, (input, start_time, false))),
                        None => {
                            let duration = start_time.elapsed();
                            if cols.contains(&"o_orderkey".to_string()) {
                                println!("build timing: {:?} with columns {:?}", duration, cols);
                            }
                            None
                        }
                    }
                }
            }
        );

        let stream = RecordBatchStreamAdapter::new(schema, stream);

        Ok(Box::pin(stream))
    }
}
