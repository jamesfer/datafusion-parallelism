use std::fmt::Debug;
use std::sync::Arc;
use crossbeam::atomic::AtomicCell;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion_common::config::ConfigOptions;
use datafusion_common::DataFusionError;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_physical_plan::ExecutionPlan;
use datafusion_physical_plan::joins::HashJoinExec;
use crate::parallel_join::ParallelJoin;
use crate::utils::concurrent_join_map::{ConcurrentJoinMap, ReadOnlyJoinMap};

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
