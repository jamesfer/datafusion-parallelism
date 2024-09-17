use std::fmt::Debug;
use std::sync::Arc;

use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion_common::config::ConfigOptions;
use datafusion_common::DataFusionError;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_physical_plan::ExecutionPlan;
use datafusion_physical_plan::joins::HashJoinExec;
use crate::operator::parallel_hash_join::ParallelHashJoin;
use crate::parse_sql::JoinReplacement;

pub struct UseParallelHashJoinRule {
    version: JoinReplacement,
}

impl UseParallelHashJoinRule {
    pub fn new(version: JoinReplacement) -> Self {
        Self { version }
    }
}

impl PhysicalOptimizerRule for UseParallelHashJoinRule {
    fn optimize(&self, plan: Arc<dyn ExecutionPlan>, _config: &ConfigOptions) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        Ok(plan.transform(|plan| transform(&self.version, plan))?.data)
    }

    fn name(&self) -> &str {
        "UseParallelJoin"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

fn transform(version: &JoinReplacement, plan: Arc<dyn ExecutionPlan>) -> Result<Transformed<Arc<dyn ExecutionPlan>>, DataFusionError> {
    match plan.as_any().downcast_ref::<HashJoinExec>() {
        Some(hash_join_exec) => {
            match ParallelHashJoin::convert_hash_join(hash_join_exec, version.clone()) {
                Ok(new_operator) => {
                    Ok(Transformed::yes(Arc::new(new_operator)))
                },
                Err(_error) => {
                    // println!("Failed to apply ParallelJoin operator due to: {}", error);
                    Ok(Transformed::no(plan))
                }
            }

        },
        None => Ok(Transformed::no(plan)),
    }
}
