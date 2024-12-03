use std::sync::Arc;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion_common::config::ConfigOptions;
use datafusion_common::DataFusionError;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_physical_plan::ExecutionPlan;
use crate::operator::parallel_hash_join::ParallelHashJoin;
use crate::operator::work_stealing_repartition_exec::WorkStealingRepartitionExec;

pub struct UseWorkStealingRepartitionRule;

impl PhysicalOptimizerRule for UseWorkStealingRepartitionRule {
    fn optimize(&self, plan: Arc<dyn ExecutionPlan>, config: &ConfigOptions) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let mut id = 0;
        Ok(plan.transform(|plan| {
            match plan.as_any().downcast_ref::<ParallelHashJoin>() {
                Some(parallel_join) => {
                    let (replaced, new_children) = parallel_join.children().into_iter()
                        .map(|child| -> (bool, Arc<dyn ExecutionPlan>) {
                            match child.as_any().downcast_ref::<WorkStealingRepartitionExec>() {
                                None => {
                                    id += 1;
                                    (true, Arc::new(WorkStealingRepartitionExec::new(id, child.clone())))
                                },
                                Some(_) => (false, child.clone())
                            }
                        })
                        .unzip::<_, _, Vec<_>, Vec<_>>();
                    if replaced.iter().any(|replaced| *replaced) {
                        let result = plan.with_new_children(new_children)?;
                        return Ok(Transformed::yes(result))
                    }
                    return Ok(Transformed::no(plan));
                }
                None => Ok(Transformed::no(plan))
            }
        })?.data)
    }

    fn name(&self) -> &str {
        "UseWorkStealingRepartitionRule"
    }

    fn schema_check(&self) -> bool {
        true
    }
}
