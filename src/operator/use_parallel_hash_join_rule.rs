use std::fmt::Debug;
use std::sync::{Arc, OnceLock};
use datafusion::physical_optimizer::enforce_distribution::EnforceDistribution;
use datafusion::physical_optimizer::optimizer::PhysicalOptimizer;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::prelude::col;
use datafusion_common::config::ConfigOptions;
use datafusion_common::DataFusionError;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_physical_expr::PhysicalExprRef;
use datafusion_physical_expr_common::expressions::column::Column;
use datafusion_physical_plan::ExecutionPlan;
use datafusion_physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion_physical_plan::projection::ProjectionExec;
use crate::operator::parallel_hash_join::ParallelHashJoin;
use crate::operator::probe_lookup_implementation::probe_lookup_implementation::ProbeLookupStreamImplementation;
use crate::operator::use_work_stealing_repartition_rule::UseWorkStealingRepartitionRule;
use crate::parse_sql::JoinReplacement;

pub struct UseParallelHashJoinRule {
    version: JoinReplacement,
    required: bool,
}

impl UseParallelHashJoinRule {
    pub fn optimizer_rules(replacement: Option<JoinReplacement>, required: bool) -> Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>> {
        let mut optimizer_rules = PhysicalOptimizer::default().rules;

        // Insert the swap rule after joins are selected
        if let Some(replacement) = replacement {
            let rule = if required {
                UseParallelHashJoinRule::new_required(replacement)
            } else {
                UseParallelHashJoinRule::new(replacement)
            };
            // optimizer_rules.insert(3, Arc::new(rule));
            optimizer_rules.push(Arc::new(rule));
            optimizer_rules.push(Arc::new(UseWorkStealingRepartitionRule));
            optimizer_rules.push(Arc::new(EnforceDistribution::new()));
        }
        optimizer_rules
    }

    pub fn new(version: JoinReplacement) -> Self {
        Self { version, required: false }
    }

    pub fn new_required(version: JoinReplacement) -> Self {
        Self { version, required: true }
    }

    fn transform(&self, plan: Arc<dyn ExecutionPlan>) -> Result<Transformed<Arc<dyn ExecutionPlan>>, DataFusionError> {
        match plan.as_any().downcast_ref::<HashJoinExec>() {
            Some(hash_join_exec) => {
                // println!("Attempting to convert hash join to parallel hash join, version: {:?}, required: {}", self.version, self.required);
                match self.convert_hash_join(hash_join_exec) {
                    Ok(new_operator) => {
                        // println!("Attempting to convert hash join to parallel hash join, version: {:?}, required: {}, result: succeeded, projection: {:?}", self.version, self.required, hash_join_exec.projection);
                        Ok(Transformed::yes(new_operator))
                    },
                    Err(error) => {
                        if self.required {
                            panic!("Failed to apply ParallelJoin operator due to: {}", error);
                        }
                        // println!("Attempting to convert hash join to parallel hash join, version: {:?}, required: {}, result: {}", self.version, self.required, error);
                        Ok(Transformed::no(plan))
                    }
                }

            },
            None => Ok(Transformed::no(plan)),
        }
    }

    pub fn convert_hash_join(&self, value: &HashJoinExec) -> Result<Arc<dyn ExecutionPlan>, String> {
        let build_implementation_version = &self.version;

        if !ProbeLookupStreamImplementation::join_type_is_supported(value.join_type()) {
            return Err(format!("Unsupported join type {}", value.join_type));
        }
        // if value.filter.is_some() {
        //     return Err(format!("Filter not supported {:?}", value.filter));
        // }
        if value.mode != PartitionMode::Partitioned {
            return Err(format!("Mode not supported {:?}", value.mode));
        }
        if value.null_equals_null {
            return Err(format!("Null equals null not supported {:?}", value.null_equals_null));
        }

        if !Arc::ptr_eq(&value.left, value.children()[0]) || !Arc::ptr_eq(&value.right, value.children()[1]) {
            return Err("Children are not equal to left and right".to_string());
        }

        // if value.projection.is_some() {
        //     return Err(format!("Projection not supported {:?}", value.projection));
        // }

        let parallel_join = Arc::new(ParallelHashJoin::new(
            value.left.clone(),
            value.right.clone(),
            value.on.clone(),
            value.join_type.clone(),
            value.filter.clone(),
            build_implementation_version.clone(),
        ));

        // The parallel join doesn't yet support projections, so create a new projection operator
        let execution_node: Arc<dyn ExecutionPlan> = match &value.projection {
            Some(indices) => Self::wrap_with_projection(parallel_join, indices)?,
            None => parallel_join,
        };
        Ok(execution_node)
    }

    fn wrap_with_projection(
        parallel_join: Arc<ParallelHashJoin>,
        projection: &Vec<usize>,
    ) -> Result<Arc<ProjectionExec>, String> {
        let schema = parallel_join.schema();
        let expressions = projection.iter()
            .map(|index| {
                let name = schema.field(*index).name();
                let col = Arc::new(Column::new(name.as_str(), *index)) as PhysicalExprRef;
                (col, name.clone())
            })
            .collect::<Vec<_>>();
        let projection = ProjectionExec::try_new(expressions, parallel_join)
            .map_err(|e| format!("Failed to create projection operator: {:?}", e))?;
        Ok(Arc::new(projection))
    }
}

impl PhysicalOptimizerRule for UseParallelHashJoinRule {
    fn optimize(&self, plan: Arc<dyn ExecutionPlan>, _config: &ConfigOptions) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        Ok(plan.transform(|plan| self.transform(plan))?.data)
    }

    fn name(&self) -> &str {
        "UseParallelJoin"
    }

    fn schema_check(&self) -> bool {
        true
    }
}
