use std::fmt::Debug;
use std::sync::{Arc, OnceLock};
use datafusion::physical_optimizer::optimizer::PhysicalOptimizer;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion_common::config::ConfigOptions;
use datafusion_common::DataFusionError;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_physical_plan::ExecutionPlan;
use datafusion_physical_plan::joins::{HashJoinExec, PartitionMode};
use crate::operator::parallel_hash_join::ParallelHashJoin;
use crate::operator::probe_lookup_implementation::probe_lookup_implementation::ProbeLookupStreamImplementation;
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
            optimizer_rules.insert(3, Arc::new(rule));
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
                        println!("Attempting to convert hash join to parallel hash join, version: {:?}, required: {}, result: succeeded, projection: {:?}", self.version, self.required, hash_join_exec.projection);
                        Ok(Transformed::yes(Arc::new(new_operator)))
                    },
                    Err(error) => {
                        if self.required {
                            panic!("Failed to apply ParallelJoin operator due to: {}", error);
                        }
                        println!("Attempting to convert hash join to parallel hash join, version: {:?}, required: {}, result: {}", self.version, self.required, error);
                        Ok(Transformed::no(plan))
                    }
                }

            },
            None => Ok(Transformed::no(plan)),
        }
    }

    pub fn convert_hash_join(&self, value: &HashJoinExec) -> Result<ParallelHashJoin, String> {
        let build_implementation_version = &self.version;

        if !ProbeLookupStreamImplementation::join_type_is_supported(value.join_type()) {
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
        if value.null_equals_null {
            return Err(format!("Null equals null not supported {:?}", value.null_equals_null));
        }

        if !Arc::ptr_eq(&value.left, value.children()[0]) || !Arc::ptr_eq(&value.right, value.children()[1]) {
            return Err("Children are not equal to left and right".to_string());
        }

        let eq_from_hash_join = value.properties().eq_properties.clone();
        let properties = ParallelHashJoin::compute_properties(eq_from_hash_join.clone(), &value.left);

        Ok(ParallelHashJoin::new(
            properties,
            value.left.clone(),
            value.right.clone(),
            value.on.clone(),
            value.join_type.clone(),
            build_implementation_version.clone(),
        ))
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
