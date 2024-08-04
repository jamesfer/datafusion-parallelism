use std::sync::Arc;
use datafusion::catalog::CatalogProviderList;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::execution::session_state::SessionState;
use datafusion::physical_optimizer::optimizer::PhysicalOptimizer;
use datafusion_common::config::{CatalogOptions, ConfigOptions, ExecutionOptions, ExplainOptions, Extensions, OptimizerOptions, SqlParserOptions};
use datafusion_common::DataFusionError;
use datafusion_physical_plan::ExecutionPlan;
use crate::use_parallel_join_rule::UseParallelJoinRule;

pub fn make_session_state(use_new_join_rule: bool) -> SessionState {
    // Use the physical optimizer
    let mut optimizer_rules = PhysicalOptimizer::default().rules;
    if use_new_join_rule {
        optimizer_rules.insert(0, Arc::new(UseParallelJoinRule));
    }

    let mut options = ConfigOptions::default();
    options.sql_parser.dialect = "postgres".to_string();
    SessionState::new_with_config_rt(
        options.into(),
        Arc::new(RuntimeEnv::default()),
    )
        .with_physical_optimizer_rules(optimizer_rules)
}

pub fn make_session_state_with_catalog(catalog_provider_list: Arc<dyn CatalogProviderList>, use_new_join_rule: bool) -> SessionState {
    // Use the physical optimizer
    let mut optimizer_rules = PhysicalOptimizer::default().rules;
    if use_new_join_rule {
        optimizer_rules.insert(0, Arc::new(UseParallelJoinRule));
    }

    let mut options = ConfigOptions::default();
    options.sql_parser.dialect = "postgres".to_string();
    SessionState::new_with_config_rt_and_catalog_list(
        options.into(),
        Arc::new(RuntimeEnv::default()),
        catalog_provider_list,
    )
        .with_physical_optimizer_rules(optimizer_rules)
}

pub async fn parse_sql(sql: &str, session_state: &SessionState) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
    let logical_plan = session_state.create_logical_plan(sql).await?;

    // let optimized_plan = session_state.optimize(&logical_plan)?;

    // create_physical_plan calls optimize on the logical plan
    // session_state.physical_optimizers();
    // let optimized_logical_plan = session_state.optimize(&logical_plan)?;
    // let physical_plan = session_state.quer
    // self.query_planner
    //     .create_physical_plan(&logical_plan, self)
    //     .await
    Ok(session_state.create_physical_plan(&logical_plan).await?)
}
