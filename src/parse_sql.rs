use std::sync::Arc;

use datafusion::catalog::CatalogProviderList;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::execution::session_state::SessionState;
use datafusion::physical_optimizer::optimizer::PhysicalOptimizer;
use datafusion_common::config::ConfigOptions;
use datafusion_common::DataFusionError;
use datafusion_physical_plan::ExecutionPlan;
use crate::version1;
use crate::version2;
use crate::version3;


pub enum JoinReplacement {
    Original,
    New,
    New3
}

pub fn make_session_state(use_new_join_rule: Option<JoinReplacement>) -> SessionState {
    // Use the physical optimizer
    let mut optimizer_rules = PhysicalOptimizer::default().rules;
    use_new_join_rule.map(|replacement| match replacement {
        JoinReplacement::Original => {
            optimizer_rules.insert(0, Arc::new(version1::UseParallelJoinRule));
        }
        JoinReplacement::New => {
            optimizer_rules.insert(0, Arc::new(version2::UseParallelJoinRule));
        }
        JoinReplacement::New3 => {
            optimizer_rules.insert(0, Arc::new(version3::UseParallelJoinRule));
        }
    });

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
        optimizer_rules.insert(0, Arc::new(version1::UseParallelJoinRule));
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
    Ok(session_state.create_physical_plan(&logical_plan).await?)
}
