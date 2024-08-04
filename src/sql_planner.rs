// use std::any::Any;
// use std::collections::HashMap;
// use std::fmt::Formatter;
// use std::sync::Arc;
//
// use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
// use async_trait::async_trait;
// use datafusion::catalog::{CatalogList, CatalogProvider, CatalogProviderList, MemoryCatalogProvider};
// use datafusion::catalog::schema::{MemorySchemaProvider, SchemaProvider};
// use datafusion::datasource::{DefaultTableSource, TableProvider, TableType};
// use datafusion::execution::{SendableRecordBatchStream, TaskContext};
// use datafusion::execution::context::SessionState;
// use datafusion::execution::runtime_env::RuntimeEnv;
// use datafusion::logical_expr::{AggregateUDF, Expr, LogicalPlan, ScalarUDF, TableProviderFilterPushDown, TableSource, WindowUDF};
// use datafusion::optimizer::{OptimizerConfig, OptimizerRule};
// use datafusion::optimizer::analyzer::AnalyzerRule;
// use datafusion::physical_planner::PhysicalPlanner;
// use datafusion::prelude::{SessionConfig, SessionContext};
// use datafusion::sql::planner::ContextProvider;
// use datafusion_common::{DataFusionError, plan_err, TableReference};
// use datafusion_common::config::{ConfigOptions, SqlParserOptions};
// use datafusion_physical_expr::{EquivalenceProperties, Partitioning};
// use datafusion_physical_plan::{displayable, DisplayAs, DisplayFormatType, ExecutionMode, ExecutionPlan, PlanProperties};
//
// use crate::coordinators::coordinator::Query;
//
// pub struct Planner {
//     // catalog_provider_list: Arc<dyn CatalogProviderList>,
//     session_state: SessionState,
// }
//
// impl Planner {
//     pub fn new(catalog_provider_list: Arc<dyn CatalogProviderList>) -> Self {
//         Self {
//             // catalog_provider_list,
//             session_state: SessionState::new_with_config_rt_and_catalog_list(
//                 ConfigOptions {
//                     sql_parser: SqlParserOptions {
//                         parse_float_as_decimal: false,
//                         enable_ident_normalization: false,
//                         dialect: "postgres".to_string(),
//                     },
//                     catalog: Default::default(),
//                     execution: Default::default(),
//                     optimizer: Default::default(),
//                     explain: Default::default(),
//                     extensions: Default::default(),
//                 }.into(),
//                 Arc::new(RuntimeEnv::default()),
//                 catalog_provider_list,
//             ),
//         }
//     }
//
//     pub async fn plan_query(&self, sql: &str) -> Result<Query, String> {
//         let execution_plan = self.sql_to_datafusion_physical_plan(sql)
//             .await
//             .map_err(|datafusion_err| format!("Error while planning sql query: {}", datafusion_err))?;
//         self.convert_datafusion_execution_plan_to_query(execution_plan)
//     }
//
//     async fn sql_to_datafusion_physical_plan(&self, sql: &str) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
//         // self.session_state.catalog_list().register_catalog("d".into(), d_catalog_provider);
//
//         // let statement = session_state.sql_to_statement(sql, "postgres")?;
//         // TODO this could call .create_logical_plan
//         let logical_plan = self.session_state.create_logical_plan(sql).await?;
//
//         // let optimized_plan = session_state.optimize(&logical_plan)?;
//
//         // create_physical_plan calls optimize on the logical plan
//         self.session_state.physical_optimizers()
//         Ok(self.session_state.create_physical_plan(&logical_plan).await?)
//     }
//
//     fn convert_datafusion_execution_plan_to_query(&self, execution_plan: Arc<dyn ExecutionPlan>) -> Result<Query, String> {
//
//     }
// }
//
// /// Source: https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/sql_frontend.rs
// pub async fn sql_to_physical_plan() -> Result<(), DataFusionError> {
//     // First, we parse the SQL string. Note that we use the DataFusion
//     // Parser, which wraps the `sqlparser-rs` SQL parser and adds DataFusion
//     // specific syntax such as `CREATE EXTERNAL TABLE`
//     // let dialect = PostgreSqlDialect {};
//     let sql = "SELECT person.name FROM d.s.person JOIN d.s.person AS j USING (age) WHERE person.age BETWEEN 21 AND 32 ";
//     // let statements = Parser::parse_sql(&dialect, sql)?;
//
//     let person_schema = Arc::new(Schema::new(vec![
//         Field::new("name", DataType::Utf8, false),
//         Field::new("age", DataType::UInt8, false),
//     ]));
//
//     let s_schema_provider = Arc::new(MemorySchemaProvider::new());
//     s_schema_provider.register_table("person".into(), Arc::new(MyTableProvider { schema: person_schema }))?;
//
//     let d_catalog_provider = Arc::new(MemoryCatalogProvider::new());
//     d_catalog_provider.register_schema("s".into(), s_schema_provider)?;
//
//     let session_state = SessionState::new_with_config_rt(
//         SessionConfig::new(),
//         Arc::new(RuntimeEnv::default()),
//     );
//     session_state.catalog_list().register_catalog("d".into(), d_catalog_provider);
//
//     let statement = session_state.sql_to_statement(sql, "postgres")?;
//     // TODO this could call .create_logical_plan
//     let logical_plan = session_state.statement_to_plan(statement).await?;
//     println!("logical plan");
//     println!("{}", logical_plan.display_indent().to_string());
//
//     let optimized_plan = session_state.optimize(&logical_plan)?;
//     println!("optimized plan");
//     println!("{}", optimized_plan.display_indent().to_string());
//
//     // create_physical_plan calls optimize
//     let physical_plan = session_state.create_physical_plan(&logical_plan).await?;
//
//     println!("{:?}", physical_plan);
//
//     println!("{}", displayable(physical_plan.as_ref()).indent(true));
//
//     println!("From logical plan");
//     let mut info = (vec![], HashMap::new());
//     let result = datafusion_substrait::logical_plan::producer::to_substrait_rel(
//         &optimized_plan,
//         &SessionContext::new(),
//         &mut info,
//     );
//     match result {
//         Ok(substrait) => println!("{:?}", substrait),
//         Err(err) => println!("Error {}", err),
//     }
//
//     println!("From physical plan");
//     let result = datafusion_substrait::physical_plan::producer::to_substrait_rel(
//         physical_plan.as_ref(),
//         &mut info,
//     );
//     match result {
//         Ok(substrait) => println!("{:?}", substrait),
//         Err(err) => println!("Error {}", err),
//     }
//
//     Ok(())
// }
//
// // Note that both the optimizer and the analyzer take a callback, called an
// // "observer" that is invoked after each pass. We do not do anything with these
// // callbacks in this example
//
// fn observe_analyzer(_plan: &LogicalPlan, _rule: &dyn AnalyzerRule) {}
// fn observe_optimizer(_plan: &LogicalPlan, _rule: &dyn OptimizerRule) {}
//
// /// Implements the `ContextProvider` trait required to plan SQL
// #[derive(Default)]
// struct MyContextProvider {
//     options: ConfigOptions,
// }
//
// impl ContextProvider for MyContextProvider {
//     fn get_table_source(&self, name: TableReference) -> Result<Arc<dyn TableSource>, DataFusionError> {
//         if name.table() == "person" {
//             let schema = Arc::new(Schema::new(vec![
//                 Field::new("name", DataType::Utf8, false),
//                 Field::new("age", DataType::UInt8, false),
//             ]));
//             Ok(Arc::new(DefaultTableSource::new(Arc::new(MyTableProvider { schema }))))
//             // Ok(Arc::new(MyTableSource {
//             //     schema: schema,
//             // }))
//         } else {
//             plan_err!("Table {} not found", name.table())
//         }
//     }
//
//     fn get_function_meta(&self, _name: &str) -> Option<Arc<ScalarUDF>> {
//         None
//     }
//
//     fn get_aggregate_meta(&self, _name: &str) -> Option<Arc<AggregateUDF>> {
//         None
//     }
//
//     fn get_window_meta(&self, _name: &str) -> Option<Arc<WindowUDF>> {
//         None
//     }
//
//     fn get_variable_type(&self, _variable_names: &[String]) -> Option<DataType> {
//         None
//     }
//
//     fn options(&self) -> &ConfigOptions {
//         &self.options
//     }
//
//     fn udfs_names(&self) -> Vec<String> {
//         Vec::new()
//     }
//
//     fn udafs_names(&self) -> Vec<String> {
//         Vec::new()
//     }
//
//     fn udwfs_names(&self) -> Vec<String> {
//         Vec::new()
//     }
// }
//
// /// TableSource is the part of TableProvider needed for creating a LogicalPlan.
// // struct MyTableSource {
// //     schema: SchemaRef,
// // }
// //
// // impl TableSource for MyTableSource {
// //     fn as_any(&self) -> &dyn Any {
// //         self
// //     }
// //
// //     fn schema(&self) -> SchemaRef {
// //         self.schema.clone()
// //     }
// //
// //     // For this example, we report to the DataFusion optimizer that
// //     // this provider can apply filters during the scan
// //     // fn supports_filters_pushdown(
// //     //     &self,
// //     //     filters: &[&Expr],
// //     // ) -> Result<Vec<TableProviderFilterPushDown>, DataFusionError> {
// //     //     Ok(vec![TableProviderFilterPushDown::Exact; filters.len()])
// //     // }
// // }
//
// struct MyTableProvider {
//     pub schema: SchemaRef,
// }
//
// #[async_trait]
// impl TableProvider for MyTableProvider {
//     fn as_any(&self) -> &dyn Any {
//         return self;
//     }
//
//     fn schema(&self) -> SchemaRef {
//         self.schema.clone()
//     }
//
//     fn table_type(&self) -> TableType {
//         TableType::Base
//     }
//
//     async fn scan(
//         &self,
//         state: &SessionState,
//         projection: Option<&Vec<usize>>,
//         filters: &[Expr],
//         limit: Option<usize>,
//     ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
//         let projected_schema = match projection {
//             None => self.schema.clone(),
//             Some(projection) => Arc::new(
//                 self.schema.project(projection)
//                     .map_err(|err| datafusion_common::DataFusionError::ArrowError(err, None))?
//             ),
//         };
//         Ok(Arc::new(MyTableScanExpr::new(projected_schema)))
//     }
//
//     fn supports_filters_pushdown(
//         &self,
//         filters: &[&Expr],
//     ) -> Result<Vec<TableProviderFilterPushDown>, DataFusionError> {
//         Ok(vec![TableProviderFilterPushDown::Exact; filters.len()])
//     }
// }
//
// #[derive(Debug)]
// pub struct MyTableScanExpr {
//     schema: SchemaRef,
//     plan_properties: PlanProperties,
// }
//
// impl MyTableScanExpr {
//     pub fn new(schema: SchemaRef) -> Self {
//         Self {
//             plan_properties: PlanProperties::new(
//                 EquivalenceProperties::new(schema.clone()),
//                 Partitioning::UnknownPartitioning(100),
//                 ExecutionMode::Bounded,
//             ),
//             schema,
//         }
//     }
// }
//
// impl DisplayAs for MyTableScanExpr {
//     fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
//         f.write_str("MyTableScanExpr")
//     }
// }
//
// impl ExecutionPlan for MyTableScanExpr {
//     fn as_any(&self) -> &dyn Any {
//         self
//     }
//
//     fn properties(&self) -> &PlanProperties {
//         &self.plan_properties
//     }
//
//     fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
//         vec![]
//     }
//
//     fn with_new_children(self: Arc<Self>, children: Vec<Arc<dyn ExecutionPlan>>) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
//         // This plan never has children, so we can just return it as is
//         Ok(self)
//     }
//
//     fn execute(&self, partition: usize, context: Arc<TaskContext>) -> datafusion_common::Result<SendableRecordBatchStream> {
//         unimplemented!()
//     }
// }
//
//
// #[cfg(test)]
// mod tests {
//     use crate::frontend::sql_planner::sql_to_physical_plan;
//
//     #[tokio::test(flavor = "multi_thread")]
//     async fn run() {
//         sql_to_physical_plan().await.unwrap();
//     }
// }
