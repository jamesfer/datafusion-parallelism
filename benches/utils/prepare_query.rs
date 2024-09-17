use std::sync::Arc;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::context::SessionState;
use datafusion::execution::TaskContext;
use datafusion_physical_plan::collect;
use datafusion_physical_plan::display::DisplayableExecutionPlan;
use futures_core::future::BoxFuture;

pub async fn prepare_query<'a>(session_state: &'a SessionState, sql: &str) -> Box<dyn FnMut() -> BoxFuture<'a, Vec<RecordBatch>> + 'a> {
    let logical_plan = session_state.create_logical_plan(sql).await.unwrap();

    Box::new(move || {
        // Clone the plan, so we only capture a reference to it
        let logical_plan = logical_plan.clone();
        Box::pin(async move {
            let physical_plan = session_state.create_physical_plan(&logical_plan).await.unwrap();

            // println!("Physical plan: {}", DisplayableExecutionPlan::new(physical_plan.as_ref()).indent(true));

            let result = collect(physical_plan, Arc::new(TaskContext::default()))
                .await
                .unwrap();
            result
        })
    })
}
