use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::thread::ThreadId;
use datafusion::arrow;
use datafusion::arrow::array::{ArrayRef, RecordBatch};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::error::ArrowError;
use datafusion::execution::SendableRecordBatchStream;
use datafusion_common::{DataFusionError, JoinSide};
use datafusion_physical_expr::PhysicalExprRef;
use datafusion_physical_plan::joins::utils::JoinFilter;
use futures::{stream, TryFutureExt};
use futures::stream::StreamExt;
use crate::shared::datafusion_private::{apply_join_filter_to_indices, equal_rows_arr};
use crate::shared::shared::{calculate_hash, evaluate_expressions, get_matching_indices, take_multiple_record_batch, ProbeBuildIndices};
use crate::utils::index_lookup::IndexLookup;
use crate::utils::plain_record_batch_stream::SendablePlainRecordBatchStream;

#[derive(Debug)]
pub struct InnerJoinProbeLookupStream {
    parallelism: usize,
}

impl InnerJoinProbeLookupStream {
    pub fn new(parallelism: usize) -> Self {
        Self { parallelism }
    }

    pub fn streaming_probe_lookup<Lookup>(
        &self,
        join_schema: SchemaRef,
        probe_stream: SendableRecordBatchStream,
        probe_expressions: Vec<PhysicalExprRef>,
        build_expressions: Vec<PhysicalExprRef>,
        filter: Option<JoinFilter>,
        build_side_records: RecordBatch,
        read_only_join_map: Lookup
    ) -> Result<SendablePlainRecordBatchStream, DataFusionError>
        where Lookup: IndexLookup<u64> + Send + Sync + 'static {
        // let thread_transitions = Arc::new(AtomicU64::new(0));
        // let thread_transitions_clone = Arc::clone(&thread_transitions);
        //
        // let mut previous_thread_id = None;

        // let thread_usage: HashMap<usize, Vec<ThreadId>> = HashMap::new();

        Ok(Box::pin(probe_stream
            .map(move |result_probe_batch| -> Result<RecordBatch, DataFusionError> {
                // let current_thread_id = std::thread::current().id();
                // if let Some(previous_thread_id) = previous_thread_id {
                //     if current_thread_id != previous_thread_id {
                //         thread_transitions_clone.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                //     }
                // }
                // previous_thread_id = Some(current_thread_id);


                lookup_inner_join_probe_batch(
                    &join_schema,
                    &probe_expressions,
                    &build_expressions,
                    &filter,
                    &build_side_records,
                    &read_only_join_map,
                    &result_probe_batch?,
                )
            })
            .chain(
                async move {
                    // println!("Thread transitions: {}", thread_transitions.load(std::sync::atomic::Ordering::Relaxed));
                    Ok(stream::empty())
                }.try_flatten_stream()
            )
        ))
    }
}

fn lookup_inner_join_probe_batch<Lookup>(
    output_schema: &SchemaRef,
    probe_expressions: &Vec<PhysicalExprRef>,
    build_expressions: &Vec<PhysicalExprRef>,
    filter: &Option<JoinFilter>,
    build_side_records: &RecordBatch,
    read_only_join_map: &Lookup,
    probe_batch: &RecordBatch,
) -> Result<RecordBatch, DataFusionError>
where
    Lookup: IndexLookup<u64> + Sync + Send
{
    // Hash the probe values
    let probe_keys = evaluate_expressions(&probe_expressions, &probe_batch)?;
    let probe_hashes = calculate_hash(&probe_keys)?;

    // Find matching rows from build side
    let ProbeBuildIndices { probe_indices, build_indices } =
        get_matching_indices(&probe_hashes, read_only_join_map);

    // Filter out rows that don't have equal values, protecting against hash collisions
    let build_keys = evaluate_expressions(&build_expressions, &build_side_records)?;
    let (build_indices, probe_indices) = equal_rows_arr(
        &build_indices,
        &probe_indices,
        build_keys.as_ref(),
        probe_keys.as_ref(),
        // TODO support null_equals_null parameter
        false,
    )?;

    // Apply join filter if exists
    let (build_indices, probe_indices) = if let Some(filter) = &filter {
        apply_join_filter_to_indices(
            build_side_records,
            probe_batch,
            build_indices,
            probe_indices,
            filter,
            JoinSide::Left,
        )?
    } else {
        (build_indices, probe_indices)
    };

    let output_columns = take_multiple_record_batch(vec![
        (build_side_records, &build_indices),
        (probe_batch, &probe_indices),
    ])?;
    Ok(RecordBatch::try_new(output_schema.clone(), output_columns)?)
}
