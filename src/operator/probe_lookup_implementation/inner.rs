use datafusion::arrow;
use datafusion::arrow::array::{ArrayRef, RecordBatch};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::error::ArrowError;
use datafusion::execution::SendableRecordBatchStream;
use datafusion_common::DataFusionError;
use datafusion_physical_expr::PhysicalExprRef;
use futures::stream::StreamExt;
use crate::shared::datafusion_private::equal_rows_arr;
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
        build_side_records: RecordBatch,
        read_only_join_map: Lookup
    ) -> Result<SendablePlainRecordBatchStream, DataFusionError>
        where Lookup: IndexLookup<u64> + Send + Sync + 'static {
        Ok(Box::pin(probe_stream
            .map(move |result_probe_batch| -> Result<RecordBatch, DataFusionError> {
                lookup_inner_join_probe_batch(
                    &join_schema,
                    &probe_expressions,
                    &build_expressions,
                    &build_side_records,
                    &read_only_join_map,
                    &result_probe_batch?,
                )
            })))
    }
}

fn lookup_inner_join_probe_batch<Lookup>(
    output_schema: &SchemaRef,
    probe_expressions: &Vec<PhysicalExprRef>,
    build_expressions: &Vec<PhysicalExprRef>,
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

    // TODO support filter

    let output_columns = take_multiple_record_batch(vec![
        (build_side_records, &build_indices),
        (probe_batch, &probe_indices),
    ])?;
    Ok(RecordBatch::try_new(output_schema.clone(), output_columns)?)
}
