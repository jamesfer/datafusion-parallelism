use crate::shared::datafusion_private::{apply_join_filter_to_indices, equal_rows_arr, get_anti_indices};
use crate::shared::shared::{calculate_hash, evaluate_expressions, get_matching_indices, ProbeBuildIndices};
use crate::utils::index_lookup::IndexLookup;
use crate::utils::plain_record_batch_stream::{PlainRecordBatchStream, SendablePlainRecordBatchStream};
use datafusion::arrow;
use datafusion::arrow::array::{ArrayRef, RecordBatch};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::error::ArrowError;
use datafusion::execution::SendableRecordBatchStream;
use datafusion_common::{DataFusionError, JoinSide};
use datafusion_physical_expr::PhysicalExprRef;
use datafusion_physical_plan::joins::utils::JoinFilter;
use futures::stream::StreamExt;

// Right side is the probe side
#[derive(Debug)]
pub struct RightAntiProbeLookupStream {
    parallelism: usize,
}

impl RightAntiProbeLookupStream {
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
        Ok(Box::pin(right_anti_join_streaming_lookup(
            join_schema,
            probe_stream,
            probe_expressions,
            build_expressions,
            filter,
            build_side_records,
            read_only_join_map,
        )))
    }
}

fn right_anti_join_streaming_lookup<Lookup>(
    output_schema: SchemaRef,
    probe_stream: SendableRecordBatchStream,
    probe_expressions: Vec<PhysicalExprRef>,
    build_expressions: Vec<PhysicalExprRef>,
    filter: Option<JoinFilter>,
    build_side_records: RecordBatch,
    read_only_join_map: Lookup,
) -> impl PlainRecordBatchStream
    where Lookup: IndexLookup<u64> + Sync + Send {

    probe_stream
        .map(move |result_probe_batch| -> Result<RecordBatch, DataFusionError> {
            let probe_batch = result_probe_batch?;
            lookup_right_anti_join_probe_batch(
                &output_schema,
                &probe_expressions,
                &build_expressions,
                &filter,
                &build_side_records,
                &read_only_join_map,
                &probe_batch,
            )
        })
}

fn lookup_right_anti_join_probe_batch<Lookup>(
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
    let (_, probe_indices) = equal_rows_arr(
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

    // Anti joins need to use unmatched probe indices
    let unmatched_probe_indices = get_anti_indices(0usize..probe_batch.num_rows(), &probe_indices);
    Ok(arrow::compute::take_record_batch(probe_batch, &unmatched_probe_indices)?)
}
