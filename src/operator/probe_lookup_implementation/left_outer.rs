use datafusion::arrow;
use datafusion::arrow::array::{ArrayRef, RecordBatch};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::error::ArrowError;
use datafusion::execution::SendableRecordBatchStream;
use datafusion_common::DataFusionError;
use datafusion_physical_expr::PhysicalExprRef;
use futures::stream::StreamExt;

use crate::shared::shared::{calculate_hash, evaluate_expressions, get_matching_indices_with_probe};
use crate::utils::index_lookup::IndexLookup;
use crate::utils::plain_record_batch_stream::{PlainRecordBatchStream, SendablePlainRecordBatchStream};

#[derive(Debug)]
pub struct LeftOuterProbeLookupStream {
    parallelism: usize,
}

impl LeftOuterProbeLookupStream {
    pub fn new(parallelism: usize) -> Self {
        Self { parallelism }
    }

    pub fn streaming_probe_lookup<Lookup>(
        &self,
        join_schema: SchemaRef,
        probe_stream: SendableRecordBatchStream,
        probe_expressions: Vec<PhysicalExprRef>,
        build_side_records: RecordBatch,
        read_only_join_map: Lookup
    ) -> Result<SendablePlainRecordBatchStream, DataFusionError>
        where Lookup: IndexLookup<u64> + Send + Sync + 'static {
        Ok(Box::pin(left_outer_join_streaming_lookup(
            join_schema,
            probe_stream,
            probe_expressions,
            build_side_records,
            read_only_join_map,
        )))
    }
}

pub fn left_outer_join_streaming_lookup<Lookup>(
    output_schema: SchemaRef,
    probe_stream: SendableRecordBatchStream,
    probe_expressions: Vec<PhysicalExprRef>,
    build_side_records: RecordBatch,
    read_only_join_map: Lookup,
) -> impl PlainRecordBatchStream
    where Lookup: IndexLookup<u64> + Sync + Send {

    probe_stream
        .map(move |result_probe_batch| -> Result<RecordBatch, DataFusionError> {
            let probe_batch = result_probe_batch?;

            // Hash the probe values
            let probe_keys = evaluate_expressions(&probe_expressions, &probe_batch)?;
            let probe_hashes = calculate_hash(&probe_keys)?;

            // Find matching rows from build side
            let (probe_indices, build_indices) = get_matching_indices_with_probe(&probe_hashes, &read_only_join_map);

            // TODO check for hash collisions

            let output_columns = probe_batch.columns().iter()
                .map(|array| arrow::compute::take(array, &probe_indices, None))
                .chain(build_side_records.columns().iter()
                    .map(|array| arrow::compute::take(array, &build_indices, None)))
                .collect::<Result<Vec<ArrayRef>, ArrowError>>()?;
            return Ok(RecordBatch::try_new(output_schema.clone(), output_columns)?);
        })
}
