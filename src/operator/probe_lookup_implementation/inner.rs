use datafusion::arrow;
use datafusion::arrow::array::{ArrayRef, RecordBatch};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::error::ArrowError;
use datafusion::execution::SendableRecordBatchStream;
use datafusion_common::DataFusionError;
use datafusion_physical_expr::PhysicalExprRef;
use futures::stream::StreamExt;
use crate::shared::shared::{calculate_hash, evaluate_expressions, get_matching_indices};
use crate::utils::index_lookup::IndexLookup;
use crate::utils::plain_record_batch_stream::{PlainRecordBatchStream, SendablePlainRecordBatchStream};

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
        build_side_records: RecordBatch,
        read_only_join_map: Lookup
    ) -> Result<SendablePlainRecordBatchStream, DataFusionError>
        where Lookup: IndexLookup<u64> + Send + Sync + 'static {
        Ok(Box::pin(inner_join_streaming_lookup(
            join_schema,
            probe_stream,
            probe_expressions,
            build_side_records,
            read_only_join_map,
        )))
    }
}

pub fn inner_join_streaming_lookup<Lookup>(
    output_schema: SchemaRef,
    probe_stream: SendableRecordBatchStream,
    probe_expressions: Vec<PhysicalExprRef>,
    build_side_records: RecordBatch,
    read_only_join_map: Lookup,
) -> impl PlainRecordBatchStream
    where Lookup: IndexLookup<u64> + Sync + Send {

    // println!("Starting stream lookup. Build size: {}, schema: {}", build_side_records.num_rows(), build_side_records.schema().fields.iter().map(|f| f.name()).fold(String::new(), |a, b| a + b + ","));
    probe_stream
        .map(move |result_probe_batch| -> Result<RecordBatch, DataFusionError> {
            let probe_batch = result_probe_batch?;

            // Hash the probe values
            let probe_keys = evaluate_expressions(&probe_expressions, &probe_batch)?;
            let probe_hashes = calculate_hash(&probe_keys)?;

            // Find matching rows from build side
            let (probe_indices, build_indices) = get_matching_indices(&probe_hashes, &read_only_join_map);

            let output_columns = build_side_records.columns().iter()
                .map(|array| arrow::compute::take(array, &build_indices, None))
                .chain(
                    probe_batch.columns().iter()
                        .map(|array| arrow::compute::take(array, &probe_indices, None))
                )
                .collect::<Result<Vec<ArrayRef>, ArrowError>>()?;
            return Ok(RecordBatch::try_new(output_schema.clone(), output_columns)?);
        })
}
