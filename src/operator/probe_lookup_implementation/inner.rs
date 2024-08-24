use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::execution::SendableRecordBatchStream;
use datafusion_common::DataFusionError;
use datafusion_physical_expr::PhysicalExprRef;
use crate::shared::streaming_probe_lookup::inner_join_streaming_lookup;
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
