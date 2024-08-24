use std::sync::{Arc, OnceLock};
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::execution::SendableRecordBatchStream;
use datafusion_common::DataFusionError;
use datafusion_physical_expr::PhysicalExprRef;
use crate::shared::streaming_probe_lookup::full_join_streaming_lookup;
use crate::utils::concurrent_bit_set::ConcurrentBitSet;
use crate::utils::index_lookup::IndexLookup;
use crate::utils::initialize_copies_once::InitializeCopiesOnce;
use crate::utils::plain_record_batch_stream::SendablePlainRecordBatchStream;

#[derive(Debug)]
pub struct FullJoinProbeLookupStream {
    parallelism: usize,
    build_side_visited_initializer: OnceLock<Arc<ConcurrentBitSet>>,
    finalizer_copies: InitializeCopiesOnce<()>,
}

impl FullJoinProbeLookupStream {
    pub fn new(parallelism: usize) -> Self {
        Self {
            parallelism,
            build_side_visited_initializer: OnceLock::new(),
            finalizer_copies: InitializeCopiesOnce::new(parallelism),
        }
    }

    pub fn streaming_probe_lookup<Lookup>(
        &self,
        output_schema: SchemaRef,
        probe_stream: SendableRecordBatchStream,
        probe_expressions: Vec<PhysicalExprRef>,
        build_side_records: RecordBatch,
        read_only_join_map: Lookup
    ) -> Result<SendablePlainRecordBatchStream, DataFusionError>
        where Lookup: IndexLookup<u64> + Sync + Send + 'static {
        Ok(Box::pin(full_join_streaming_lookup(
            output_schema,
            read_only_join_map,
            build_side_records,
            probe_stream,
            probe_expressions,
            &self.build_side_visited_initializer,
            self.finalizer_copies.get_clone_or_initialize(|| ())
                .map_err(|err| DataFusionError::Internal(err))?,
        )))
    }
}
