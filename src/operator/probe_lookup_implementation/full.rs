use std::sync::{Arc, OnceLock};
use datafusion::arrow;
use datafusion::arrow::array::{ArrayRef, RecordBatch};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::error::ArrowError;
use datafusion::execution::SendableRecordBatchStream;
use datafusion_common::DataFusionError;
use datafusion_physical_expr::PhysicalExprRef;
use futures::stream::iter;
use futures::stream::StreamExt;
use futures::TryFutureExt;
use crate::shared::shared::{calculate_hash, evaluate_expressions, get_matching_indices_with_probe};
use crate::utils::concurrent_bit_set::ConcurrentBitSet;
use crate::utils::index_lookup::IndexLookup;
use crate::utils::initialize_copies_once::InitializeCopiesOnce;
use crate::utils::limited_rc::LimitedRc;
use crate::utils::plain_record_batch_stream::{PlainRecordBatchStream, SendablePlainRecordBatchStream};

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

pub fn full_join_streaming_lookup<Lookup>(
    join_schema: SchemaRef,
    read_only_join_map: Lookup,
    build_side_records: RecordBatch,
    probe_stream: SendableRecordBatchStream,
    probe_expressions: Vec<PhysicalExprRef>,
    build_side_visited_initializer: &OnceLock<Arc<ConcurrentBitSet>>,
    finalizer: LimitedRc<()>,
) -> impl PlainRecordBatchStream
    where Lookup: IndexLookup<u64> + Sync + Send
{
    let build_side_visited = Arc::clone(build_side_visited_initializer.get_or_init(||
        Arc::new(ConcurrentBitSet::with_capacity(build_side_records.num_rows()))
    ));
    let build_side_visited_clone = build_side_visited.clone();
    let join_schema_clone = join_schema.clone();
    let build_side_records_clone = build_side_records.clone();

    let probe_side_columns: Vec<_> = probe_stream.schema().fields.iter().map(|f| f.data_type().clone()).collect();

    probe_stream
        .map(move |result_probe_batch| -> Result<RecordBatch, DataFusionError> {
            let probe_batch = result_probe_batch?;

            // Hash the probe values
            let probe_keys = evaluate_expressions(&probe_expressions, &probe_batch)?;
            let probe_hashes = calculate_hash(&probe_keys)?;
            // Find matching rows from build side
            let (probe_indices, build_indices) = get_matching_indices_with_probe(&probe_hashes, &read_only_join_map);

            // TODO check for hash collisions

            // Update the visited build indices
            let mut vec = build_indices.iter().flatten().map(|x| x as usize).collect::<Vec<_>>();
            vec.sort();
            build_side_visited_clone.set_ones(vec);

            // Extract the rows matching the indices
            let output_columns = probe_batch.columns().iter()
                .map(|array| arrow::compute::take(array, &probe_indices, None))
                .chain(build_side_records_clone.columns().iter()
                    .map(|array| arrow::compute::take(array, &build_indices, None)))
                .collect::<Result<Vec<ArrayRef>, ArrowError>>()?;

            Ok(RecordBatch::try_new(join_schema_clone.clone(), output_columns)?)
        })
        // The use of an async block here is required to prevent the match from being consumed immediately
        .chain(
            async move {
                // This is the finalisation step and is only computed once
                match LimitedRc::into_inner(finalizer) {
                    None => Ok(iter(vec![])),
                    Some(_) => {
                        let unmatched_indices = build_side_visited.get_unset_indices_array();

                        let output_columns = probe_side_columns.iter()
                            .map(|data_type| Ok(arrow::array::new_null_array(data_type, unmatched_indices.len())))
                            .chain(build_side_records.columns().iter()
                                .map(|array| arrow::compute::take(array, &unmatched_indices, None)))
                            .collect::<Result<Vec<ArrayRef>, ArrowError>>()?;
                        Ok(iter(vec![Ok(RecordBatch::try_new(join_schema.clone(), output_columns)?)]))
                    }
                }
            }
                .try_flatten_stream()
        )
}
