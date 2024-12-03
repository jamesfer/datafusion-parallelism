use std::sync::{Arc, OnceLock};
use std::vec::IntoIter;
use datafusion::arrow;
use datafusion::arrow::array::{ArrayRef, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Schema, SchemaRef};
use datafusion::arrow::error::ArrowError;
use datafusion::execution::SendableRecordBatchStream;
use datafusion_common::{DataFusionError, JoinSide};
use datafusion_physical_expr::PhysicalExprRef;
use datafusion_physical_plan::joins::utils::JoinFilter;
use futures::stream::{iter, Iter};
use futures::stream::StreamExt;
use futures::TryFutureExt;
use crate::shared::datafusion_private::{append_right_indices, apply_join_filter_to_indices, equal_rows_arr};
use crate::shared::shared::{calculate_hash, evaluate_expressions, get_matching_indices_with_probe, take_multiple_record_batch, ProbeBuildIndices};
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
        build_expressions: Vec<PhysicalExprRef>,
        filter: Option<JoinFilter>,
        build_side_records: RecordBatch,
        read_only_join_map: Lookup
    ) -> Result<SendablePlainRecordBatchStream, DataFusionError>
        where Lookup: IndexLookup<u64> + Sync + Send + 'static {
        Ok(Box::pin(full_join_streaming_lookup(
            output_schema,
            read_only_join_map,
            probe_stream,
            probe_expressions,
            build_expressions,
            filter,
            build_side_records,
            &self.build_side_visited_initializer,
            self.finalizer_copies.get_clone_or_initialize(|| ())
                .map_err(|err| DataFusionError::Internal(err))?,
        )))
    }
}

pub fn full_join_streaming_lookup<Lookup>(
    join_schema: SchemaRef,
    read_only_join_map: Lookup,
    probe_stream: SendableRecordBatchStream,
    probe_expressions: Vec<PhysicalExprRef>,
    build_expressions: Vec<PhysicalExprRef>,
    filter: Option<JoinFilter>,
    build_side_records: RecordBatch,
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
            lookup_full_join_probe_batch(
                &join_schema_clone,
                &probe_expressions,
                &build_expressions,
                &filter,
                &build_side_records_clone,
                &build_side_visited_clone,
                &read_only_join_map,
                &result_probe_batch?,
            )
        })
        .chain(
            // The use of an async block here is required to prevent the match from being consumed immediately
            async move {
                emit_unmatched_build_records(
                    &join_schema,
                    &probe_side_columns,
                    &build_side_records,
                    &build_side_visited,
                    finalizer,
                )
            }
                .try_flatten_stream()
        )
}

fn lookup_full_join_probe_batch<Lookup>(
    output_schema: &SchemaRef,
    probe_expressions: &Vec<PhysicalExprRef>,
    build_expressions: &Vec<PhysicalExprRef>,
    filter: &Option<JoinFilter>,
    build_side_records: &RecordBatch,
    build_side_visited: &ConcurrentBitSet,
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
        get_matching_indices_with_probe(&probe_hashes, read_only_join_map);

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

    // Update the visited build indices
    let mut vec = build_indices.iter().flatten().map(|x| x as usize).collect::<Vec<_>>();
    vec.sort();
    build_side_visited.set_ones(vec);

    // The full join needs the probe indices from non-matching rows
    let (build_indices, probe_indices) = append_right_indices(
        build_indices,
        probe_indices,
        0usize..probe_batch.num_rows(),
        false,
    );

    // Extract the rows matching the indices
    let output_columns = take_multiple_record_batch(vec![
        (build_side_records, &build_indices),
        (probe_batch, &probe_indices),
    ])?;
    Ok(RecordBatch::try_new(output_schema.clone(), output_columns)?)
}

fn emit_unmatched_build_records(
    join_schema: &SchemaRef,
    probe_side_columns: &Vec<DataType>,
    build_side_records: &RecordBatch,
    build_side_visited: &ConcurrentBitSet,
    finalizer: LimitedRc<()>,
) -> Result<Iter<IntoIter<Result<RecordBatch, DataFusionError>>>, DataFusionError> {
    // This is the finalisation step and is only computed once
    match LimitedRc::into_inner(finalizer) {
        None => Ok(iter(vec![])),
        Some(_) => {
            let unmatched_indices = build_side_visited.get_unset_indices_array();
            let output_columns = build_side_records.columns().iter()
                .map(|array| arrow::compute::take(array, &unmatched_indices, None))
                .chain(probe_side_columns.iter()
                    .map(|data_type| Ok(arrow::array::new_null_array(data_type, unmatched_indices.len()))))
                .collect::<Result<Vec<ArrayRef>, ArrowError>>()?;
            Ok(iter(vec![Ok(RecordBatch::try_new(join_schema.clone(), output_columns)?)]))
        }
    }
}
