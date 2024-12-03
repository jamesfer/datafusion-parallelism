use crate::shared::datafusion_private::{apply_join_filter_to_indices, equal_rows_arr};
use crate::shared::shared::{calculate_hash, evaluate_expressions, get_matching_indices, ProbeBuildIndices};
use crate::utils::concurrent_bit_set::ConcurrentBitSet;
use crate::utils::index_lookup::IndexLookup;
use crate::utils::initialize_copies_once::InitializeCopiesOnce;
use crate::utils::limited_rc::LimitedRc;
use crate::utils::plain_record_batch_stream::{PlainRecordBatchStream, SendablePlainRecordBatchStream};
use datafusion::arrow;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::execution::SendableRecordBatchStream;
use datafusion_common::{DataFusionError, JoinSide};
use datafusion_physical_expr::PhysicalExprRef;
use futures::stream::{iter, Iter, StreamExt};
use futures::{TryFutureExt, TryStreamExt};
use std::sync::{Arc, OnceLock};
use std::vec::IntoIter;
use datafusion_physical_plan::joins::utils::JoinFilter;
use futures_core::Stream;

// Right side is the probe side
#[derive(Debug)]
pub struct LeftSemiProbeLookupStream {
    parallelism: usize,
    build_side_visited_initializer: OnceLock<Arc<ConcurrentBitSet>>,
    finalizer_copies: InitializeCopiesOnce<()>,
}

impl LeftSemiProbeLookupStream {
    pub fn new(parallelism: usize) -> Self {
        Self {
            parallelism,
            build_side_visited_initializer: OnceLock::new(),
            finalizer_copies: InitializeCopiesOnce::new(parallelism),
        }
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
        Ok(Box::pin(left_semi_join_streaming_lookup(
            join_schema,
            probe_stream,
            probe_expressions,
            build_expressions,
            filter,
            build_side_records,
            read_only_join_map,
            &self.build_side_visited_initializer,
            self.finalizer_copies.get_clone_or_initialize(|| ())
                .map_err(|err| DataFusionError::Internal(err))?,
        )))
    }
}

fn left_semi_join_streaming_lookup<Lookup>(
    output_schema: SchemaRef,
    probe_stream: SendableRecordBatchStream,
    probe_expressions: Vec<PhysicalExprRef>,
    build_expressions: Vec<PhysicalExprRef>,
    filter: Option<JoinFilter>,
    build_side_records: RecordBatch,
    read_only_join_map: Lookup,
    build_side_visited_initializer: &OnceLock<Arc<ConcurrentBitSet>>,
    finalizer: LimitedRc<()>,
) -> impl PlainRecordBatchStream
    where Lookup: IndexLookup<u64> + Sync + Send {

    let build_side_visited = Arc::clone(build_side_visited_initializer.get_or_init(||
        Arc::new(ConcurrentBitSet::with_capacity(build_side_records.num_rows()))
    ));
    let build_side_visited_clone = build_side_visited.clone();
    let build_side_records_clone = build_side_records.clone();

    probe_stream
        .flat_map(move |result_probe_batch| {
            match result_probe_batch.and_then(|result_probe_batch| lookup_left_semi_join_probe_batch(
                &output_schema,
                &probe_expressions,
                &build_expressions,
                &filter,
                &build_side_records_clone,
                &build_side_visited_clone,
                &read_only_join_map,
                &result_probe_batch,
            )) {
                Ok(_) => iter(vec![]),
                Err(err) => iter(vec![Err(err)]),
            }
        })
        .chain(
            // The use of an async block here is required to prevent the match from being consumed immediately
            async move {
                emit_matched_build_records(
                    &build_side_records,
                    &build_side_visited,
                    finalizer,
                )
            }
                .try_flatten_stream()
        )
}

fn lookup_left_semi_join_probe_batch<Lookup>(
    output_schema: &SchemaRef,
    probe_expressions: &Vec<PhysicalExprRef>,
    build_expressions: &Vec<PhysicalExprRef>,
    filter: &Option<JoinFilter>,
    build_side_records: &RecordBatch,
    build_side_visited: &Arc<ConcurrentBitSet>,
    read_only_join_map: &Lookup,
    probe_batch: &RecordBatch,
) -> Result<(), DataFusionError>
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
    let (build_indices, _) = equal_rows_arr(
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

    Ok(())
}

fn emit_matched_build_records(
    build_side_records: &RecordBatch,
    build_side_visited: &ConcurrentBitSet,
    finalizer: LimitedRc<()>,
) -> Result<Iter<IntoIter<Result<RecordBatch, DataFusionError>>>, DataFusionError> {
    // This is the finalisation step and is only computed once
    match LimitedRc::into_inner(finalizer) {
        None => Ok(iter(vec![])),
        Some(_) => {
            let unmatched_indices = build_side_visited.get_set_indices_array();
            Ok(iter(vec![Ok(arrow::compute::take_record_batch(build_side_records, &unmatched_indices)?)]))
        }
    }
}
