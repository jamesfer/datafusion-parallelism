use std::future::ready;
use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::compute::concat_batches;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::execution::RecordBatchStream;
use datafusion_common::DataFusionError;
use datafusion_physical_expr::PhysicalExprRef;
use datafusion_physical_plan::SendableRecordBatchStream;
use futures::{StreamExt, TryStreamExt};
use crate::operator::lookup_consumers::{IndexLookupProvider, SimpleIndexLookupProvider};

use crate::operator::version8::hash_lookup_builder::LocalAccumulator;
use crate::operator::version8::parallel_join_execution_state::ParallelJoinExecutionState;
use crate::shared::shared::{calculate_hash, evaluate_expressions};
use crate::utils::async_initialize_once::AsyncInitializeOnce;
use crate::utils::partitioned_concurrent_join_map::{ReadonlyPartitionedConcurrentJoinMap, WritablePartitionedConcurrentJoinMap};

#[derive(Debug)]
pub struct Version8 {
    state: ParallelJoinExecutionState,
}

impl Version8 {
    pub fn new(parallelism: usize) -> Self {
        Self {
            state: ParallelJoinExecutionState::new(parallelism),
        }
    }

    pub async fn build_right_side(
        &self,
        partition: usize,
        stream: SendableRecordBatchStream,
        build_expressions: &Vec<PhysicalExprRef>,
    ) -> Result<impl IndexLookupProvider, DataFusionError> {
        let mut state = self.state.take(partition)
            .ok_or(DataFusionError::Internal(format!("State already consumed for partition {}", partition)))?;

        let right_schema = stream.schema().clone();
        consume_build_side(stream, &mut state.accumulator, &build_expressions).await?;

        // Once we are finished with the accumulator, we can submit it to convert it to a compactor
        let compactor = state.accumulator.submit();
        let (build_side_records, read_only_join_map) = compactor.compact(right_schema).await?;

        Ok(SimpleIndexLookupProvider::new(read_only_join_map, build_side_records))
    }
}

async fn consume_build_side(
    left: SendableRecordBatchStream,
    accumulator: &mut LocalAccumulator,
    build_expressions: &Vec<PhysicalExprRef>,
) -> Result<(), DataFusionError> {
    // Exhaust build side
    left.try_for_each(|record_batch| {
        ready(process_input_batch(
            record_batch,
            accumulator,
            &build_expressions,
        ))
    }).await
}

fn process_input_batch(
    input: RecordBatch,
    accumulator: &mut LocalAccumulator,
    expressions: &Vec<PhysicalExprRef>,
) -> Result<(), DataFusionError> {
    let keys = evaluate_expressions(expressions, &input)?;
    let hashes = calculate_hash(&keys)?;

    accumulator.add_records(keys, hashes, input);

    Ok(())
}

async fn compact_join_map(
    input_schema: &SchemaRef,
    join_map: WritablePartitionedConcurrentJoinMap,
    batch_list: &boxcar::Vec<(usize, RecordBatch)>,
    compute_compacted_batch_list: Arc<AsyncInitializeOnce<Result<RecordBatch, DataFusionError>>>,
) -> Result<(RecordBatch, ReadonlyPartitionedConcurrentJoinMap), DataFusionError> {
    // Waits until all copies of shards have been consumed, then distributes them via a channel.
    // Awaiting this ensures that all threads have finished writing to them before continuing with
    // the compaction operation.
    let shard_compactor = join_map.consume_and_distribute_shards().await;

    // Once all shards have been dispatched, we can compact the batch list into a single vector
    // which makes lookups much easier. This operation will only be executed by the first thread,
    // to call this method.
    let compaction_result = compute_compacted_batch_list.run_once(|| async {
        compact_batches(input_schema, batch_list)
    });

    match compaction_result {
        // The run_once method returns Ok if we are the first thread. If so, we should wait for the
        // operation to complete and then start participating in shard compaction. This ensures that
        // the thread is fully dedicated to compacting batches as it is quite expensive.
        Ok(compacted_batches_future) => {
            let compacted_batches = match compacted_batches_future.await {
                Ok(record_batch) => Ok(record_batch.clone()),
                Err(err_ref) => Err(DataFusionError::Internal(format!("Failed to compact batches due to: {}", err_ref.to_string()))),
            }?;
            let read_only_join_map = shard_compactor.compact_into_read_only_join_map().await;
            Ok((compacted_batches, read_only_join_map))
        }
        // Otherwise, the method returns Err if we are not the first thread. In this case, we can
        // immediately join the cooperative compaction operation and wait for the compacted batches
        // afterwards.
        Err(compacted_batches_future) => {
            let read_only_join_map = shard_compactor.compact_into_read_only_join_map().await;
            let compacted_batches = match compacted_batches_future.await {
                Ok(record_batch) => Ok(record_batch.clone()),
                Err(err_ref) => Err(DataFusionError::Internal(format!("Failed to compact batches due to: {}", err_ref.to_string()))),
            }?;
            Ok((compacted_batches, read_only_join_map))
        }
    }
}

fn compact_batches(input_schema: &SchemaRef, batch_list: &boxcar::Vec<(usize, RecordBatch)>) -> Result<RecordBatch, DataFusionError> {
    // Create a copy of just the target indices from the batch list. This array will be used for
    // many accesses, and it's more efficient to access a normal, non-concurrent vec than a boxcar.
    let target_indices: Vec<_> = batch_list.iter().map(|(index, _)| index).collect();

    // Create a range the size of the batch list, then sort it based on each index's target index.
    // This creates a vector where each value is the index of a record batch, and the position is
    // its sorted location based on the target index of that record batch.
    let mut sorted_indices: Vec<_> = (0..target_indices.len()).collect();
    // Whether stable or unstable sorting is faster here is probably debatable, given that the list
    // is probably very close to sorted
    sorted_indices.sort_by_key(|location_index| target_indices[*location_index]);

    let batches = sorted_indices.into_iter()
        .map(|location_index|
            batch_list.get(location_index)
                .map(|(_, batch)| batch)
                .ok_or(DataFusionError::Internal("Batch list missing index from sorted list".to_string()))
        )
        .collect::<Result<Vec<_>, _>>()?;
    Ok(concat_batches(input_schema, batches)?)
}
