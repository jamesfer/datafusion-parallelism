use std::future::ready;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::compute::concat_batches;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::execution::RecordBatchStream;
use datafusion_common::DataFusionError;
use datafusion_physical_expr::PhysicalExprRef;
use datafusion_physical_plan::SendableRecordBatchStream;
use futures::{StreamExt, TryStreamExt};
use crate::operator::lookup_consumers::{IndexLookupProvider, SimpleIndexLookupProvider};

use crate::operator::version2::parallel_join_execution_state::ParallelJoinExecutionState;
use crate::shared::shared::{calculate_hash, evaluate_expressions};
use crate::utils::partitioned_concurrent_join_map::{ReadonlyPartitionedConcurrentJoinMap, WritablePartitionedConcurrentJoinMap};

#[derive(Debug)]
pub struct Version2 {
    state: ParallelJoinExecutionState,
}

impl Version2 {
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
        let state = self.state.take(partition)
            .ok_or(DataFusionError::Internal(format!("State already consumed for partition {}", partition)))?;

        let right_schema = stream.schema().clone();
        consume_build_side(stream, &state.join_map, &build_expressions, &state.batch_list).await?;

        let (build_side_records, read_only_join_map) = compact_join_map(
            &right_schema,
            state.join_map,
            &state.batch_list,
        ).await?;

        Ok(SimpleIndexLookupProvider::new(read_only_join_map, build_side_records))
    }
}

async fn consume_build_side(
    left: SendableRecordBatchStream,
    join_map: &WritablePartitionedConcurrentJoinMap,
    build_expressions: &Vec<PhysicalExprRef>,
    batch_list: &boxcar::Vec<(usize, RecordBatch)>,
) -> Result<(), DataFusionError> {
    // Exhaust build side
    left.try_for_each(|record_batch| {
        ready(process_input_batch(
            record_batch,
            &join_map,
            &build_expressions,
            batch_list,
        ))
    }).await
}

fn process_input_batch(
    input: RecordBatch,
    join_map: &WritablePartitionedConcurrentJoinMap,
    expressions: &Vec<PhysicalExprRef>,
    batch_list: &boxcar::Vec<(usize, RecordBatch)>,
) -> Result<(), DataFusionError> {
    let keys = evaluate_expressions(expressions, &input)?;
    let hashes = calculate_hash(&keys)?;

    // let buffer_index = join_map.append_block(size, |inserter| {
    //     inserter.insert_all(hashes);
    // });
    let buffer_index = join_map.insert_all(hashes);

    // Add the record batch to the list
    batch_list.push((buffer_index, input));
    Ok(())
}

async fn compact_join_map(
    input_schema: &SchemaRef,
    join_map: WritablePartitionedConcurrentJoinMap,
    batch_list: &boxcar::Vec<(usize, RecordBatch)>,
) -> Result<(RecordBatch, ReadonlyPartitionedConcurrentJoinMap), DataFusionError> {
    let read_only_join_map = join_map.compact().await;

    // Concatenate the batches together
    let output_batch = {
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
        concat_batches(input_schema, batches)
    }?;

    // Return the state
    Ok((output_batch, read_only_join_map))
}
