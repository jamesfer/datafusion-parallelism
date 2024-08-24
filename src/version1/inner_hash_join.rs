use std::future::ready;
use std::sync::Arc;

use crossbeam::atomic::AtomicCell;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::compute::concat_batches;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::execution::RecordBatchStream;
use datafusion_common::{DataFusionError, JoinType};
use datafusion_physical_expr::PhysicalExprRef;
use datafusion_physical_plan::SendableRecordBatchStream;
use futures::{StreamExt, TryStreamExt};
use futures_core::stream::Stream;

use crate::operator::build_implementation::IndexLookupConsumer;
use crate::shared::shared::{calculate_hash, evaluate_expressions};
use crate::shared::streaming_probe_lookup::streaming_probe_lookup;
use crate::utils::concurrent_join_map::{ConcurrentJoinMap, ReadOnlyJoinMap};
use crate::utils::limited_rc::LimitedRc;
use crate::version1::parallel_join_execution_state::ParallelJoinExecutionState;

#[derive(Debug)]
pub struct Version1 {
    state: ParallelJoinExecutionState,
}

impl Version1 {
    pub fn new(parallelism: usize) -> Self {
        Self {
            state: ParallelJoinExecutionState::new(parallelism),
        }
    }

    pub async fn build_right_side<C>(
        &self,
        partition: usize,
        stream: SendableRecordBatchStream,
        build_expressions: &Vec<PhysicalExprRef>,
        consume: C
    ) -> Result<C::R, DataFusionError>
        where C: IndexLookupConsumer + Send {
        let state = self.state.take(partition)
            .ok_or(DataFusionError::Internal(format!("State already consumed for partition {}", partition)))?;

        let right_schema = stream.schema().clone();
        consume_build_side(stream, &state.join_map, &build_expressions, &state.batch_list).await?;

        let (build_side_records, read_only_join_map) = compact_join_map(
            &right_schema,
            state.join_map,
            &state.batch_list,
            state.compacted_join_map_sender,
            state.compacted_join_map_receiver,
        ).await?;

        Ok(consume.call(read_only_join_map, build_side_records))
    }
}

pub async fn inner_join_stream(
    join_schema: SchemaRef,
    left: SendableRecordBatchStream,
    right: SendableRecordBatchStream,
    on: Vec<(PhysicalExprRef, PhysicalExprRef)>,
    join_map: LimitedRc<ConcurrentJoinMap<u64>>,
    batch_list: LimitedRc<boxcar::Vec<(usize, RecordBatch)>>,
    compact_join_map_sender: Arc<AtomicCell<Option<tokio::sync::broadcast::Sender<(RecordBatch, Arc<ReadOnlyJoinMap<u64>>)>>>>,
    compacted_join_map_receiver: tokio::sync::broadcast::Receiver<(RecordBatch, Arc<ReadOnlyJoinMap<u64>>)>
) -> Result<impl Stream<Item=Result<RecordBatch, DataFusionError>>, DataFusionError> {
    let (probe_expressions, build_expressions): (Vec<PhysicalExprRef>, Vec<PhysicalExprRef>) = on.into_iter().unzip();

    let right_schema = right.schema().clone();
    consume_build_side(right, &join_map, &build_expressions, &batch_list).await?;

    let (build_side_records, read_only_join_map) = compact_join_map(
        &right_schema,
        join_map,
        &batch_list,
        compact_join_map_sender,
        compacted_join_map_receiver,
    ).await?;

    // Stream left side
    Ok(streaming_probe_lookup(JoinType::Inner, join_schema, left, probe_expressions, build_side_records, read_only_join_map))
}

async fn consume_build_side(
    left: SendableRecordBatchStream,
    join_map: &ConcurrentJoinMap<u64>,
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
    join_map: &ConcurrentJoinMap<u64>,
    expressions: &Vec<PhysicalExprRef>,
    batch_list: &boxcar::Vec<(usize, RecordBatch)>,
) -> Result<(), DataFusionError> {
    let size = input.num_rows();
    let keys = evaluate_expressions(expressions, &input)?;
    let hashes = calculate_hash(&keys)?;

    let buffer_index = join_map.append_block(size, |inserter| {
        inserter.insert_all(hashes);
    });

    // Add the record batch to the list
    batch_list.push((buffer_index, input));
    Ok(())
}

async fn compact_join_map(
    input_schema: &SchemaRef,
    join_map: LimitedRc<ConcurrentJoinMap<u64>>,
    batch_list: &boxcar::Vec<(usize, RecordBatch)>,
    compact_join_map_sender: Arc<AtomicCell<Option<tokio::sync::broadcast::Sender<(RecordBatch, Arc<ReadOnlyJoinMap<u64>>)>>>>,
    mut compacted_join_map_receiver: tokio::sync::broadcast::Receiver<(RecordBatch, Arc<ReadOnlyJoinMap<u64>>)>
) -> Result<(RecordBatch, Arc<ReadOnlyJoinMap<u64>>), DataFusionError> {
    // If we are the last owners of the join map, we can run finalize
    match join_map.into_inner() {
        None => {
            compacted_join_map_receiver.recv().await
                .map_err(|err| DataFusionError::Internal(format!("Receive error {:?}", err)))
        }
        Some(join_map) => {
            let read_only_join_map = Arc::new(join_map.compact());

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
            let output_batch = concat_batches(input_schema, batches)?;

            let output = (output_batch, read_only_join_map);

            // Write the output to the sender
            match compact_join_map_sender.take() {
                None => Err(DataFusionError::Internal("Sender was already consumed".to_string())),
                Some(sender) => sender.send(output.clone())
                    .map_err(|err| DataFusionError::Internal("Send error".to_string())),
            }?;

            // Return the state
            Ok(output)
        }
    }
}
