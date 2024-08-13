use std::future::ready;

use ahash::RandomState;
use datafusion::arrow;
use datafusion::arrow::array::{ArrayRef, PrimitiveArray, RecordBatch, UInt32Array, UInt32BufferBuilder};
use datafusion::arrow::buffer::ScalarBuffer;
use datafusion::arrow::compute::concat_batches;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::error::ArrowError;
use datafusion::execution::RecordBatchStream;
use datafusion_common::DataFusionError;
use datafusion_physical_expr::PhysicalExprRef;
use datafusion_physical_plan::SendableRecordBatchStream;
use futures::{StreamExt, TryStreamExt};
use futures_core::stream::Stream;

use crate::utils::limited_rc::LimitedRc;
use crate::utils::partitioned_concurrent_join_map::{ReadonlyPartitionedConcurrentJoinMap, WritablePartitionedConcurrentJoinMap};

pub async fn inner_join_stream(
    join_schema: SchemaRef,
    left: SendableRecordBatchStream,
    right: SendableRecordBatchStream,
    on: Vec<(PhysicalExprRef, PhysicalExprRef)>,
    join_map: WritablePartitionedConcurrentJoinMap,
    batch_list: LimitedRc<boxcar::Vec<(usize, RecordBatch)>>,
) -> Result<impl Stream<Item=Result<RecordBatch, DataFusionError>>, DataFusionError> {
    let (probe_expressions, build_expressions): (Vec<PhysicalExprRef>, Vec<PhysicalExprRef>) = on.into_iter().unzip();

    let right_schema = right.schema().clone();
    consume_build_side(right, &join_map, &build_expressions, &batch_list).await?;

    let (build_side_records, read_only_join_map) = compact_join_map(
        &right_schema,
        join_map,
        &batch_list
    ).await?;

    // Stream left side
    Ok(left.map(move |result_probe_batch| -> Result<RecordBatch, DataFusionError> {
        let probe_batch = result_probe_batch?;

        // Hash the probe values
        let probe_keys = evaluate_expressions(&probe_expressions, &probe_batch)?;
        let probe_hashes = calculate_hash(&probe_keys)?;

        // Find matching rows from build side
        let (probe_indices, build_indices) = get_matching_indices(&probe_hashes, &read_only_join_map);

        let output_columns = probe_batch.columns().iter()
            .map(|array| arrow::compute::take(array, &probe_indices, None))
            .chain(build_side_records.columns().iter()
                .map(|array| arrow::compute::take(array, &build_indices, None)))
            .collect::<Result<Vec<ArrayRef>, ArrowError>>()?;
        return Ok(RecordBatch::try_new(join_schema.clone(), output_columns)?);
    }))
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
    let size = input.num_rows();
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

pub fn calculate_hash(values: &Vec<ArrayRef>) -> Result<Vec<u64>, ArrowError> {
    let capacity = values.get(0).map(|array| array.len()).unwrap_or(0);
    let mut probe_hashes = vec![0; capacity];
    datafusion_common::hash_utils::create_hashes(&values, &RandomState::with_seed(0), &mut probe_hashes)?;
    Ok(probe_hashes)
}

pub fn evaluate_expressions(expressions: &Vec<PhysicalExprRef>, batch: &RecordBatch) -> Result<Vec<ArrayRef>, DataFusionError> {
    expressions.iter()
        .map(|expression| expression.evaluate(batch)?.into_array(batch.num_rows()))
        .collect::<datafusion_common::Result<Vec<_>>>()
}

pub fn get_matching_indices(probe_hashes: &Vec<u64>, join_map: &ReadonlyPartitionedConcurrentJoinMap) -> (UInt32Array, UInt32Array) {
    let mut probe_indices = UInt32BufferBuilder::new(0);
    let mut build_indices = UInt32BufferBuilder::new(0);
    for (probe_index, hash) in probe_hashes.iter().enumerate() {
        // Append all the matching build indices
        let mut matching_count = 0;
        for matching_build_index in join_map.get_iter(*hash) {
            build_indices.append(matching_build_index as u32);
            matching_count += 1;
        }
        probe_indices.append_n(matching_count, probe_index as u32);
    }

    let probe_indices: UInt32Array = PrimitiveArray::new(ScalarBuffer::from(probe_indices.finish()), None);
    let build_indices: UInt32Array = PrimitiveArray::new(ScalarBuffer::from(build_indices.finish()), None);
    (probe_indices, build_indices)
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





// ----------------------------------------------------

// pub struct InnerHashLookupDriver {
//     build_side: ReceivedValue<(RecordBatch, ReadOnlyJoinMap<u64>)>,
//     probe_side_input: DriverStreamInput<RecordBatch>,
//     probe_expressions: Vec<PhysicalExprRef>,
//     output_schema: SchemaRef,
//     output: DriverStreamOutput<RecordBatch>,
// }
//
// impl InnerHashLookupDriver {
//     fn as_record_batch_stream(&self) -> impl RecordBatchStream {
//         RecordBatchStreamAdapter::new(self.output_schema.clone(), self.execute_as_stream())
//     }
//
//     fn execute_as_stream(&self) -> impl Stream<Item=Result<RecordBatch, DataFusionError>> {
//
//     }
// }
//
// // pub trait SimpleIntoRecordBatchStream {
// //     fn make_stream(&self) -> impl Stream<Item=Result<RecordBatch, DataFusionError>>;
// //
// //     fn schema(&self) -> SchemaRef;
// // }
// //
// // impl Into<dyn RecordBatchStream> for SimpleIntoRecordBatchStream {
// //
// // }
//
// // impl Stream<Item=Result<RecordBatch, DataFusionError>> for InnerHashLookupDriver {
// //     type Item = Result<RecordBatch, DataFusionError>;
// //
// //     fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
// //         todo!()
// //     }
// // }
//
// impl RecordBatchStream for InnerHashLookupDriver {
//     fn schema(&self) -> SchemaRef {
//         Arc::clone(&self.schema)
//     }
// }
//
// fn process_probe_batch(
//     join_map: &ReadOnlyJoinMap<u64>,
//     build_side_records: &RecordBatch,
//     probe_batch: &RecordBatch,
//     probe_expressions: &Vec<PhysicalExprRef>,
//     output_schema: &SchemaRef,
//     output: &DriverStreamOutput<RecordBatch>,
// ) -> Result<(), DriverError> {
//     // Hash the probe values
//     let probe_keys = evaluate_expressions(probe_expressions, &probe_batch)?;
//     let probe_hashes = calculate_hash(&probe_keys)?;
//
//     // Find matching rows from build side
//     let (probe_indices, build_indices) = get_matching_indices(&probe_hashes, join_map);
//
//     // TODO check for hash collisions
//
//     // Extract the rows matching the indices
//     let output_columns = probe_batch.columns().iter()
//         .map(|array| arrow::compute::take(array, &probe_indices, None))
//         .chain(build_side_records.columns().iter()
//             .map(|array| arrow::compute::take(array, &build_indices, None)))
//         .collect::<Result<Vec<ArrayRef>, ArrowError>>()?;
//     let output_batch = RecordBatch::try_new(output_schema.clone(), output_columns)?;
//     output.send(output_batch)?;
//
//     Ok(())
// }
//
// impl Driver for InnerHashLookupDriver {
//     fn run(&mut self, channel: usize) -> Result<(), DriverError> {
//         // Check the result of the build side
//         let (build_side_records, join_map) = match self.build_side.get() {
//             ReceivedContents::Empty => return Err(DriverError::NothingToProcess),
//             // TODO this should really be treated as a cancelled
//             ReceivedContents::Disconnected => return Err(DriverError::Finished),
//             ReceivedContents::Value(build_side) => build_side
//         };
//
//         match pull_next_input(&self.probe_side_input) {
//             NextInput::Empty => Err(DriverError::NothingToProcess),
//             NextInput::Value(probe_batch) => process_probe_batch(
//                 join_map,
//                 build_side_records,
//                 &probe_batch,
//                 &self.probe_expressions,
//                 &self.output_schema,
//                 &self.output,
//             ),
//             NextInput::Finished => Err(DriverError::Finished)
//         }
//     }
// }
