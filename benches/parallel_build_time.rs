use std::iter::{repeat, repeat_n};
use std::sync::Arc;
use std::time::Duration;

use ahash::RandomState;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use crossbeam::channel::{bounded, Receiver, Sender};
use datafusion::arrow::array::{ArrayRef, UInt32Array};
use datafusion::arrow::compute::{take, take_record_batch};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::SendableRecordBatchStream;
use datafusion_common::DataFusionError;
use datafusion_physical_expr::PhysicalExprRef;
use datafusion_physical_expr_common::expressions::column::col;
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use futures::stream::iter;
use futures::StreamExt;
use hashbrown::raw::{Bucket, InsertSlot, RawTable};
use rand::Rng;
use tokio::runtime::Builder;
use tokio::task::JoinSet;

use datafusion_parallelism::api_utils::{make_int_array, make_string_array};
use datafusion_parallelism::operator::build_implementation::BuildVersion;
use datafusion_parallelism::operator::version10::build_implementation::Version10;

fn make_config() -> Criterion {
    Criterion::default()
        .warm_up_time(Duration::from_secs(5))
        .measurement_time(Duration::from_secs(15))
}

const PARALLELISM: usize = 8;
const BATCHES_PER_PARTITION: usize = 64;
const BATCH_SIZE: usize = 8192;

fn create_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("col1", DataType::Int32, false),
        Field::new("col2", DataType::Int32, false),
        Field::new("col3", DataType::Utf8, false),
    ]))
}

fn generate_random_batches(schema: SchemaRef, num_batches: usize, batch_size: usize) -> Vec<RecordBatch> {
    let mut rng = rand::thread_rng();

    (0..num_batches)
        .map(|_| {
            let col1_data: Vec<i32> = (0..batch_size).map(|_| rng.gen_range(0..1000000)).collect();
            let col2_data: Vec<i32> = (0..batch_size).map(|_| rng.gen_range(0..1000000)).collect();
            let col3_data: Vec<String> = (0..batch_size)
                .map(|_| format!("value_{}", rng.gen_range(0..1000)))
                .collect();

            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(make_int_array(col1_data)),
                    Arc::new(make_int_array(col2_data)),
                    Arc::new(make_string_array(col3_data)),
                ],
            ).unwrap()
        })
        .collect()
}

// Helper functions for hash calculation
fn calculate_hash(values: &Vec<ArrayRef>) -> Result<Vec<u64>, ArrowError> {
    let capacity = values.get(0).map(|array| array.len()).unwrap_or(0);
    let mut probe_hashes = vec![0; capacity];
    datafusion_common::hash_utils::create_hashes(&values, &RandomState::with_seed(0), &mut probe_hashes)?;
    Ok(probe_hashes)
}

fn evaluate_expressions(expressions: &Vec<PhysicalExprRef>, batch: &RecordBatch) -> Result<Vec<ArrayRef>, DataFusionError> {
    expressions.iter()
        .map(|expression| expression.evaluate(batch)?.into_array(batch.num_rows()))
        .collect::<datafusion_common::Result<Vec<_>>>()
}

fn criterion_benchmark(c: &mut Criterion) {
    let rt = Builder::new_multi_thread()
        .enable_all()
        .worker_threads(PARALLELISM)
        .build()
        .unwrap();

    let schema = create_schema();

    // Generate random data ahead of time for all partitions
    let all_partition_data: Vec<Vec<RecordBatch>> = (0..PARALLELISM)
        .map(|_| generate_random_batches(schema.clone(), BATCHES_PER_PARTITION, BATCH_SIZE))
        .collect();

    // Create physical expression for the build key (using col1)
    let build_expressions = vec![col("col1", &schema).unwrap()];

    let mut group = c.benchmark_group("Version10BuildTime");

    group.bench_function(BenchmarkId::new("version10", PARALLELISM), |bencher| {
        bencher.to_async(&rt).iter(|| {
            let all_partition_data = all_partition_data.clone();
            let schema = schema.clone();
            let build_expressions = build_expressions.clone();

            build_version10(all_partition_data, schema, build_expressions)
        });
    });

    group.bench_function(BenchmarkId::new("rawmap", PARALLELISM), |bencher| {
        bencher.to_async(&rt).iter(|| {
            let all_partition_data = all_partition_data.clone();
            let schema = schema.clone();
            let build_expressions = build_expressions.clone();

            build_rawmap(all_partition_data, schema, build_expressions)
        });
    });

    group.finish();
}

async fn build_version10(
    all_partition_data: Vec<Vec<RecordBatch>>,
    schema: SchemaRef,
    build_expressions: Vec<PhysicalExprRef>,
) {
    // Create Version10 instance with parallelism = 8
    let version10 = Arc::new(Version10::new(PARALLELISM, schema.clone()));

    // Start 8 parallel tokio tasks
    let mut join_set = JoinSet::new();

    for partition_id in 0..PARALLELISM {
        let version10 = Arc::clone(&version10);
        let partition_data = all_partition_data[partition_id].clone();
        let schema = schema.clone();
        let build_expressions = build_expressions.clone();

        join_set.spawn(async move {
            // Create stream from pre-generated batches
            let stream: SendableRecordBatchStream = Box::pin(
                RecordBatchStreamAdapter::new(
                    schema,
                    iter(partition_data).map(|batch| Ok(batch)),
                )
            );

            // Call build_lookup_map
            version10.build_lookup_map(partition_id, stream, &build_expressions)
                .await
                .unwrap()
        });
    }

    // Wait for all tasks to complete
    while let Some(result) = join_set.join_next().await {
        let _ = result.unwrap();
    }
}

async fn build_rawmap(
    all_partition_data: Vec<Vec<RecordBatch>>,
    schema: SchemaRef,
    build_expressions: Vec<PhysicalExprRef>,
) {
    // Create channels for each partition
    let channel_capacity = 10;

    let (senders, receivers): (Vec<tokio::sync::mpsc::Sender<(RecordBatch, Vec<u64>)>>, Vec<tokio::sync::mpsc::Receiver<(RecordBatch, Vec<u64>)>>) = (0..PARALLELISM).into_iter()
        .map(|_| tokio::sync::mpsc::channel(channel_capacity))
        .unzip();
    let shared_senders = vec![senders; PARALLELISM];

    // Start 8 parallel tokio tasks for repartitioning
    let mut repartition_set = JoinSet::new();
    // for (sender, senders.into_iter().zip(all_partition_data.clone().into_iter())
    for (partition_data, senders) in all_partition_data.iter().zip(shared_senders.into_iter()) {
        let partition_data = partition_data.clone();
        let build_expressions = build_expressions.clone();

        repartition_set.spawn(async move {
            repartition_stream(partition_data, &senders, &build_expressions, PARALLELISM).await;
        });
    }

    // Start 8 parallel tokio tasks for building hashmaps
    let mut build_set = JoinSet::new();

    for receiver in receivers {
        let schema = schema.clone();
        build_set.spawn(async move {
            build_hashmap_from_channel(receiver, schema).await
        });
    }

    // Wait for all tasks to complete
    while let Some(result) = repartition_set.join_next().await {
        result.unwrap();
    }

    while let Some(result) = build_set.join_next().await {
        let _ = result.unwrap();
    }
}

async fn repartition_stream(
    record_batches: Vec<RecordBatch>,
    senders: &[tokio::sync::mpsc::Sender<(RecordBatch, Vec<u64>)>],
    build_expressions: &Vec<PhysicalExprRef>,
    parallelism: usize,
) {
    for batch in record_batches {
        let keys = evaluate_expressions(&build_expressions, &batch).unwrap();
        let hashes = calculate_hash(&keys).unwrap();

        // Group rows by partition
        let mut partition_indices: Vec<Vec<u32>> = (0..parallelism)
            .map(|_| Vec::new())
            .collect();

        for (row_idx, &hash) in hashes.iter().enumerate() {
            let bits = hash >> 32;
            let target_partition = (bits % parallelism as u64) as usize;
            partition_indices[target_partition].push(row_idx as u32);
        }

        // Send partitioned batches
        for (target_partition, indices) in partition_indices.iter().enumerate() {
            if !indices.is_empty() {
                // Use take to select rows for this partition
                let indices_array = UInt32Array::from(indices.clone());
                let partitioned_batch = take_record_batch(&batch, &indices_array).unwrap();

                // Extract hashes for this partition
                let partition_hashes: Vec<u64> = indices
                    .iter()
                    .map(|&idx| hashes[idx as usize])
                    .collect();

                senders[target_partition]
                    .send((partitioned_batch, partition_hashes))
                    .await
                    .unwrap();
            }
        }
    }
}

async fn build_hashmap_from_channel(
    mut receiver: tokio::sync::mpsc::Receiver<(RecordBatch, Vec<u64>)>,
    schema: SchemaRef,
) -> (RawTable<(u64, usize)>, Vec<usize>, RecordBatch) {
    let mut record_batches = vec![];
    let mut overflow_buffer = vec![0];
    let mut hash_map = RawTable::new();
    let mut row_offset = 0;

    while let Some((batch, hashes)) = receiver.recv().await {
        overflow_buffer.reserve(batch.num_rows());
        overflow_buffer.extend(repeat_n(0, batch.num_rows()));

        for (local_row_idx, &hash) in hashes.iter().enumerate() {
            let value = row_offset + local_row_idx + 1;
            match hash_map.find_or_find_insert_slot(
                hash,
                |(existing, _)| existing == &hash,
                |(existing, _)| *existing,
            ) {
                Ok(bucket) => {
                    let existing = std::mem::replace(&mut unsafe { bucket.as_mut() }.1, value);
                    overflow_buffer[value] = existing;
                }
                Err(insert_slot) => {
                    unsafe {
                        hash_map.insert_in_slot(hash, insert_slot, (hash, value));
                    }
                }
            }
        }
        row_offset += batch.num_rows();

        record_batches.push(batch);
    }

    let record_batch = datafusion::arrow::compute::concat_batches(
        &schema,
        &record_batches,
    ).unwrap();

    (hash_map, overflow_buffer, record_batch)
}

criterion_main!(benches);
criterion_group! {
    name = benches;
    config = make_config();
    targets = criterion_benchmark
}
