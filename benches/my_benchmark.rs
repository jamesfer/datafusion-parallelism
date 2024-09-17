use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;

use criterion::{BatchSize, BenchmarkId, Criterion, criterion_group, criterion_main};
use criterion::async_executor::AsyncExecutor;
use datafusion::arrow::array::AsArray;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::{CatalogProvider, SchemaProvider};
use datafusion::datasource::TableProvider;
use datafusion::execution::context::SessionState;
use datafusion_physical_plan::{DisplayAs, ExecutionPlan};
use datafusion_physical_plan::streaming::PartitionStream;
use futures_core::future::BoxFuture;
use rand::distributions::Alphanumeric;
use rand::Rng;
use tokio::runtime::{Builder, Runtime};

use datafusion_parallelism::api_utils::{make_int_array, make_string_array, make_string_constant_array};
use datafusion_parallelism::parse_sql::{JoinReplacement, make_session_state, make_session_state_with_target_partitions};

use crate::utils::prepare_query::prepare_query;
use crate::utils::register_tables::register_tables;

mod utils;

// Number of power cores on an Apple M1 max
const PARALLELISM: usize = 8;

fn make_config() -> Criterion {
    Criterion::default()
        .warm_up_time(Duration::from_secs(30))
        .measurement_time(Duration::from_secs(300))
        .sample_size(50)
}

fn criterion_benchmark(c: &mut Criterion) {
    let rt = Builder::new_multi_thread()
        .worker_threads(PARALLELISM)
        .build()
        .unwrap();

    let tests: Vec<Box<dyn BenchmarkQuery>> = vec![
        // Box::new(AllEqualSize),
        // Box::new(MuchLargerProbeSize),
        Box::new(TinyProbeSize),
    ];

    let sessions = vec![
        ("control", make_session_state_with_target_partitions(None, Some(PARALLELISM))),
        // ("version1", make_session_state_with_target_partitions(Some(JoinReplacement::Original), Some(PARALLELISM))),
        // ("version2", make_session_state(Some(JoinReplacement::New))),
        // ("version3", make_session_state(Some(JoinReplacement::New3))),
        // ("version4", make_session_state(Some(JoinReplacement::New4))),
        // ("version5", make_session_state(Some(JoinReplacement::New5))),
        ("version6", make_session_state_with_target_partitions(Some(JoinReplacement::New6), Some(PARALLELISM))),
    ];

    for test in tests {
        let mut group = c.benchmark_group(test.name());

        for (name, session) in sessions.iter() {
            let mut operation = rt.block_on(test.run(&session));
            group.bench_function(BenchmarkId::new(*name, ""), |bencher| {
                bencher.to_async(&rt).iter(|| operation());
                // bencher.to_async(&rt).iter_batched(|| 1, |_| operation(), BatchSize::LargeInput);
            });
        }
    }
}

criterion_main!(benches);
criterion_group! {
    name = benches;
    config = make_config();
    targets = criterion_benchmark
}


fn base_table_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id1", DataType::Int32, false),
        Field::new("id2", DataType::Int32, false),
        Field::new("id3", DataType::Int32, false),
        Field::new("id4", DataType::Int32, false),
        Field::new("value", DataType::Utf8, false),
    ]))
}

fn small_table_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("value", DataType::Utf8, false),
    ]))
}

fn random_string(length: usize) -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(length)
        .map(char::from)
        .collect()
}



// const FOUR_TABLE_SQL: &str = "SELECT * \
//     FROM my_catalog.my_schema.base_table \
//     JOIN my_catalog.my_schema.small_table_1 \
//       ON base_table.id1 = small_table_1.id \
//     JOIN my_catalog.my_schema.small_table_2 \
//       ON base_table.id2 = small_table_2.id \
//     JOIN my_catalog.my_schema.small_table_3 \
//       ON base_table.id3 = small_table_3.id \
//     JOIN my_catalog.my_schema.small_table_4 \
//       ON base_table.id4 = small_table_4.id";

const FOUR_TABLE_SQL: &str = "SELECT result.id1, result.id2, result.id3, result.id4 \
  FROM my_catalog.my_schema.small_table_4 \
  JOIN ( \
    SELECT result.id1, result.id2, result.id3, result.id4 \
    FROM my_catalog.my_schema.small_table_3 \
    JOIN ( \
      SELECT result.id1, result.id2, result.id3, result.id4 \
      FROM my_catalog.my_schema.small_table_2 \
      JOIN ( \
        SELECT base_table.id1, base_table.id2, base_table.id3, base_table.id4 \
        FROM my_catalog.my_schema.small_table_1 \
        JOIN my_catalog.my_schema.base_table \
        ON base_table.id1 = small_table_1.id \
      ) as result \
      ON result.id2 = small_table_2.id \
    ) as result \
    ON result.id3 = small_table_3.id \
  ) as result \
  ON result.id4 = small_table_4.id";


// Trial queries

trait BenchmarkQuery {
    fn name(&self) -> &str;

    fn run<'a>(&self, session_state: &'a SessionState) -> BoxFuture<'a, Box<dyn FnMut() -> BoxFuture<'a, Vec<RecordBatch>> + 'a>>;
}

struct AllEqualSize;

impl BenchmarkQuery for AllEqualSize {
    fn name(&self) -> &str {
        "AllEqualSize"
    }

    fn run<'a>(&self, session_state: &'a SessionState) -> BoxFuture<'a, Box<dyn FnMut() -> BoxFuture<'a, Vec<RecordBatch>> + 'a>> {
        Box::pin(async move {
            let base_data =
                (0..100).into_iter()
                    .map(|i| {
                        RecordBatch::try_new(
                            base_table_schema(),
                            vec![
                                Arc::new(make_int_array(i * 1024, (i + 1) * 1024, 1)),
                                Arc::new(make_int_array(i * 1024, (i + 1) * 1024, 2)),
                                Arc::new(make_int_array(i * 1024, (i + 1) * 1024, 3)),
                                Arc::new(make_int_array(i * 1024, (i + 1) * 1024, 4)),
                                Arc::new(make_string_constant_array("hello".to_string(), 1024)),
                            ],
                        ).unwrap()
                    })
                    .collect::<Vec<_>>();

            let join_tables = (1..5)
                .map(|table_number| {
                    let data =
                        (0..10).into_iter()
                            .map(|i| {
                                RecordBatch::try_new(
                                    small_table_schema(),
                                    vec![
                                        Arc::new(make_int_array(i * 1024, (i + 1) * 1024, table_number)),
                                        Arc::new(make_string_constant_array("world".to_string(), 1024)),
                                    ],
                                ).unwrap()
                            })
                            .collect::<Vec<_>>();
                    (format!("small_table_{table_number}"), small_table_schema(), data)
                })
                .collect::<Vec<_>>();

            register_tables(
                session_state,
                vec![("base_table".to_string(), base_table_schema(), base_data)]
                    .into_iter()
                    .chain(join_tables.into_iter())
                    .collect::<Vec<_>>(),
            );

            // Plan query
            prepare_query(session_state, FOUR_TABLE_SQL).await
        })
    }
}

struct MuchLargerProbeSize;

impl BenchmarkQuery for MuchLargerProbeSize {
    fn name(&self) -> &str {
        "MuchLargerProbeSize"
    }

    fn run<'a>(&self, session_state: &'a SessionState) -> BoxFuture<'a, Box<dyn FnMut() -> BoxFuture<'a, Vec<RecordBatch>> + 'a>> {
        Box::pin(async move {
            let base_data =
                (0..10000).into_iter()
                    .map(|i| {
                        let i = i % 100;
                        RecordBatch::try_new(
                            base_table_schema(),
                            vec![
                                Arc::new(make_int_array(i * 1024, (i + 1) * 1024, 1)),
                                Arc::new(make_int_array(i * 1024, (i + 1) * 1024, 2)),
                                Arc::new(make_int_array(i * 1024, (i + 1) * 1024, 3)),
                                Arc::new(make_int_array(i * 1024, (i + 1) * 1024, 4)),
                                Arc::new(make_string_constant_array("hello".to_string(), 1024)),
                            ],
                        ).unwrap()
                    })
                    .collect::<Vec<_>>();

            let join_tables = (1..5)
                .map(|table_number| {
                    let data =
                        (0..100).into_iter()
                            .map(|i| {
                                RecordBatch::try_new(
                                    small_table_schema(),
                                    vec![
                                        Arc::new(make_int_array(i * 1024, (i + 1) * 1024, table_number)),
                                        Arc::new(make_string_constant_array("world".to_string(), 1024)),
                                    ],
                                ).unwrap()
                            })
                            .collect::<Vec<_>>();
                    (format!("small_table_{table_number}"), small_table_schema(), data)
                })
                .collect::<Vec<_>>();

            register_tables(
                session_state,
                vec![("base_table".to_string(), base_table_schema(), base_data)]
                    .into_iter()
                    .chain(join_tables.into_iter())
                    .collect::<Vec<_>>(),
            );

            // Plan query
            prepare_query(session_state, FOUR_TABLE_SQL).await
        })
    }
}

struct TinyProbeSize;

impl BenchmarkQuery for TinyProbeSize {
    fn name(&self) -> &str {
        "TinyProbeSide"
    }

    fn run<'a>(&self, session_state: &'a SessionState) -> BoxFuture<'a, Box<dyn FnMut() -> BoxFuture<'a, Vec<RecordBatch>> + 'a>> {
        let batch_size = 1024;
        Box::pin(async move {
            // The main table has 5000 * 1024 rows, using 256 unique batches of ids to match the
            // sizes of the small tables
            let base_data =
                (0..10_000).into_iter()
                    .map(|i| {
                        let i = i % 256;
                        RecordBatch::try_new(
                            base_table_schema(),
                            vec![
                                // Each id is offset by 1 so that they are different to each other
                                Arc::new(make_int_array(i * batch_size, (i + 1) * batch_size, 0)),
                                Arc::new(make_int_array(i * batch_size, (i + 1) * batch_size, 1)),
                                Arc::new(make_int_array(i * batch_size, (i + 1) * batch_size, 2)),
                                Arc::new(make_int_array(i * batch_size, (i + 1) * batch_size, 3)),
                                Arc::new(make_string_constant_array("hello".to_string(), 1024)),
                                // Arc::new(make_string_array(std::iter::repeat_with(|| random_string(32)).take(batch_size as usize))),
                            ],
                        ).unwrap()
                    })
                    .collect::<Vec<_>>();

            // The 4 main tables have 256 * batch_size rows (as they should be above the maximum threshold
            // for broadcast joins)
            let join_tables = (1..5)
                .map(|table_number| {
                    let data =
                        (0..256).into_iter()
                            .map(|i| {
                                RecordBatch::try_new(
                                    small_table_schema(),
                                    vec![
                                        Arc::new(make_int_array(i * batch_size, (i + 1) * batch_size, table_number)),
                                        Arc::new(make_string_array(std::iter::repeat_with(|| random_string(32)).take(batch_size as usize))),
                                    ],
                                ).unwrap()
                            })
                            .collect::<Vec<_>>();
                    (format!("small_table_{table_number}"), small_table_schema(), data)
                })
                .collect::<Vec<_>>();

            register_tables(
                session_state,
                vec![("base_table".to_string(), base_table_schema(), base_data)]
                    .into_iter()
                    .chain(join_tables.into_iter())
                    .collect::<Vec<_>>(),
            );

            // Plan query
            prepare_query(session_state, FOUR_TABLE_SQL).await
        })
    }
}
