use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use criterion::async_executor::AsyncExecutor;
use criterion::profiler::Profiler;
use datafusion::arrow::array::{Int32Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::{CatalogProvider, MemoryCatalogProvider, MemorySchemaProvider, SchemaProvider};
use datafusion::datasource::streaming::StreamingTable;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::execution::context::SessionState;
use datafusion_common::DataFusionError;
use datafusion_physical_plan::collect;
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_physical_plan::streaming::PartitionStream;
use futures_core::future::BoxFuture;
use tokio::runtime::Runtime;
use datafusion_parallelism::api_utils::{make_int_array, make_string_constant_array};

use datafusion_parallelism::parse_sql::{JoinReplacement, make_session_state};

// use cpuprofiler::PROFILER;

struct MyProfiler {
    // guard: Option<ProfilerGuard<'static>>
}

impl MyProfiler {
    fn new() -> Self {
        MyProfiler {
            // guard: None,
        }
    }
}

impl Profiler for MyProfiler {
    fn start_profiling(&mut self, benchmark_id: &str, benchmark_dir: &Path) {
        // PROFILER.lock().unwrap().start(format!("./{benchmark_id}.profile")).unwrap();
        // self.guard = Some(pprof::ProfilerGuardBuilder::default()
        //     .frequency(1000)
        //     .blocklist(&["libc", "libgcc", "pthread", "vdso"])
        //     .build()
        //     .unwrap());
    }

    fn stop_profiling(&mut self, benchmark_id: &str, benchmark_dir: &Path) {
        // PROFILER.lock().unwrap().stop().unwrap();
        // if let Some(g) = &self.guard {
        //     if let Ok(report) = g.report().build() {
        //         println!("report: {:?}", &report);
        //     };
        // }
        // self.guard = None
    }
}

fn make_config() -> Criterion {
    Criterion::default()
        .warm_up_time(Duration::from_secs(10))
        .measurement_time(Duration::from_secs(30))
        // .with_profiler(PProfProfiler::new(100, Output::Flamegraph(None)))
        .with_profiler(MyProfiler::new())
}

fn criterion_benchmark(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let tests: Vec<Box<dyn BenchmarkQuery>> = vec![
        Box::new(AllEqualSize),
        Box::new(MuchLargerProbeSize),
    ];

    let sessions = vec![
        ("control", make_session_state(None)),
        ("version1", make_session_state(Some(JoinReplacement::Original))),
        ("version2", make_session_state(Some(JoinReplacement::New))),
        ("version3", make_session_state(Some(JoinReplacement::New3))),
    ];

    for test in tests {
        let mut group = c.benchmark_group(test.name());

        for (name, session) in sessions.iter() {
            group.bench_function(BenchmarkId::new(*name, ""), |bencher| {
                let operation = test.run(&session);
                let mut operation = rt.block_on(operation);
                bencher.to_async(&rt).iter(|| {
                    operation()
                });
            });
        }
    }
}

criterion_main!(benches);
criterion_group! {
    name = benches;
    // This can be any expression that returns a `Criterion` object.
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

struct StaticPartitionStream {
    schema: SchemaRef,
    data: Vec<RecordBatch>,
}

impl PartitionStream for StaticPartitionStream {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let batches = self.data.clone().into_iter()
            .map(|batch| -> Result<RecordBatch, DataFusionError> { Ok(batch) })
            .collect::<Vec<_>>();
        Box::pin(RecordBatchStreamAdapter::new(self.schema.clone(), futures::stream::iter(batches)))
    }
}


const FOUR_TABLE_SQL: &str = "SELECT * \
    FROM my_catalog.my_schema.base_table \
    JOIN my_catalog.my_schema.small_table_1 \
      ON base_table.id1 = small_table_1.id \
    JOIN my_catalog.my_schema.small_table_2 \
      ON base_table.id2 = small_table_2.id \
    JOIN my_catalog.my_schema.small_table_3 \
      ON base_table.id3 = small_table_3.id \
    JOIN my_catalog.my_schema.small_table_4 \
      ON base_table.id4 = small_table_4.id";


// Trial queries

trait BenchmarkQuery {
    fn name(&self) -> &str;

    fn run<'a>(&self, session_state: &'a SessionState) -> BoxFuture<'a, Box<dyn FnMut() -> BoxFuture<'a, ()> + 'a>>;
}

struct AllEqualSize;

impl BenchmarkQuery for AllEqualSize {
    fn name(&self) -> &str {
        "AllEqualSize"
    }

    fn run<'a>(&self, session_state: &'a SessionState) -> BoxFuture<'a, Box<dyn FnMut() -> BoxFuture<'a, ()> + 'a>> {
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

struct MuchLargerProbeSize;

impl BenchmarkQuery for MuchLargerProbeSize {
    fn name(&self) -> &str {
        "MuchLargerProbeSize"
    }

    fn run<'a>(&self, session_state: &'a SessionState) -> BoxFuture<'a, Box<dyn FnMut() -> BoxFuture<'a, ()> + 'a>> {
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

async fn prepare_query(session_state: &SessionState, sql: &str) -> Box<fn() -> BoxFuture<()>> {
    let logical_plan = session_state.create_logical_plan(sql).await.unwrap();

    Box::new(move || {
        // Clone the plan, so we only capture a reference to it
        let logical_plan = logical_plan.clone();
        Box::pin(async move {
            let physical_plan = session_state.create_physical_plan(&logical_plan).await.unwrap();

            collect(physical_plan, Arc::new(TaskContext::default()))
                .await
                .unwrap();
            ()
        })
    })
}

fn register_tables(session_state: &SessionState, tables: Vec<(String, SchemaRef, Vec<RecordBatch>)>) {
    // Register all tables
    let schema_provider = Arc::new(MemorySchemaProvider::new());
    for (name, schema, data) in tables {
        let data_stream = StaticPartitionStream { schema: schema.clone(), data };
        let table = StreamingTable::try_new(schema.clone(), vec![Arc::new(data_stream)]).unwrap();
        schema_provider.register_table(name.to_string(), Arc::new(table)).unwrap();
    }

    let catalog = Arc::new(MemoryCatalogProvider::new());
    catalog.register_schema("my_schema", schema_provider)
        .map_err(|err| format!("register schema error: {}", err))
        .unwrap();
    session_state.catalog_list().register_catalog("my_catalog".to_string(), catalog);
}
