use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use criterion::{Criterion, criterion_group, criterion_main};
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
use datafusion_physical_plan::{collect, ExecutionPlan};
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_physical_plan::streaming::PartitionStream;
use pprof::ProfilerGuard;
use tokio::runtime::Runtime;

use datafusion_parallelism::parse_sql::{make_session_state, parse_sql};

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
        .warm_up_time(Duration::from_secs(20))
        .measurement_time(Duration::from_secs(60))
        // .with_profiler(PProfProfiler::new(100, Output::Flamegraph(None)))
        .with_profiler(MyProfiler::new())
}

fn criterion_benchmark(c: &mut Criterion) {
    // Create the session
    let session_state = make_session_state(false);
    register_tables(&session_state);


    c.bench_function("without_new_hash_operator", |b| {
        let rt = Runtime::new().unwrap();
        b.to_async(rt).iter(|| {
            run_plan(&session_state)
        });
    });

    // Create the session
    let session_state = make_session_state(true);
    register_tables(&session_state);
    c.bench_function("with_new_hash_operator", |b| {
        let rt = Runtime::new().unwrap();
        b.to_async(rt).iter(|| {
            run_plan(&session_state)
        });
    });
}

criterion_main!(benches);
criterion_group! {
    name = benches;
    // This can be any expression that returns a `Criterion` object.
    config = make_config();
    targets = criterion_benchmark
}

async fn run_plan(session_state: &SessionState) {
    let plan = parse_sql(
        "SELECT * \
            FROM my_catalog.my_schema.base_table \
            JOIN my_catalog.my_schema.small_table_1 \
              ON base_table.id1 = small_table_1.id \
            JOIN my_catalog.my_schema.small_table_2 \
              ON base_table.id2 = small_table_2.id \
            JOIN my_catalog.my_schema.small_table_3 \
              ON base_table.id3 = small_table_3.id \
            JOIN my_catalog.my_schema.small_table_4 \
              ON base_table.id4 = small_table_4.id",
        &session_state,
    ).await.unwrap();

    collect(plan, Arc::new(TaskContext::default()))
        .await
        .unwrap();
}

fn register_tables(session_state: &SessionState) {
    // Schemas
    let base_table_schema = Arc::new(Schema::new(vec![
        Field::new("id1", DataType::Int32, false),
        Field::new("id2", DataType::Int32, false),
        Field::new("id3", DataType::Int32, false),
        Field::new("id4", DataType::Int32, false),
        Field::new("value", DataType::Utf8, false),
    ]));
    let small_table_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("value", DataType::Utf8, false),
    ]));

    let schema = Arc::new(MemorySchemaProvider::new());

    // Data
    let base_record_batches =
        (0..1000).into_iter()
            .map(|i| {
                RecordBatch::try_new(
                    base_table_schema.clone(),
                    vec![
                        Arc::new(make_int_array(i * 1024, (i + 1) * 1024)),
                        Arc::new(make_int_array(i * 1024, (i + 1) * 1024)),
                        Arc::new(make_int_array(i * 1024, (i + 1) * 1024)),
                        Arc::new(make_int_array(i * 1024, (i + 1) * 1024)),
                        Arc::new(make_string_array("hello".to_string(), 1024)),
                    ],
                ).unwrap()
            })
            .collect::<Vec<_>>();
    let base_data = StaticPartitionStream {
        schema: base_table_schema.clone(),
        data: base_record_batches,
    };
    let base_table = StreamingTable::try_new(base_table_schema, vec![Arc::new(base_data)])
        .unwrap();
    schema.register_table("base_table".to_string(), Arc::new(base_table)).unwrap();

    for i in 1..5 {
        let small_record_batches =
            (0..1000).into_iter()
                .map(|i| {
                    RecordBatch::try_new(
                        small_table_schema.clone(),
                        vec![
                            Arc::new(make_int_array(i * 1024, (i + 1) * 1024)),
                            Arc::new(make_string_array("world".to_string(), 1024)),
                        ],
                    ).unwrap()
                })
                .collect::<Vec<_>>();
        let small_data = StaticPartitionStream {
            schema: small_table_schema.clone(),
            data: small_record_batches,
        };
        let small_table = StreamingTable::try_new(small_table_schema.clone(), vec![Arc::new(small_data)])
            .unwrap();
        schema.register_table(format!("small_table_{}", i), Arc::new(small_table)).unwrap();
    }

    // Register catalog
    let catalog = Arc::new(MemoryCatalogProvider::new());
    catalog.register_schema("my_schema", schema)
        .map_err(|err| format!("register schema error: {}", err))
        .unwrap();
    session_state.catalog_list().register_catalog("my_catalog".to_string(), catalog);
}

fn make_int_array(min: i32, max: i32) -> Int32Array {
    Int32Array::from((min..max).collect::<Vec<_>>())
}

fn make_string_array(value: String, count: i32) -> StringArray {
    StringArray::from((0..count).map(|_| value.clone()).collect::<Vec<_>>())
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
