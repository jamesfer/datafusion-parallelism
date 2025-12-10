mod utils;

use std::future::Future;
use std::panic;
use std::sync::Arc;
use std::time::Duration;
use async_trait::async_trait;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use criterion::async_executor::AsyncExecutor;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::logical_expr::col;
use datafusion_common::DFSchema;
use datafusion_physical_expr::create_physical_expr;
use datafusion_physical_expr::execution_props::ExecutionProps;
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use futures::stream::iter;
use futures::stream::StreamExt;
use futures_core::future::BoxFuture;
use tokio::runtime::Builder;
use tokio::task::JoinSet;

use datafusion_parallelism::api_utils::{make_int_array_with_shift, make_string_constant_array};
use datafusion_parallelism::operator::build_implementation::{BuildImplementation};
use datafusion_parallelism::operator::lookup_consumers::IndexLookupConsumer;
use datafusion_parallelism::parse_sql::JoinReplacement;
use datafusion_parallelism::utils::index_lookup::IndexLookup;

fn make_config() -> Criterion {
    Criterion::default()
        .warm_up_time(Duration::from_secs(5))
        .measurement_time(Duration::from_secs(15))
}

// Number of power cores on an Apple M1 max
const PARALLELISM: usize = 8;

type BuildImplementationSession = Box<dyn Fn(SchemaRef) -> BuildImplementation + Sync>;

fn criterion_benchmark(c: &mut Criterion) {
    let rt = Builder::new_multi_thread()
        .enable_all()
        .worker_threads(PARALLELISM)
        .build()
        .unwrap();

    let scenarios: Vec<Box<dyn BenchmarkQuery>> = vec![
        Box::new(Size512),
    ];

    let sessions: Vec<(&str, BuildImplementationSession)> = vec![
        ("version1", Box::new(|schema| BuildImplementation::new(JoinReplacement::Original, PARALLELISM, schema))),
        ("version2", Box::new(|schema| BuildImplementation::new(JoinReplacement::New, PARALLELISM, schema))),
        ("version3", Box::new(|schema| BuildImplementation::new(JoinReplacement::New3, PARALLELISM, schema))),
        // ("version4", Box::new(|schema| BuildImplementation::new(JoinReplacement::New4, PARALLELISM, schema))),
        // ("version5", Box::new(|schema| BuildImplementation::new(JoinReplacement::New5, PARALLELISM, schema))),
        // ("version6", Box::new(|schema| BuildImplementation::new(JoinReplacement::New6, PARALLELISM, schema))),
        // ("version7", Box::new(|schema| BuildImplementation::new(JoinReplacement::New7, PARALLELISM, schema))),
        // ("version7", Box::new(|schema| BuildImplementation::new(JoinReplacement::New7, PARALLELISM, schema))),
        ("version8", Box::new(|schema| BuildImplementation::new(JoinReplacement::New8, PARALLELISM, schema))),
        ("version10", Box::new(|schema| BuildImplementation::new(JoinReplacement::New10, PARALLELISM, schema))),
    ];

    for scenario in scenarios {
        let mut group = c.benchmark_group(format!("BuildSpeed/{}", scenario.name()));

        for (name, session) in sessions.iter() {
            group.bench_function(BenchmarkId::new(*name, ""), |bencher| {
                let mut operation = rt.block_on(scenario.setup_async(&session));
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


fn small_table_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("value", DataType::Utf8, false),
    ]))
}


struct NoopConsumer;

impl IndexLookupConsumer for NoopConsumer {
    type R = ();

    fn call<Lookup>(
        self,
        _: Lookup,
        _: RecordBatch,
    ) -> Self::R
        where Lookup: IndexLookup<u64> + Sync + Send + 'static
    {
        ()
    }
}

// Trial queries

#[async_trait]
trait BenchmarkQuery {
    fn name(&self) -> &str;

    async fn setup_async<'a>(&self, session_state: &'a BuildImplementationSession) -> Box<dyn FnMut() -> BoxFuture<'a, ()> + 'a>;
}

fn operation_func<'a, Fn, Fut>(mut func: Fn) -> Box<dyn FnMut() -> BoxFuture<'a, ()> + 'a>
    where
        Fn: FnMut() -> Fut + 'a,
        Fut: Future<Output=()> + 'a + Send
{
    Box::new(move || Box::pin(func()))
}


struct Size512;

#[async_trait]
impl BenchmarkQuery for Size512 {
    fn name(&self) -> &str {
        "Size512"
    }

    async fn setup_async<'a>(&self, implementation: &'a BuildImplementationSession) -> Box<dyn FnMut() -> BoxFuture<'a, ()> + 'a> {
        let batches = 512;
        let batches_per_parallelism = (batches / PARALLELISM) as i32;
        let batch_size = 8192;
        let schema = small_table_schema();
        let join_tables = (0..PARALLELISM).into_iter()
            .map(|parallelism| {
                let data = (0..batches_per_parallelism).into_iter()
                    .map(|i| {
                        RecordBatch::try_new(
                            Arc::clone(&schema),
                            vec![
                                Arc::new(make_int_array_with_shift(i * batch_size, (i + 1) * batch_size, 0)),
                                Arc::new(make_string_constant_array("world".to_string(), batch_size)),
                            ],
                        ).unwrap()
                    })
                    .collect::<Vec<_>>();
                (Arc::clone(&schema), data)
            })
            .collect::<Vec<_>>();
        let expr = col("id");
        let df_schema = DFSchema::try_from(schema.clone()).unwrap();
        let props = ExecutionProps::new();
        let physical_expr = vec![create_physical_expr(&expr, &df_schema, &props).unwrap()];

        operation_func(move || {
            // Create a clone of the variables that we would like to take ownership of inside the
            // async block. This is because the mutable function only has access to the variables
            // while it is executing, but since we use them in the async block, we need an exclusive
            // copy that we will use for this future, while the original will continue to be owned
            // by the function.
            let physical_expr = physical_expr.clone();
            let join_tables = join_tables.clone();
            let schema = schema.clone();

            async move {
                // Create an implementation for all threads to use
                let implementation = Arc::new(implementation(schema.clone()));

                let mut join_set = JoinSet::new();
                for i in 0..PARALLELISM {
                    let (schema, data) = join_tables[i].clone();
                    let stream: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
                        schema,
                        iter(data).map(|record_batch| Ok(record_batch)),
                    ));

                    // Copy the variables used inside the future, so they can live long enough
                    let implementation = Arc::clone(&implementation);
                    let physical_expr = physical_expr.clone();
                    join_set.spawn(async move {
                        implementation.build_side(
                            i,
                            stream,
                            &physical_expr,
                            NoopConsumer,
                        ).await.unwrap();
                    });
                }

                // Wait for all tasks
                while let Some(res) = join_set.join_next().await {
                    match res {
                        Ok(_) => {},
                        Err(err) if err.is_panic() => panic::resume_unwind(err.into_panic()),
                        Err(err) => panic!("{err}"),
                    }
                }

                ()
            }
        })
    }
}
