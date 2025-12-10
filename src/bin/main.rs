use std::any::Any;
use std::collections::HashSet;
use std::fmt::{Debug, Formatter};
use std::hash::RandomState;
use std::sync::Arc;
use async_trait::async_trait;

use datafusion::arrow::array::{AsArray, Int32Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Int32Type, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::{CatalogProvider, MemoryCatalogProvider, MemorySchemaProvider, SchemaProvider};
use datafusion::datasource::streaming::StreamingTable;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::Expr;
use datafusion_common::{ColumnStatistics, DataFusionError, Statistics};
use datafusion_common::stats::Precision;
use datafusion_physical_plan::{collect, DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion_physical_plan::metrics::MetricsSet;
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_physical_plan::streaming::PartitionStream;
use rand::distributions::Alphanumeric;
use rand::Rng;
use datafusion_parallelism::api_utils::{make_int_array_with_shift, make_string_array, make_string_constant_array};

use datafusion_parallelism::parse_sql::{JoinReplacement, make_session_state, parse_sql};

#[tokio::main]
async fn main() {
    // let session_state = make_session_state(None);
    // register_tables(&session_state);
    // run_plan(&session_state).await;
    //
    // let session_state = make_session_state(Some(JoinReplacement::Original));
    // register_tables(&session_state);
    // run_plan(&session_state).await;

    let session_state = make_session_state(Some(JoinReplacement::New10));
    register_tables(&session_state);
    for _ in 0..10 {
        run_plan(&session_state).await;
    }
}

async fn run_plan(session_state: &SessionState) {
    let plan = parse_sql(
        "SELECT result.id1, result.id2, result.id3, result.id4 \
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
  ON result.id4 = small_table_4.id",
        &session_state,
    ).await.unwrap();

    let result = collect(plan, Arc::new(TaskContext::default()))
        .await
        .unwrap();
    println!("Rows {:?}", result.iter().map(|b| b.num_rows()).reduce(|a, b| a + b))
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
    let batch_size = 512;
    let base_table_batch_count = 0;
    let small_table_batch_count = 12000;

    let base_record_batches =
        (0..base_table_batch_count).into_iter()
            .map(|i| {
                let i = i % 256;
                RecordBatch::try_new(
                    base_table_schema.clone(),
                    vec![
                        // Each id is offset by 1 so that they are different to each other
                        Arc::new(make_int_array_with_shift(i * batch_size, (i + 1) * batch_size, 0)),
                        Arc::new(make_int_array_with_shift(i * batch_size, (i + 1) * batch_size, 1)),
                        Arc::new(make_int_array_with_shift(i * batch_size, (i + 1) * batch_size, 2)),
                        Arc::new(make_int_array_with_shift(i * batch_size, (i + 1) * batch_size, 3)),
                        Arc::new(make_string_constant_array("hello".to_string(), batch_size)),
                        // Arc::new(make_string_array(std::iter::repeat_with(|| random_string(32)).take(batch_size as usize))),
                    ],
                ).unwrap()
            })
            .collect::<Vec<_>>();
    let base_table = StaticTable::new_with_row_count(base_table_schema, base_record_batches, 100_000_000);
    schema.register_table("base_table".to_string(), Arc::new(base_table)).unwrap();

    for table_number in 1..5 {
        let small_record_batches =
            (0..small_table_batch_count).into_iter()
                .map(|i| {
                    RecordBatch::try_new(
                        small_table_schema.clone(),
                        vec![
                            // Arc::new(make_int_array_with_shift(i * batch_size, (i + 1) * batch_size, table_number)),
                            Arc::new(make_int_array_with_shift(0, batch_size, table_number)),
                            Arc::new(make_string_array(std::iter::repeat_with(|| random_string(32)).take(batch_size as usize))),
                        ],
                    ).unwrap()
                })
                .collect::<Vec<_>>();
        let small_table = StaticTable::new(small_table_schema.clone(), small_record_batches);
        schema.register_table(format!("small_table_{}", table_number), Arc::new(small_table)).unwrap();
    }

    // Register catalog
    let catalog = Arc::new(MemoryCatalogProvider::new());
    catalog.register_schema("my_schema", schema)
        .map_err(|err| format!("register schema error: {}", err))
        .unwrap();
    session_state.catalog_list().register_catalog("my_catalog".to_string(), catalog);
}

fn random_string(length: usize) -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(length)
        .map(char::from)
        .collect()
}

struct StaticPartitionStream {
    schema: SchemaRef,
    data: Vec<RecordBatch>,
}

impl PartitionStream for StaticPartitionStream {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let batches = self.data.clone().into_iter()
            .map(|batch| -> Result<RecordBatch, DataFusionError> { Ok(batch) })
            .collect::<Vec<_>>();
        Box::pin(RecordBatchStreamAdapter::new(self.schema.clone(), futures::stream::iter(batches)))
    }
}

pub struct StaticTable {
    streaming_table: StreamingTable,
    statistics: Statistics,
}

impl StaticTable {
    pub fn new(schema: SchemaRef, data: Vec<RecordBatch>) -> Self {
        let statistics = Self::make_statistics(&schema, &data);
        let data_stream = StaticPartitionStream { schema: schema.clone(), data };
        let table = StreamingTable::try_new(schema.clone(), vec![Arc::new(data_stream)]).unwrap();
        Self {
            statistics,
            streaming_table: table,
        }
    }

    pub fn new_with_row_count(schema: SchemaRef, data: Vec<RecordBatch>, row_count: usize) -> Self {
        let mut this = Self::new(schema.clone(), data.clone());
        this.statistics.num_rows = Precision::Exact(row_count);
        this
    }

    pub fn make_statistics(schema: &Schema, data: &Vec<RecordBatch>) -> Statistics {
        let row_count = data.iter().map(|batch| batch.num_rows()).sum::<usize>();
        let data_size = data.iter().map(|batch| batch.get_array_memory_size()).sum::<usize>();
        let column_details = (0..schema.fields.len()).into_iter()
            .map(|col| {
                let col_schema = schema.field(col);
                if col_schema.data_type() == &DataType::Int32 {
                    let values_set: HashSet<i32, RandomState> = HashSet::from_iter(data.iter()
                        .flat_map(|batch| {
                            let col_data = batch.column(col);
                            col_data.as_primitive::<Int32Type>().iter()
                        })
                        .flatten());
                    let unknown_stats = ColumnStatistics::new_unknown();
                    ColumnStatistics {
                        null_count: unknown_stats.null_count,
                        max_value: Precision::Absent,
                        min_value: Precision::Absent,
                        distinct_count: Precision::Exact(values_set.len()),
                    }
                } else {
                    ColumnStatistics::new_unknown()
                }
            })
            .collect::<Vec<_>>();

        Statistics {
            num_rows: Precision::Exact(row_count),
            total_byte_size: Precision::Exact(data_size),
            column_statistics: column_details,
        }
    }

    fn always_statistics(&self) -> Statistics {
        self.statistics.clone()
    }
}

#[async_trait]
impl TableProvider for StaticTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.streaming_table.schema()
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    async fn scan(&self, state: &SessionState, projection: Option<&Vec<usize>>, filters: &[Expr], limit: Option<usize>) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(StreamingExecutionPlanWrapper {
            inner: self.streaming_table.scan(state, projection, filters, limit).await?,
            statistics: self.always_statistics(),
        }))
    }

    fn statistics(&self) -> Option<Statistics> {
        Some(self.always_statistics())
    }
}

struct StreamingExecutionPlanWrapper {
    pub statistics: Statistics,
    pub inner: Arc<dyn ExecutionPlan>,
}

impl Debug for StreamingExecutionPlanWrapper {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.inner.fmt(f)
    }
}

impl DisplayAs for StreamingExecutionPlanWrapper {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        self.inner.fmt_as(t, f)
    }
}

impl ExecutionPlan for StreamingExecutionPlanWrapper {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        self.inner.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.inner.children()
    }

    fn with_new_children(self: Arc<Self>, children: Vec<Arc<dyn ExecutionPlan>>) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        let inner = self.inner.clone().with_new_children(children)?;
        Ok(Arc::new(Self {
            statistics: self.statistics.clone(),
            inner
        }))
    }

    fn execute(&self, partition: usize, context: Arc<TaskContext>) -> datafusion_common::Result<SendableRecordBatchStream> {
        self.inner.execute(partition, context)
    }

    fn metrics(&self) -> Option<MetricsSet> {
        self.inner.metrics()
    }

    fn statistics(&self) -> datafusion_common::Result<Statistics> {
        Ok(self.statistics.clone())
    }
}
