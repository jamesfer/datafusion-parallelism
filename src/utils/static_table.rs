use std::any::Any;
use std::collections::HashSet;
use std::fmt::{Debug, Formatter};
use std::hash::RandomState;
use std::sync::Arc;
use async_trait::async_trait;
use datafusion::arrow::array::{AsArray, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Int32Type, Schema, SchemaRef};
use datafusion::datasource::streaming::StreamingTable;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::execution::context::SessionState;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::Expr;
use datafusion_common::{ColumnStatistics, DataFusionError, Statistics};
use datafusion_common::stats::Precision;
use datafusion_physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion_physical_plan::metrics::MetricsSet;
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_physical_plan::streaming::PartitionStream;

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
        let streaming_table = StreamingTable::try_new(schema.clone(), vec![Arc::new(data_stream)]).unwrap();
        Self {
            statistics,
            streaming_table,
        }
    }

    pub fn new_with_fixed_row_count(schema: SchemaRef, data: Vec<RecordBatch>, row_count: usize) -> Self {
        let mut statistics = Self::make_statistics(&schema, &data);
        statistics.num_rows = Precision::Exact(row_count);

        let data_stream = StaticPartitionStream { schema: schema.clone(), data };
        let streaming_table = StreamingTable::try_new(schema.clone(), vec![Arc::new(data_stream)]).unwrap();
        Self {
            statistics,
            streaming_table,
        }
    }

    pub fn new_with_fixed_statistics(schema: SchemaRef, data: Vec<RecordBatch>, statistics: Statistics) -> Self {
        let data_stream = StaticPartitionStream { schema: schema.clone(), data };
        let streaming_table = StreamingTable::try_new(schema.clone(), vec![Arc::new(data_stream)]).unwrap();
        Self {
            statistics,
            streaming_table,
        }
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
