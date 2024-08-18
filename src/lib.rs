mod sql_planner;
pub mod parse_sql;
mod utils;
pub mod version1;
pub mod version2;
pub mod version3;
pub mod api_utils;

pub fn add(left: usize, right: usize) -> usize {
    left + right
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use datafusion::arrow::array::{Int32Array, StringArray};
    use datafusion::arrow::compute::{concat_batches, sort_to_indices, take};
    use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::catalog::{CatalogProvider, MemoryCatalogProvider, MemorySchemaProvider, SchemaProvider};
    use datafusion::datasource::streaming::StreamingTable;
    use datafusion::execution::{SendableRecordBatchStream, TaskContext};
    use datafusion::execution::context::SessionState;
    use datafusion_common::DataFusionError;
    use datafusion_physical_plan::{collect, DefaultDisplay, displayable};
    use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
    use datafusion_physical_plan::streaming::PartitionStream;
    use crate::parse_sql::{JoinReplacement, make_session_state, parse_sql};

    #[tokio::test(flavor = "multi_thread")]
    async fn without_new_rule() {
        // Create session state
        let session_state = make_session_state(None);

        register_tables(&session_state);

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
        ).await
            .unwrap();

        let mut display_plan = displayable(plan.as_ref());
        println!("{}", display_plan.set_show_schema(true).indent(true));

        let results = collect(plan, Arc::new(TaskContext::default())).await.unwrap();
        let single_result = concat_batches(&results[0].schema(), results.iter()).unwrap();
        let indices = sort_to_indices(single_result.column(0), None, None).unwrap();
        let columns = single_result.columns().iter().map(|c| take(&*c, &indices, None).unwrap()).collect();
        let sorted = RecordBatch::try_new(single_result.schema(), columns).unwrap();

        assert_eq!(sorted, {
            let output_schema = Arc::new(Schema::new(vec![
                Field::new("id1", DataType::Int32, false),
                Field::new("id2", DataType::Int32, false),
                Field::new("id3", DataType::Int32, false),
                Field::new("id4", DataType::Int32, false),
                Field::new("value", DataType::Utf8, false),
                Field::new("id", DataType::Int32, false),
                Field::new("value", DataType::Utf8, false),
                Field::new("id", DataType::Int32, false),
                Field::new("value", DataType::Utf8, false),
                Field::new("id", DataType::Int32, false),
                Field::new("value", DataType::Utf8, false),
                Field::new("id", DataType::Int32, false),
                Field::new("value", DataType::Utf8, false),
            ]));
            RecordBatch::try_new(
                output_schema.clone(),
                vec![
                    Arc::new(make_int_array(0, 1024)),
                    Arc::new(make_int_array(0, 1024)),
                    Arc::new(make_int_array(0, 1024)),
                    Arc::new(make_int_array(0, 1024)),
                    Arc::new(make_string_array("hello".to_string(), 1024)),
                    Arc::new(make_int_array(0, 1024)),
                    Arc::new(make_string_array("world".to_string(), 1024)),
                    Arc::new(make_int_array(0, 1024)),
                    Arc::new(make_string_array("world".to_string(), 1024)),
                    Arc::new(make_int_array(0, 1024)),
                    Arc::new(make_string_array("world".to_string(), 1024)),
                    Arc::new(make_int_array(0, 1024)),
                    Arc::new(make_string_array("world".to_string(), 1024)),
                ],
            ).unwrap()
        });
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn with_new_rule() {
        // Create session state
        let session_state = make_session_state(Some(JoinReplacement::Original));

        register_tables(&session_state);

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
        ).await
            .unwrap();

        let mut display_plan = displayable(plan.as_ref());
        println!("{}", display_plan.set_show_schema(true).indent(true));

        let results = collect(plan, Arc::new(TaskContext::default())).await.unwrap();
        let single_result = concat_batches(&results[0].schema(), results.iter()).unwrap();
        let indices = sort_to_indices(single_result.column(0), None, None).unwrap();
        let columns = single_result.columns().iter().map(|c| take(&*c, &indices, None).unwrap()).collect();
        let sorted = RecordBatch::try_new(single_result.schema(), columns).unwrap();

        assert_eq!(sorted, {
            let output_schema = Arc::new(Schema::new(vec![
                Field::new("id1", DataType::Int32, false),
                Field::new("id2", DataType::Int32, false),
                Field::new("id3", DataType::Int32, false),
                Field::new("id4", DataType::Int32, false),
                Field::new("value", DataType::Utf8, false),
                Field::new("id", DataType::Int32, false),
                Field::new("value", DataType::Utf8, false),
                Field::new("id", DataType::Int32, false),
                Field::new("value", DataType::Utf8, false),
                Field::new("id", DataType::Int32, false),
                Field::new("value", DataType::Utf8, false),
                Field::new("id", DataType::Int32, false),
                Field::new("value", DataType::Utf8, false),
            ]));
            RecordBatch::try_new(
                output_schema.clone(),
                vec![
                    Arc::new(make_int_array(0, 1024)),
                    Arc::new(make_int_array(0, 1024)),
                    Arc::new(make_int_array(0, 1024)),
                    Arc::new(make_int_array(0, 1024)),
                    Arc::new(make_string_array("hello".to_string(), 1024)),
                    Arc::new(make_int_array(0, 1024)),
                    Arc::new(make_string_array("world".to_string(), 1024)),
                    Arc::new(make_int_array(0, 1024)),
                    Arc::new(make_string_array("world".to_string(), 1024)),
                    Arc::new(make_int_array(0, 1024)),
                    Arc::new(make_string_array("world".to_string(), 1024)),
                    Arc::new(make_int_array(0, 1024)),
                    Arc::new(make_string_array("world".to_string(), 1024)),
                ],
            ).unwrap()
        });
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn with_new_rule2() {
        // Create session state
        let session_state = make_session_state(Some(JoinReplacement::New));

        register_tables(&session_state);

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
        ).await
            .unwrap();

        let mut display_plan = displayable(plan.as_ref());
        println!("{}", display_plan.set_show_schema(true).indent(true));

        let results = collect(plan, Arc::new(TaskContext::default())).await.unwrap();
        let single_result = concat_batches(&results[0].schema(), results.iter()).unwrap();
        let indices = sort_to_indices(single_result.column(0), None, None).unwrap();
        let columns = single_result.columns().iter().map(|c| take(&*c, &indices, None).unwrap()).collect();
        let sorted = RecordBatch::try_new(single_result.schema(), columns).unwrap();

        assert_eq!(sorted, {
            let output_schema = Arc::new(Schema::new(vec![
                Field::new("id1", DataType::Int32, false),
                Field::new("id2", DataType::Int32, false),
                Field::new("id3", DataType::Int32, false),
                Field::new("id4", DataType::Int32, false),
                Field::new("value", DataType::Utf8, false),
                Field::new("id", DataType::Int32, false),
                Field::new("value", DataType::Utf8, false),
                Field::new("id", DataType::Int32, false),
                Field::new("value", DataType::Utf8, false),
                Field::new("id", DataType::Int32, false),
                Field::new("value", DataType::Utf8, false),
                Field::new("id", DataType::Int32, false),
                Field::new("value", DataType::Utf8, false),
            ]));
            RecordBatch::try_new(
                output_schema.clone(),
                vec![
                    Arc::new(make_int_array(0, 1024)),
                    Arc::new(make_int_array(0, 1024)),
                    Arc::new(make_int_array(0, 1024)),
                    Arc::new(make_int_array(0, 1024)),
                    Arc::new(make_string_array("hello".to_string(), 1024)),
                    Arc::new(make_int_array(0, 1024)),
                    Arc::new(make_string_array("world".to_string(), 1024)),
                    Arc::new(make_int_array(0, 1024)),
                    Arc::new(make_string_array("world".to_string(), 1024)),
                    Arc::new(make_int_array(0, 1024)),
                    Arc::new(make_string_array("world".to_string(), 1024)),
                    Arc::new(make_int_array(0, 1024)),
                    Arc::new(make_string_array("world".to_string(), 1024)),
                ],
            ).unwrap()
        });
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn with_new_rule3() {
        // Create session state
        let session_state = make_session_state(Some(JoinReplacement::New3));

        register_tables(&session_state);

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
        ).await
            .unwrap();

        let mut display_plan = displayable(plan.as_ref());
        println!("{}", display_plan.set_show_schema(true).indent(true));

        let results = collect(plan, Arc::new(TaskContext::default())).await.unwrap();
        let single_result = concat_batches(&results[0].schema(), results.iter()).unwrap();
        let indices = sort_to_indices(single_result.column(0), None, None).unwrap();
        let columns = single_result.columns().iter().map(|c| take(&*c, &indices, None).unwrap()).collect();
        let sorted = RecordBatch::try_new(single_result.schema(), columns).unwrap();

        assert_eq!(sorted, {
            let output_schema = Arc::new(Schema::new(vec![
                Field::new("id1", DataType::Int32, false),
                Field::new("id2", DataType::Int32, false),
                Field::new("id3", DataType::Int32, false),
                Field::new("id4", DataType::Int32, false),
                Field::new("value", DataType::Utf8, false),
                Field::new("id", DataType::Int32, false),
                Field::new("value", DataType::Utf8, false),
                Field::new("id", DataType::Int32, false),
                Field::new("value", DataType::Utf8, false),
                Field::new("id", DataType::Int32, false),
                Field::new("value", DataType::Utf8, false),
                Field::new("id", DataType::Int32, false),
                Field::new("value", DataType::Utf8, false),
            ]));
            RecordBatch::try_new(
                output_schema.clone(),
                vec![
                    Arc::new(make_int_array(0, 1024)),
                    Arc::new(make_int_array(0, 1024)),
                    Arc::new(make_int_array(0, 1024)),
                    Arc::new(make_int_array(0, 1024)),
                    Arc::new(make_string_array("hello".to_string(), 1024)),
                    Arc::new(make_int_array(0, 1024)),
                    Arc::new(make_string_array("world".to_string(), 1024)),
                    Arc::new(make_int_array(0, 1024)),
                    Arc::new(make_string_array("world".to_string(), 1024)),
                    Arc::new(make_int_array(0, 1024)),
                    Arc::new(make_string_array("world".to_string(), 1024)),
                    Arc::new(make_int_array(0, 1024)),
                    Arc::new(make_string_array("world".to_string(), 1024)),
                ],
            ).unwrap()
        });
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
            (0..1).into_iter()
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
                (0..1).into_iter()
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
}
