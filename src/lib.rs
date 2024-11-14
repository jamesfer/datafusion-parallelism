pub mod parse_sql;
pub mod utils;
pub mod api_utils;
mod shared;
pub mod operator;

pub fn add(left: usize, right: usize) -> usize {
    left + right
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use datafusion::arrow::array::{ArrayRef, Int32Array, StringArray};
    use datafusion::arrow::compute::{concat_batches, sort_to_indices, take, SortOptions};
    use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::catalog::{CatalogProvider, MemoryCatalogProvider, MemorySchemaProvider, SchemaProvider};
    use datafusion::datasource::streaming::StreamingTable;
    use datafusion::execution::{SendableRecordBatchStream, TaskContext};
    use datafusion::execution::context::SessionState;
    use datafusion_common::{ColumnStatistics, DataFusionError, JoinType, Statistics};
    use datafusion_common::stats::Precision;
    use datafusion_physical_plan::{collect, displayable, ExecutionPlan};
    use datafusion_physical_plan::joins::HashJoinExec;
    use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
    use datafusion_physical_plan::streaming::PartitionStream;
    use crate::api_utils::{make_int_array, make_int_array_from_range, make_int_array_it, make_int_array_it_nullable, make_string_array, make_string_array_nullable, make_string_constant_array};
    use crate::operator::parallel_hash_join::ParallelHashJoin;
    use crate::parse_sql::{JoinReplacement, make_session_state, parse_sql, make_session_state_with_config};
    use crate::utils::static_table::StaticTable;

    macro_rules! multi_tests {
        ($f:ident, $($name:ident: $value:expr,)*) => {
            $(
                #[tokio::test(flavor = "multi_thread")]
                async fn $name() {
                    $f($value).await
                }
            )*
        }
    }

    multi_tests! {
        inner_join_no_filter,
        inner_join_no_filter_none: None,
        inner_join_no_filter_original: Some(JoinReplacement::Original),
        inner_join_no_filter_new: Some(JoinReplacement::New),
        inner_join_no_filter_new3: Some(JoinReplacement::New3),
        inner_join_no_filter_new4: Some(JoinReplacement::New4),
        inner_join_no_filter_new5: Some(JoinReplacement::New5),
        inner_join_no_filter_new6: Some(JoinReplacement::New6),
        inner_join_no_filter_new7: Some(JoinReplacement::New7),
        inner_join_no_filter_new8: Some(JoinReplacement::New8),
        inner_join_no_filter_new9: Some(JoinReplacement::New9),
    }

    async fn inner_join_no_filter(rule: Option<JoinReplacement>) {
        let session_state = make_session_state_with_config(rule, None, true);

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
                Field::new("id1", DataType::Int32, true),
                Field::new("id2", DataType::Int32, true),
                Field::new("id3", DataType::Int32, true),
                Field::new("id4", DataType::Int32, true),
                Field::new("value", DataType::Utf8, true),
                Field::new("id", DataType::Int32, true),
                Field::new("value", DataType::Utf8, true),
                Field::new("id", DataType::Int32, true),
                Field::new("value", DataType::Utf8, true),
                Field::new("id", DataType::Int32, true),
                Field::new("value", DataType::Utf8, true),
                Field::new("id", DataType::Int32, true),
                Field::new("value", DataType::Utf8, true),
            ]));
            RecordBatch::try_new(
                output_schema.clone(),
                vec![
                    Arc::new(make_int_array_from_range(0, 1024)),
                    Arc::new(make_int_array_from_range(0, 1024)),
                    Arc::new(make_int_array_from_range(0, 1024)),
                    Arc::new(make_int_array_from_range(0, 1024)),
                    Arc::new(make_string_constant_array("hello".to_string(), 1024)),
                    Arc::new(make_int_array_from_range(0, 1024)),
                    Arc::new(make_string_constant_array("world".to_string(), 1024)),
                    Arc::new(make_int_array_from_range(0, 1024)),
                    Arc::new(make_string_constant_array("world".to_string(), 1024)),
                    Arc::new(make_int_array_from_range(0, 1024)),
                    Arc::new(make_string_constant_array("world".to_string(), 1024)),
                    Arc::new(make_int_array_from_range(0, 1024)),
                    Arc::new(make_string_constant_array("world".to_string(), 1024)),
                ],
            ).unwrap()
        });
    }

    multi_tests! {
        inner_join_with_nulls,
        inner_join_with_nulls_none: None,
        inner_join_with_nulls_original: Some(JoinReplacement::Original),
        inner_join_with_nulls_new: Some(JoinReplacement::New),
        inner_join_with_nulls_new3: Some(JoinReplacement::New3),
        inner_join_with_nulls_new4: Some(JoinReplacement::New4),
        inner_join_with_nulls_new5: Some(JoinReplacement::New5),
        inner_join_with_nulls_new6: Some(JoinReplacement::New6),
        inner_join_with_nulls_new7: Some(JoinReplacement::New7),
        inner_join_with_nulls_new8: Some(JoinReplacement::New8),
        inner_join_with_nulls_new9: Some(JoinReplacement::New9),
    }

    async fn inner_join_with_nulls(rule: Option<JoinReplacement>) {
        let session_state = make_session_state_with_config(rule, None, true);
        let schema = Arc::new(MemorySchemaProvider::new());

        schema.register_table("left".to_string(), Arc::new(make_table(
            1,
            vec![
                ("id", DataType::Int32, Box::new(|i| Arc::new(make_int_array(vec![Some(1), Some(2), None].into_iter())))),
                ("value", DataType::Utf8, Box::new(|_| Arc::new(make_string_constant_array("left".to_string(), 3)))),
            ],
        ))).unwrap();
        schema.register_table("right".to_string(), Arc::new(make_table(
            1,
            vec![
                ("id", DataType::Int32, Box::new(|i| Arc::new(make_int_array(vec![None, Some(2), Some(3)].into_iter())))),
                ("value", DataType::Utf8, Box::new(|_| Arc::new(make_string_constant_array("right".to_string(), 3)))),
            ],
        ))).unwrap();

        make_catalog_from_schema(&session_state, schema);

        let plan = parse_sql(
            "SELECT * \
            FROM my_catalog.my_schema.left \
            JOIN my_catalog.my_schema.right \
              ON left.id = right.id",
            &session_state,
        ).await
            .unwrap();

        let results = collect(plan, Arc::new(TaskContext::default())).await.unwrap();
        let single_result = concat_batches(&results[0].schema(), results.iter()).unwrap();
        let indices = sort_to_indices(single_result.column(0), None, None).unwrap();
        let columns = single_result.columns().iter().map(|c| take(&*c, &indices, None).unwrap()).collect();
        let sorted = RecordBatch::try_new(single_result.schema(), columns).unwrap();

        assert_eq!(sorted, make_record_batch(
            vec![
                ("id", DataType::Int32, Arc::new(make_int_array(vec![Some(2)].into_iter()))),
                ("value", DataType::Utf8, Arc::new(make_string_array(vec!["left".to_string()]))),
                ("id", DataType::Int32, Arc::new(make_int_array(vec![Some(2)].into_iter()))),
                ("value", DataType::Utf8, Arc::new(make_string_array(vec!["right".to_string()]))),
            ],
        ));
    }

    multi_tests! {
        inner_join_without_matches,
        inner_join_without_matches_none: None,
        inner_join_without_matches_original: Some(JoinReplacement::Original),
        inner_join_without_matches_new: Some(JoinReplacement::New),
        inner_join_without_matches_new3: Some(JoinReplacement::New3),
        inner_join_without_matches_new4: Some(JoinReplacement::New4),
        inner_join_without_matches_new5: Some(JoinReplacement::New5),
        inner_join_without_matches_new6: Some(JoinReplacement::New6),
        inner_join_without_matches_new7: Some(JoinReplacement::New7),
        inner_join_without_matches_new8: Some(JoinReplacement::New8),
        inner_join_without_matches_new9: Some(JoinReplacement::New9),
    }

    async fn inner_join_without_matches(rule: Option<JoinReplacement>) {
        let session_state = make_session_state_with_config(rule, None, true);
        let schema = Arc::new(MemorySchemaProvider::new());

        schema.register_table("left".to_string(), Arc::new(make_table(
            1,
            vec![
                ("id", DataType::Int32, Box::new(|i| Arc::new(make_int_array(vec![Some(1), Some(2), None].into_iter())))),
                ("value", DataType::Utf8, Box::new(|_| Arc::new(make_string_constant_array("left".to_string(), 3)))),
            ],
        ))).unwrap();
        schema.register_table("right".to_string(), Arc::new(make_table(
            1,
            vec![
                ("id", DataType::Int32, Box::new(|i| Arc::new(make_int_array(vec![None, Some(4), Some(5)].into_iter())))),
                ("value", DataType::Utf8, Box::new(|_| Arc::new(make_string_constant_array("right".to_string(), 3)))),
            ],
        ))).unwrap();

        make_catalog_from_schema(&session_state, schema);

        let plan = parse_sql(
            "SELECT * \
            FROM my_catalog.my_schema.left \
            JOIN my_catalog.my_schema.right \
              ON left.id = right.id",
            &session_state,
        ).await
            .unwrap();

        let results = collect(plan, Arc::new(TaskContext::default())).await.unwrap();

        // TODO for now we accept either no batches, or a single empty batch. We should fix the
        //   new implementations to not emit empty batches
        let total_length = results.iter().map(|batch| batch.num_rows()).sum::<usize>();
        assert_eq!(total_length, 0);
    }

    // TODO left join not yet supported
    // multi_tests! {
    //     left_join,
    //     left_join_none: None,
    //     left_join_original: Some(JoinReplacement::Original),
    //     left_join_new: Some(JoinReplacement::New),
    //     left_join_new3: Some(JoinReplacement::New3),
    //     left_join_new4: Some(JoinReplacement::New4),
    //     left_join_new5: Some(JoinReplacement::New5),
    //     left_join_new6: Some(JoinReplacement::New6),
    //     left_join_new7: Some(JoinReplacement::New7),
    //     left_join_new8: Some(JoinReplacement::New8),
    //     left_join_new9: Some(JoinReplacement::New9),
    // }

    async fn left_join(rule: Option<JoinReplacement>) {
        let session_state = make_session_state_with_config(rule, None, true);
        let schema = Arc::new(MemorySchemaProvider::new());

        schema.register_table("left".to_string(), Arc::new(make_table(
            1,
            vec![
                ("id", DataType::Int32, Box::new(|i| Arc::new(make_int_array(vec![Some(1), Some(2), None].into_iter())))),
                ("value", DataType::Utf8, Box::new(|_| Arc::new(make_string_constant_array("left".to_string(), 3)))),
            ],
        ))).unwrap();
        schema.register_table("right".to_string(), Arc::new(make_table(
            1,
            vec![
                ("id", DataType::Int32, Box::new(|i| Arc::new(make_int_array(vec![Some(1), Some(1), None].into_iter())))),
                ("value", DataType::Utf8, Box::new(|_| Arc::new(make_string_constant_array("right".to_string(), 3)))),
            ],
        ))).unwrap();

        make_catalog_from_schema(&session_state, schema);

        let plan = parse_sql(
            "SELECT * \
            FROM my_catalog.my_schema.left \
            LEFT JOIN my_catalog.my_schema.right \
              ON left.id = right.id",
            &session_state,
        ).await
            .unwrap();

        let results = collect(plan, Arc::new(TaskContext::default())).await.unwrap();
        let single_result = concat_batches(&results[0].schema(), results.iter()).unwrap();
        let indices = sort_to_indices(single_result.column(0), None, None).unwrap();
        let columns = single_result.columns().iter().map(|c| take(&*c, &indices, None).unwrap()).collect();
        let sorted = RecordBatch::try_new(single_result.schema(), columns).unwrap();

        assert_eq!(sorted, make_record_batch(
            vec![
                ("id", DataType::Int32, Arc::new(make_int_array(vec![1, 1, 2].into_iter()))),
                ("value", DataType::Utf8, Arc::new(make_string_array(vec!["left".to_string(), "left".to_string(), "left".to_string()]))),
                ("id", DataType::Int32, Arc::new(make_int_array(vec![Some(1), Some(1), None].into_iter()))),
                ("value", DataType::Utf8, Arc::new(make_string_array_nullable(vec![Some("right".to_string()), Some("right".to_string()), None]))),
            ],
        ));
    }

    multi_tests! {
        right_join,
        right_join_none: None,
        right_join_original: Some(JoinReplacement::Original),
        right_join_new: Some(JoinReplacement::New),
        right_join_new3: Some(JoinReplacement::New3),
        right_join_new4: Some(JoinReplacement::New4),
        right_join_new5: Some(JoinReplacement::New5),
        right_join_new6: Some(JoinReplacement::New6),
        right_join_new7: Some(JoinReplacement::New7),
        right_join_new8: Some(JoinReplacement::New8),
        right_join_new9: Some(JoinReplacement::New9),
    }

    async fn right_join(rule: Option<JoinReplacement>) {
        let session_state = make_session_state_with_config(rule.clone(), None, true);
        let schema = Arc::new(MemorySchemaProvider::new());

        schema.register_table("left".to_string(), Arc::new(make_table(
            1,
            vec![
                ("id", DataType::Int32, Box::new(|i| Arc::new(make_int_array(vec![Some(1), Some(1), None].into_iter())))),
                ("value", DataType::Utf8, Box::new(|_| Arc::new(make_string_constant_array("left".to_string(), 3)))),
            ],
        ))).unwrap();
        schema.register_table("right".to_string(), Arc::new(make_table(
            1,
            vec![
                ("id", DataType::Int32, Box::new(|i| Arc::new(make_int_array(vec![Some(1), Some(2), None].into_iter())))),
                ("value", DataType::Utf8, Box::new(|_| Arc::new(make_string_constant_array("right".to_string(), 3)))),
            ],
        ))).unwrap();

        make_catalog_from_schema(&session_state, schema);

        let plan = parse_sql(
            "SELECT * \
            FROM my_catalog.my_schema.left \
            FULL OUTER JOIN my_catalog.my_schema.right \
              ON left.id = right.id \
            WHERE right.id IS NOT NULL",
            &session_state,
        ).await
            .unwrap();

        // Assert that the plan is actually using a right join
        let join_type = get_join_type(&rule, &plan);
        assert_eq!(join_type, Some(&JoinType::Right), "plan must use right anti join for test to be valid: {}", displayable(plan.as_ref()).indent(true));

        let results = collect_and_sort_results(plan, 2).await.unwrap();
        assert_eq!(results, make_record_batch(
            vec![
                ("id", DataType::Int32, Arc::new(make_int_array(vec![Some(1), Some(1), None].into_iter()))),
                ("value", DataType::Utf8, Arc::new(make_string_array_nullable(vec![Some("left".to_string()), Some("left".to_string()), None]))),
                ("id", DataType::Int32, Arc::new(make_int_array(vec![1, 1, 2].into_iter()))),
                ("value", DataType::Utf8, Arc::new(make_string_array(vec!["right".to_string(), "right".to_string(), "right".to_string()]))),
            ],
        ));
    }

    multi_tests! {
        right_anti_join,
        right_anti_join_none: None,
        right_anti_join_original: Some(JoinReplacement::Original),
        right_anti_join_new: Some(JoinReplacement::New),
        right_anti_join_new3: Some(JoinReplacement::New3),
        right_anti_join_new4: Some(JoinReplacement::New4),
        right_anti_join_new5: Some(JoinReplacement::New5),
        right_anti_join_new6: Some(JoinReplacement::New6),
        right_anti_join_new7: Some(JoinReplacement::New7),
        right_anti_join_new8: Some(JoinReplacement::New8),
        right_anti_join_new9: Some(JoinReplacement::New9),
    }

    async fn right_anti_join(rule: Option<JoinReplacement>) {
        let session_state = make_session_state_with_config(rule.clone(), None, true);
        let schema = Arc::new(MemorySchemaProvider::new());

        schema.register_table("left".to_string(), Arc::new(make_table_with_statistics(
            1,
            vec![
                ("id", DataType::Int32, Box::new(|i| Arc::new(make_int_array(vec![Some(1), Some(2), None].into_iter())))),
                ("value", DataType::Utf8, Box::new(|_| Arc::new(make_string_constant_array("left".to_string(), 3)))),
            ],
        ))).unwrap();
        schema.register_table("right".to_string(), Arc::new(make_table_with_fixed_statistics(
            1,
            // We fake the number of rows in the right table to force the optimiser to transform it
            // into a right anti join
            Statistics {
                num_rows: Precision::Exact(1_000_000),
                total_byte_size: Precision::Absent,
                column_statistics: vec![
                    ColumnStatistics {
                        null_count: Precision::Absent,
                        max_value: Precision::Absent,
                        min_value: Precision::Absent,
                        distinct_count: Precision::Exact(1_000_000),
                    },
                    ColumnStatistics::new_unknown(),
                ],
            },
            vec![
                ("id", DataType::Int32, Box::new(|i| Arc::new(make_int_array(vec![Some(1), Some(2), Some(3), None].into_iter())))),
                ("value", DataType::Utf8, Box::new(|_| Arc::new(make_string_constant_array("right".to_string(), 4)))),
            ],
        ))).unwrap();

        make_catalog_from_schema(&session_state, schema);

        let plan = parse_sql(
            "SELECT * \
            FROM my_catalog.my_schema.right \
            WHERE NOT EXISTS ( \
              SELECT * \
              FROM my_catalog.my_schema.left \
              WHERE left.id = right.id \
            )",
            &session_state,
        ).await
            .unwrap();

        // Assert that the plan is actually using a right anti join
        let join_type = get_join_type(&rule, &plan);
        assert_eq!(join_type, Some(&JoinType::RightAnti), "plan must use right anti join for test to be valid: {}", displayable(plan.as_ref()).indent(true));

        let results = collect_and_sort_results(plan, 0).await.unwrap();
        assert_eq!(results, make_record_batch(
            vec![
                ("id", DataType::Int32, Arc::new(make_int_array(vec![Some(3), None].into_iter()))),
                ("value", DataType::Utf8, Arc::new(make_string_array(vec!["right".to_string(), "right".to_string()]))),
            ],
        ));
    }

    multi_tests! {
        full_join,
        full_join_none: None,
        full_join_original: Some(JoinReplacement::Original),
        full_join_new: Some(JoinReplacement::New),
        full_join_new3: Some(JoinReplacement::New3),
        full_join_new4: Some(JoinReplacement::New4),
        full_join_new5: Some(JoinReplacement::New5),
        full_join_new6: Some(JoinReplacement::New6),
        full_join_new7: Some(JoinReplacement::New7),
        full_join_new8: Some(JoinReplacement::New8),
        full_join_new9: Some(JoinReplacement::New9),
    }

    async fn full_join(rule: Option<JoinReplacement>) {
        let session_state = make_session_state_with_config(rule.clone(), None, true);
        let schema = Arc::new(MemorySchemaProvider::new());

        schema.register_table("left".to_string(), Arc::new(make_table(
            1,
            vec![
                ("id", DataType::Int32, Box::new(|i| Arc::new(make_int_array(vec![Some(1), Some(2), None].into_iter())))),
                ("value", DataType::Utf8, Box::new(|_| Arc::new(make_string_constant_array("left".to_string(), 3)))),
            ],
        ))).unwrap();
        schema.register_table("right".to_string(), Arc::new(make_table(
            1,
            vec![
                ("id", DataType::Int32, Box::new(|i| Arc::new(make_int_array(vec![Some(2), Some(3), None].into_iter())))),
                ("value", DataType::Utf8, Box::new(|_| Arc::new(make_string_constant_array("right".to_string(), 3)))),
            ],
        ))).unwrap();

        make_catalog_from_schema(&session_state, schema);

        let plan = parse_sql(
            "SELECT * \
            FROM my_catalog.my_schema.left \
            FULL JOIN my_catalog.my_schema.right \
              ON left.id = right.id",
            &session_state,
        ).await
            .unwrap();

        // Assert that the plan is actually using a full join
        let join_type = get_join_type(&rule, &plan);
        assert_eq!(join_type, Some(&JoinType::Full), "plan must use full join for test to be valid: {}", displayable(plan.as_ref()).indent(true));

        let results = collect_and_order_results(plan, vec![0, 2]).await.unwrap();
        assert_eq!(results, make_record_batch(
            vec![
                ("id", DataType::Int32, Arc::new(make_int_array(vec![Some(1), Some(2), None, None, None].into_iter()))),
                ("value", DataType::Utf8, Arc::new(make_string_array_nullable(vec![Some("left".to_string()), Some("left".to_string()), None, None, Some("left".to_string())]))),
                ("id", DataType::Int32, Arc::new(make_int_array(vec![None, Some(2), Some(3), None, None].into_iter()))),
                ("value", DataType::Utf8, Arc::new(make_string_array_nullable(vec![None, Some("right".to_string()), Some("right".to_string()), Some("right".to_string()), None]))),
            ],
        ));
    }

    // multi_tests! {
    //     full_join_with_filter,
    //     full_join_with_filter_none: None,
    //     full_join_with_filter_original: Some(JoinReplacement::Original),
    //     full_join_with_filter_new: Some(JoinReplacement::New),
    //     full_join_with_filter_new3: Some(JoinReplacement::New3),
    //     full_join_with_filter_new4: Some(JoinReplacement::New4),
    //     full_join_with_filter_new5: Some(JoinReplacement::New5),
    //     full_join_with_filter_new6: Some(JoinReplacement::New6),
    //     full_join_with_filter_new7: Some(JoinReplacement::New7),
    //     full_join_with_filter_new8: Some(JoinReplacement::New8),
    //     full_join_with_filter_new9: Some(JoinReplacement::New9),
    // }
    //
    // async fn full_join_with_filter(rule: Option<JoinReplacement>) {
    //     let session_state = make_session_state_with_config(rule.clone(), None, true);
    //     let schema = Arc::new(MemorySchemaProvider::new());
    //
    //     schema.register_table("left".to_string(), Arc::new(make_table(
    //         1,
    //         vec![
    //             ("id", DataType::Int32, Box::new(|i| Arc::new(make_int_array(vec![Some(1), Some(2), Some(3), None].into_iter())))),
    //             ("value", DataType::Utf8, Box::new(|_| Arc::new(make_string_array(vec!["left".to_string(), "left".to_string(), "same".to_string(), "left".to_string()])))),
    //         ],
    //     ))).unwrap();
    //     schema.register_table("right".to_string(), Arc::new(make_table(
    //         1,
    //         vec![
    //             ("id", DataType::Int32, Box::new(|i| Arc::new(make_int_array(vec![Some(2), Some(3), Some(4), None].into_iter())))),
    //             ("value", DataType::Utf8, Box::new(|_| Arc::new(make_string_array(vec!["right".to_string(), "same".to_string(), "right".to_string(), "right".to_string()])))),
    //         ],
    //     ))).unwrap();
    //
    //     make_catalog_from_schema(&session_state, schema);
    //
    //     let plan = parse_sql(
    //         "SELECT left.id, right.id \
    //         FROM my_catalog.my_schema.left \
    //         FULL JOIN my_catalog.my_schema.right \
    //           ON left.id = right.id \
    //         WHERE left.value != right.value",
    //         &session_state,
    //     ).await
    //         .unwrap();
    //
    //     // Assert that the plan is actually using a full join
    //     let join_type = get_join_type(&rule, &plan);
    //     assert_eq!(join_type, Some(&JoinType::Full), "plan must use full join for test to be valid: {}", displayable(plan.as_ref()).indent(true));
    //
    //     let results = collect_and_order_results(plan, vec![0, 2]).await.unwrap();
    //     assert_eq!(results, make_record_batch(
    //         vec![
    //             ("id", DataType::Int32, Arc::new(make_int_array(vec![
    //                 Some(1),
    //                 Some(2),
    //                 Some(3),
    //                 None,
    //                 None,
    //                 None,
    //                 None,
    //             ]))),
    //             ("value", DataType::Utf8, Arc::new(make_string_array_nullable(vec![
    //                 Some("left".to_string()),
    //                 Some("left".to_string()),
    //                 Some("same".to_string()),
    //                 None,
    //                 None,
    //                 None,
    //                 Some("left".to_string()),
    //             ]))),
    //             ("id", DataType::Int32, Arc::new(make_int_array(vec![
    //                 None,
    //                 None,
    //                 Some(3),
    //                 Some(2),
    //                 Some(4),
    //                 None,
    //                 None,
    //             ].into_iter()))),
    //             ("value", DataType::Utf8, Arc::new(make_string_array_nullable(vec![
    //                 None,
    //                 None,
    //                 Some("same".to_string()),
    //                 Some("right".to_string()),
    //                 Some("right".to_string()),
    //                 None,
    //             ]))),
    //         ],
    //     ));
    // }

    fn get_join_type<'a>(rule: &Option<JoinReplacement>, plan: &'a Arc<dyn ExecutionPlan>) -> Option<&'a JoinType> {
        if rule.is_none() {
            // Root node is a CoalesceBatchesExec
            plan.children()
                .first()
                .map(|child| child.as_any().downcast_ref::<HashJoinExec>())
                .flatten()
                .map(|hash_join| hash_join.join_type())
        } else {
            plan.as_any().downcast_ref::<ParallelHashJoin>().map(|parallel_join| parallel_join.join_type())
        }
    }


    async fn collect_and_sort_results(plan: Arc<dyn ExecutionPlan>, sort_column: usize) -> Option<RecordBatch> {
        let results = collect(plan, Arc::new(TaskContext::default())).await.unwrap();
        match results.first() {
            None => None,
            Some(first) => {
                let single_result = concat_batches(&first.schema(), results.iter()).unwrap();
                let sort_options = SortOptions {
                    descending: false,
                    nulls_first: false,
                };
                let indices = sort_to_indices(single_result.column(sort_column), Some(sort_options), None).unwrap();
                let columns = single_result.columns().iter().map(|c| take(&*c, &indices, None).unwrap()).collect();
                let sorted = RecordBatch::try_new(single_result.schema(), columns).unwrap();
                Some(sorted)
            }
        }
    }

    async fn collect_and_order_results(plan: Arc<dyn ExecutionPlan>, sort_columns: Vec<usize>) -> Option<RecordBatch> {
        let results = collect(plan, Arc::new(TaskContext::default())).await.unwrap();
        match results.first() {
            None => None,
            Some(first) => {
                let single_result = concat_batches(&first.schema(), results.iter()).unwrap();
                let sort_options = SortOptions {
                    descending: false,
                    nulls_first: false,
                };
                let sorted = sort_columns.iter().rev().fold(single_result, |acc, sort_column| {
                    let indices = sort_to_indices(acc.column(*sort_column), Some(sort_options), None).unwrap();
                    let columns = acc.columns().iter().map(|c| take(&*c, &indices, None).unwrap()).collect();
                    RecordBatch::try_new(acc.schema(), columns).unwrap()
                });
                Some(sorted)
            }
        }
    }

    const BATCH_SIZE: i32 = 64;

    fn register_tables(session_state: &SessionState) {
        let schema = Arc::new(MemorySchemaProvider::new());
        schema.register_table("base_table".to_string(), Arc::new(make_table(
            16,
            vec![
                ("id1", DataType::Int32, Box::new(|i| Arc::new(make_int_array_from_range(i as i32 * BATCH_SIZE, (i as i32 + 1) * BATCH_SIZE)))),
                ("id2", DataType::Int32, Box::new(|i| Arc::new(make_int_array_from_range(i as i32 * BATCH_SIZE, (i as i32 + 1) * BATCH_SIZE)))),
                ("id3", DataType::Int32, Box::new(|i| Arc::new(make_int_array_from_range(i as i32 * BATCH_SIZE, (i as i32 + 1) * BATCH_SIZE)))),
                ("id4", DataType::Int32, Box::new(|i| Arc::new(make_int_array_from_range(i as i32 * BATCH_SIZE, (i as i32 + 1) * BATCH_SIZE)))),
                ("value", DataType::Utf8, Box::new(|_| Arc::new(make_string_constant_array("hello".to_string(), BATCH_SIZE)))),
            ],
        ))).unwrap();

        for i in 1..5 {
            schema.register_table(format!("small_table_{}", i), Arc::new(make_table(
                16,
                vec![
                    ("id", DataType::Int32, Box::new(|i| Arc::new(make_int_array_from_range(i as i32 * BATCH_SIZE, (i as i32 + 1) * BATCH_SIZE)))),
                    ("value", DataType::Utf8, Box::new(|_| Arc::new(make_string_constant_array("world".to_string(), BATCH_SIZE)))),
                ],
            ))).unwrap();
        }

        make_catalog_from_schema(session_state, schema);
    }

    fn make_catalog_from_schema(session_state: &SessionState, schema: Arc<dyn SchemaProvider>) {
        let catalog = Arc::new(MemoryCatalogProvider::new());
        catalog.register_schema("my_schema", schema)
            .map_err(|err| format!("register schema error: {}", err))
            .unwrap();
        session_state.catalog_list().register_catalog("my_catalog".to_string(), catalog);
    }

    fn make_table(batches: usize, columns: Vec<(&str, DataType, Box<dyn Fn(usize) -> ArrayRef>)>) -> StreamingTable {
        let schema = Arc::new(Schema::new(
            columns.iter()
                .map(|(name, data_type, _)| Field::new(*name, data_type.clone(), true))
                .collect::<Vec<_>>(),
        ));

        let data = (0..batches).into_iter()
            .map(|i| {
                RecordBatch::try_new(
                    schema.clone(),
                    columns.iter()
                        .map(|(_, _, f)| f(i))
                        .collect(),
                ).unwrap()
            })
            .collect::<Vec<_>>();
        let base_data = StaticPartitionStream {
            schema: schema.clone(),
            data,
        };
        StreamingTable::try_new(schema, vec![Arc::new(base_data)]).unwrap()
    }

    fn make_table_with_fixed_row_count(batches: usize, row_count: usize, columns: Vec<(&str, DataType, Box<dyn Fn(usize) -> ArrayRef>)>) -> StaticTable {
        let schema = Arc::new(Schema::new(
            columns.iter()
                .map(|(name, data_type, _)| Field::new(*name, data_type.clone(), true))
                .collect::<Vec<_>>(),
        ));

        let data = (0..batches).into_iter()
            .map(|i| {
                RecordBatch::try_new(
                    schema.clone(),
                    columns.iter()
                        .map(|(_, _, f)| f(i))
                        .collect(),
                ).unwrap()
            })
            .collect::<Vec<_>>();
        StaticTable::new_with_fixed_row_count(schema, data, row_count)
    }

    fn make_table_with_statistics(batches: usize, columns: Vec<(&str, DataType, Box<dyn Fn(usize) -> ArrayRef>)>) -> StaticTable {
        let schema = Arc::new(Schema::new(
            columns.iter()
                .map(|(name, data_type, _)| Field::new(*name, data_type.clone(), true))
                .collect::<Vec<_>>(),
        ));

        let data = (0..batches).into_iter()
            .map(|i| {
                RecordBatch::try_new(
                    schema.clone(),
                    columns.iter()
                        .map(|(_, _, f)| f(i))
                        .collect(),
                ).unwrap()
            })
            .collect::<Vec<_>>();
        StaticTable::new(schema, data)
    }

    fn make_table_with_fixed_statistics(batches: usize, statistics: Statistics, columns: Vec<(&str, DataType, Box<dyn Fn(usize) -> ArrayRef>)>) -> StaticTable {
        let schema = Arc::new(Schema::new(
            columns.iter()
                .map(|(name, data_type, _)| Field::new(*name, data_type.clone(), true))
                .collect::<Vec<_>>(),
        ));

        let data = (0..batches).into_iter()
            .map(|i| {
                RecordBatch::try_new(
                    schema.clone(),
                    columns.iter()
                        .map(|(_, _, f)| f(i))
                        .collect(),
                ).unwrap()
            })
            .collect::<Vec<_>>();
        StaticTable::new_with_fixed_statistics(schema, data, statistics)
    }

    fn make_record_batch(columns: Vec<(&str, DataType, ArrayRef)>) -> RecordBatch {
        let schema = Arc::new(Schema::new(
            columns.iter()
                .map(|(name, data_type, _)| Field::new(*name, data_type.clone(), true))
                .collect::<Vec<_>>(),
        ));
        let data = RecordBatch::try_new(
                    schema.clone(),
                    columns.into_iter()
                        .map(|(_, _, f)| f)
                        .collect(),
                ).unwrap();
        data
    }

    // fn make_int_array(min: i32, max: i32) -> Int32Array {
    //     Int32Array::from((min..max).collect::<Vec<_>>())
    // }

    // fn make_string_constant_array(value: String, count: i32) -> StringArray {
    //     StringArray::from((0..count).map(|_| value.clone()).collect::<Vec<_>>())
    // }

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
