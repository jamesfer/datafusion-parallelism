use std::future::ready;
use crate::operator::lookup_consumers::{IndexLookupProvider, SimpleIndexLookupProvider};
use crate::operator::version10::lookup_implementation_3::Version10Lookup;
use crate::operator::version10::parallel_join_execution_state::{JoinStateInstance, JoinStateInstances};
use crate::shared::shared::{calculate_hash, evaluate_expressions};
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion_common::DataFusionError;
use datafusion_physical_expr::PhysicalExprRef;
use datafusion_physical_plan::SendableRecordBatchStream;
use futures::{StreamExt, TryStreamExt};

// Version 10 is based of the new hashmap table implementation
#[derive(Debug)]
pub struct Version10 {
    state: JoinStateInstances,
}

impl Version10 {
    pub fn new(parallelism: usize, input_schema: SchemaRef) -> Self {
        Self {
            state: JoinStateInstances::new(parallelism, input_schema),
        }
    }

    pub async fn build_lookup_map(
        &self,
        partition: usize,
        build_side_stream: SendableRecordBatchStream,
        build_expressions: &Vec<PhysicalExprRef>,
    ) -> Result<impl IndexLookupProvider, DataFusionError> {
        let mut state = self.state.take(partition)
            .ok_or(DataFusionError::Internal(format!("State already consumed for partition {}", partition)))?;

        consume_build_side(build_side_stream, &mut state, build_expressions).await?;

        let (build_side_records, version_10_lookup) = compact_join_map(
            state,
        ).await?;

        Ok(SimpleIndexLookupProvider::new(version_10_lookup, build_side_records))
    }
}

async fn consume_build_side(
    build_side_stream: SendableRecordBatchStream,
    join_state: &mut JoinStateInstance,
    build_expressions: &Vec<PhysicalExprRef>,
) -> Result<(), DataFusionError> {
    // Exhaust build side
    build_side_stream.try_for_each(|record_batch| {
        ready(process_input_batch(
            record_batch,
            join_state,
            &build_expressions,
        ))
    }).await
}

fn process_input_batch(
    input: RecordBatch,
    join_state: &mut JoinStateInstance,
    expressions: &Vec<PhysicalExprRef>,
) -> Result<(), DataFusionError> {
    let keys = evaluate_expressions(expressions, &input)?;
    let hashes = calculate_hash(&keys)?;

    join_state.add(hashes, input);

    Ok(())
}

async fn compact_join_map(
    join_state: JoinStateInstance,
) -> Result<(RecordBatch, Version10Lookup), DataFusionError> {
    let (read_only_index_map, overflow_buffer, record_batch) = join_state.compact().await?;

    Ok((record_batch, Version10Lookup::new(read_only_index_map, overflow_buffer.0)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use datafusion::arrow::array::UInt64Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::logical_expr::col;
    use datafusion_common::DFSchema;
    use datafusion_physical_expr::create_physical_expr;
    use datafusion_physical_expr::execution_props::ExecutionProps;
    use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
    use futures::stream::iter;
    use crate::api_utils::{make_int_array, make_string_constant_array};
    use crate::operator::lookup_consumers::IndexLookupBorrower;
    use crate::utils::index_lookup::IndexLookup;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_version10_build_lookup_map() {
        // Create test schema with id and value columns
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Utf8, false),
        ]));

        // Create test data with some duplicates
        let test_data = vec![
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(make_int_array(vec![1, 2, 3])),
                    Arc::new(make_string_constant_array("batch1".to_string(), 3)),
                ],
            ).unwrap(),
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(make_int_array(vec![2, 4, 5])), // 2 is duplicate
                    Arc::new(make_string_constant_array("batch2".to_string(), 3)),
                ],
            ).unwrap(),
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(make_int_array(vec![1, 6, 7])), // 1 is duplicate
                    Arc::new(make_string_constant_array("batch3".to_string(), 3)),
                ],
            ).unwrap(),
        ];

        // Create a stream from the test data
        let stream = Box::pin(RecordBatchStreamAdapter::new(
            schema.clone(),
            iter(test_data).map(|batch| Ok(batch)),
        ));

        // Create physical expression for the "id" column
        let expr = col("id");
        let df_schema = DFSchema::try_from(schema.clone()).unwrap();
        let props = ExecutionProps::new();
        let build_expressions = vec![create_physical_expr(&expr, &df_schema, &props).unwrap()];

        // Create Version10 instance with 1 parallelism
        let version10 = Version10::new(1, schema.clone());

        // Build the lookup map
        let lookup_provider = version10.build_lookup_map(0, stream, &build_expressions).await.unwrap();

        // Test that all expected entries are in the lookup map.
        // Map each value to its expected indexes in the compacted result
        // Batch 1: [1, 2, 3] -> indexes [0, 1, 2]
        // Batch 2: [2, 4, 5] -> indexes [3, 4, 5]  
        // Batch 3: [1, 6, 7] -> indexes [6, 7, 8]
        let expected_mappings = vec![
            (1u64, vec![0, 6]),  // ID 1 appears at indexes 0 and 6 -> offset by +1 -> 1 and 7
            (2u64, vec![1, 3]),  // ID 2 appears at indexes 1 and 3 -> offset by +1 -> 2 and 4
            (3u64, vec![2]),     // ID 3 appears at index 2 -> offset by +1 -> 3
            (4u64, vec![4]),     // ID 4 appears at index 4 -> offset by +1 -> 5
            (5u64, vec![5]),     // ID 5 appears at index 5 -> offset by +1 -> 6
            (6u64, vec![7]),     // ID 6 appears at index 7 -> offset by +1 -> 8
            (7u64, vec![8]),     // ID 7 appears at index 8 -> offset by +1 -> 9
        ];

        for (id, expected_indexes) in expected_mappings {
            let hash = calculate_hash(&vec![Arc::new(UInt64Array::from(vec![id]))]).unwrap()[0];
            let mut lookup_results = lookup_provider.borrow(LookupAsVec(hash));
            lookup_results.reverse();

            assert_eq!(lookup_results, expected_indexes, 
                "ID {} should map to indexes {:?} but found {:?}", 
                id, expected_indexes, lookup_results);
        }

        // Test that a non-existent key returns empty results
        let non_existent_hash = calculate_hash(&vec![Arc::new(UInt64Array::from(vec![999]))]).unwrap()[0];
        let empty_results = lookup_provider.borrow(LookupAsVec(non_existent_hash));
        assert!(empty_results.is_empty(), "Non-existent key should return empty results");
    }

    struct LookupAsVec(u64);
    impl IndexLookupBorrower for LookupAsVec {
        type R<'t> = Vec<usize>;

        fn call<'a, Lookup>(self, index_lookup: &'a Lookup, _record_batch: &'a RecordBatch) -> Self::R<'a>
        where
            Lookup: IndexLookup<u64> + Sync + Send + 'static
        {
            index_lookup.get_iter(&self.0).collect()
        }
    }
}
