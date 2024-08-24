use std::sync::Arc;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::execution::SendableRecordBatchStream;
use datafusion_common::{DataFusionError, JoinType};
use datafusion_physical_expr::PhysicalExprRef;
use datafusion_physical_plan::joins::utils::build_join_schema;
use crate::operator::probe_lookup_implementation::full::FullJoinProbeLookupStream;
use crate::operator::probe_lookup_implementation::inner::InnerJoinProbeLookupStream;
use crate::utils::index_lookup::IndexLookup;
use crate::utils::plain_record_batch_stream::SendablePlainRecordBatchStream;

#[derive(Debug)]
pub enum ProbeLookupStreamImplementation {
    InnerJoin(InnerJoinProbeLookupStream),
    FullJoin(FullJoinProbeLookupStream),
}

impl ProbeLookupStreamImplementation {
    pub fn new(join_type: JoinType, parallelism: usize) -> ProbeLookupStreamImplementation {
        match join_type {
            JoinType::Inner => ProbeLookupStreamImplementation::InnerJoin(
                InnerJoinProbeLookupStream::new(parallelism),
            ),
            JoinType::Full => ProbeLookupStreamImplementation::FullJoin(
                FullJoinProbeLookupStream::new(parallelism),
            ),

            // TODO
            // JoinType::Left => {}
            // JoinType::Right => {}
            // JoinType::Full => {}
            // JoinType::LeftSemi => {}
            // JoinType::RightSemi => {}
            // JoinType::LeftAnti => {}
            // JoinType::RightAnti => {}
            _ => panic!(),
        }
    }

    pub fn schema(
        &self,
        left: SchemaRef,
        right: SchemaRef,
    ) -> SchemaRef {
        let join_type = match self {
            ProbeLookupStreamImplementation::InnerJoin(_) => JoinType::Inner,
            ProbeLookupStreamImplementation::FullJoin(_) => JoinType::Full,
        };
        let (schema, _) = build_join_schema(&left, &right, &join_type);
        Arc::new(schema)
    }

    pub fn streaming_probe_lookup<Lookup>(
        &self,
        output_schema: SchemaRef,
        probe_stream: SendableRecordBatchStream,
        probe_expressions: Vec<PhysicalExprRef>,
        build_side_records: RecordBatch,
        read_only_join_map: Lookup,
    ) -> Result<SendablePlainRecordBatchStream, DataFusionError>
        where Lookup: IndexLookup<u64> + Sync + Send + 'static {
        match self {
            ProbeLookupStreamImplementation::InnerJoin(inner) => inner.streaming_probe_lookup(
                output_schema,
                probe_stream,
                probe_expressions,
                build_side_records,
                read_only_join_map,
            ),
            ProbeLookupStreamImplementation::FullJoin(full) => full.streaming_probe_lookup(
                output_schema,
                probe_stream,
                probe_expressions,
                build_side_records,
                read_only_join_map,
            ),
        }
    }
}
