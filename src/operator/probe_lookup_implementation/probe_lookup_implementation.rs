use std::sync::Arc;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::execution::SendableRecordBatchStream;
use datafusion_common::{DataFusionError, JoinType};
use datafusion::error::Result;
use datafusion_physical_expr::PhysicalExprRef;
use datafusion_physical_plan::joins::utils::{build_join_schema, JoinFilter};
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use crate::operator::probe_lookup_implementation::full::FullJoinProbeLookupStream;
use crate::operator::probe_lookup_implementation::inner::InnerJoinProbeLookupStream;
use crate::operator::probe_lookup_implementation::left_anti::LeftAntiProbeLookupStream;
use crate::operator::probe_lookup_implementation::left_outer::LeftOuterProbeLookupStream;
use crate::operator::probe_lookup_implementation::left_semi::LeftSemiProbeLookupStream;
use crate::operator::probe_lookup_implementation::right_anti::RightAntiProbeLookupStream;
use crate::operator::probe_lookup_implementation::right_outer::RightOuterProbeLookupStream;
use crate::operator::probe_lookup_implementation::right_semi::RightSemiProbeLookupStream;
use crate::utils::index_lookup::IndexLookup;
use crate::utils::plain_record_batch_stream::SendablePlainRecordBatchStream;

#[derive(Debug)]
pub enum ProbeLookupStreamImplementation {
    InnerJoin(InnerJoinProbeLookupStream),
    FullJoin(FullJoinProbeLookupStream),
    LeftOuter(LeftOuterProbeLookupStream),
    LeftSemi(LeftSemiProbeLookupStream),
    LeftAnti(LeftAntiProbeLookupStream),
    RightAnti(RightAntiProbeLookupStream),
    RightOuter(RightOuterProbeLookupStream),
    RightSemi(RightSemiProbeLookupStream),
}

impl ProbeLookupStreamImplementation {
    pub fn join_type_is_supported(join_type: &JoinType) -> bool {
        match join_type {
            JoinType::Inner => true,
            JoinType::Full => true,
            JoinType::LeftSemi => true,
            JoinType::LeftAnti => true,
            JoinType::Right => true,
            JoinType::RightSemi => true,
            JoinType::RightAnti => true,
            JoinType::Left => true,
        }
    }

    pub fn new(join_type: JoinType, parallelism: usize) -> ProbeLookupStreamImplementation {
        match join_type {
            JoinType::Inner => ProbeLookupStreamImplementation::InnerJoin(
                InnerJoinProbeLookupStream::new(parallelism),
            ),
            JoinType::Full => ProbeLookupStreamImplementation::FullJoin(
                FullJoinProbeLookupStream::new(parallelism),
            ),
            JoinType::Left => ProbeLookupStreamImplementation::LeftOuter(
                LeftOuterProbeLookupStream::new(parallelism),
            ),
            JoinType::LeftSemi => ProbeLookupStreamImplementation::LeftSemi(
                LeftSemiProbeLookupStream::new(parallelism),
            ),
            JoinType::LeftAnti => ProbeLookupStreamImplementation::LeftAnti(
                LeftAntiProbeLookupStream::new(parallelism),
            ),
            JoinType::Right => ProbeLookupStreamImplementation::RightOuter(
                RightOuterProbeLookupStream::new(parallelism),
            ),
            JoinType::RightSemi => ProbeLookupStreamImplementation::RightSemi(
                RightSemiProbeLookupStream::new(parallelism),
            ),
            JoinType::RightAnti => ProbeLookupStreamImplementation::RightAnti(
                RightAntiProbeLookupStream::new(parallelism),
            ),

            // TODO
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
            ProbeLookupStreamImplementation::LeftOuter(_) => JoinType::Left,
            ProbeLookupStreamImplementation::LeftSemi(_) => JoinType::LeftSemi,
            ProbeLookupStreamImplementation::LeftAnti(_) => JoinType::LeftAnti,
            ProbeLookupStreamImplementation::RightAnti(_) => JoinType::RightAnti,
            ProbeLookupStreamImplementation::RightOuter(_) => JoinType::Right,
            ProbeLookupStreamImplementation::RightSemi(_) => JoinType::RightSemi,
        };
        let (schema, _) = build_join_schema(&left, &right, &join_type);
        Arc::new(schema)
    }

    pub fn streaming_probe_lookup<Lookup>(
        &self,
        output_schema: SchemaRef,
        probe_stream: SendableRecordBatchStream,
        probe_id: Option<usize>,
        probe_expressions: Vec<PhysicalExprRef>,
        build_expressions: Vec<PhysicalExprRef>,
        filter: Option<JoinFilter>,
        build_side_records: RecordBatch,
        read_only_join_map: Lookup,
    ) -> Result<SendableRecordBatchStream>
        where Lookup: IndexLookup<u64> + Sync + Send + 'static {
        Ok(match self {
            ProbeLookupStreamImplementation::InnerJoin(implementation) => {
                let stream = implementation.streaming_probe_lookup(
                    output_schema.clone(),
                    probe_stream,
                    probe_expressions,
                    build_expressions,
                    filter,
                    build_side_records,
                    read_only_join_map,
                )?;
                Box::pin(RecordBatchStreamAdapter::new(output_schema, stream))
            },
            ProbeLookupStreamImplementation::FullJoin(implementation) => {
                let stream = implementation.streaming_probe_lookup(
                    output_schema.clone(),
                    probe_stream,
                    probe_expressions,
                    build_expressions,
                    filter,
                    build_side_records,
                    read_only_join_map,
                )?;
                Box::pin(RecordBatchStreamAdapter::new(output_schema, stream))
            },
            ProbeLookupStreamImplementation::LeftOuter(implementation) => {
                let stream = implementation.streaming_probe_lookup(
                    output_schema.clone(),
                    probe_stream,
                    probe_expressions,
                    build_expressions,
                    filter,
                    build_side_records,
                    read_only_join_map,
                )?;
                Box::pin(RecordBatchStreamAdapter::new(output_schema, stream))
            },
            ProbeLookupStreamImplementation::LeftSemi(implementation) => {
                let stream = implementation.streaming_probe_lookup(
                    output_schema.clone(),
                    probe_stream,
                    probe_expressions,
                    build_expressions,
                    filter,
                    build_side_records,
                    read_only_join_map,
                )?;
                Box::pin(RecordBatchStreamAdapter::new(output_schema, stream))
            },
            ProbeLookupStreamImplementation::LeftAnti(implementation) => {
                let stream = implementation.streaming_probe_lookup(
                    output_schema.clone(),
                    probe_stream,
                    probe_expressions,
                    build_expressions,
                    filter,
                    build_side_records,
                    read_only_join_map,
                )?;
                Box::pin(RecordBatchStreamAdapter::new(output_schema, stream))
            },
            ProbeLookupStreamImplementation::RightAnti(implementation) => {
                let stream = implementation.streaming_probe_lookup(
                    output_schema.clone(),
                    probe_stream,
                    probe_expressions,
                    build_expressions,
                    filter,
                    build_side_records,
                    read_only_join_map,
                )?;
                Box::pin(RecordBatchStreamAdapter::new(output_schema, stream))
            },
            ProbeLookupStreamImplementation::RightOuter(implementation) => {
                let stream = implementation.streaming_probe_lookup(
                    output_schema.clone(),
                    probe_stream,
                    probe_expressions,
                    build_expressions,
                    filter,
                    build_side_records,
                    read_only_join_map,
                )?;
                Box::pin(RecordBatchStreamAdapter::new(output_schema, stream))
            },
            ProbeLookupStreamImplementation::RightSemi(implementation) => {
                let stream = implementation.streaming_probe_lookup(
                    output_schema.clone(),
                    probe_stream,
                    probe_expressions,
                    build_expressions,
                    filter,
                    build_side_records,
                    read_only_join_map,
                )?;
                Box::pin(RecordBatchStreamAdapter::new(output_schema, stream))
            },
        })
    }
}
