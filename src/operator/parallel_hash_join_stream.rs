use std::fmt::Debug;
use std::pin::Pin;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::SendableRecordBatchStream;
use datafusion_common::{DataFusionError, JoinType};
use datafusion_physical_expr::PhysicalExprRef;
use crate::operator::build_implementation::BuildImplementation;
use crate::operator::lookup_consumers::IndexLookupConsumer;

use crate::operator::probe_lookup_implementation::probe_lookup_implementation::ProbeLookupStreamImplementation;
use crate::parse_sql::JoinReplacement;
use crate::utils::future_to_record_batch_stream::future_to_record_batch_stream;
use crate::utils::index_lookup::IndexLookup;
use crate::utils::plain_record_batch_stream::PlainRecordBatchStream;

struct PerformProbeLookup {
    output_schema: SchemaRef,
    left: SendableRecordBatchStream,
    probe_expressions: Vec<PhysicalExprRef>,
    probe_stream_lookup: Arc<ProbeLookupStreamImplementation>,
}

impl PerformProbeLookup {
    pub fn new(output_schema: SchemaRef, left: SendableRecordBatchStream, probe_expressions: Vec<PhysicalExprRef>, probe_stream_lookup: Arc<ProbeLookupStreamImplementation>) -> Self {
        Self { output_schema, left, probe_expressions, probe_stream_lookup }
    }
}

impl IndexLookupConsumer for PerformProbeLookup {
    type R = Result<Pin<Box<dyn PlainRecordBatchStream<Item=Result<RecordBatch, DataFusionError>>>>, DataFusionError>;

    fn call<Lookup>(
        self,
        index_lookup: Lookup,
        build_side_records: RecordBatch,
    ) -> Self::R
        where Lookup: IndexLookup<u64> + Sync + Send + 'static {
        let result = self.probe_stream_lookup.streaming_probe_lookup(
            self.output_schema,
            self.left,
            self.probe_expressions,
            build_side_records,
            index_lookup,
        );
        match result {
            Ok(stream) => Ok(Box::pin(stream)),
            Err(err) => Err(err),
        }
    }
}

#[derive(Debug)]
pub struct ParallelHashJoinStream {
    build_implementation: Arc<BuildImplementation>,
    probe_lookup_implementation: Arc<ProbeLookupStreamImplementation>,
}

impl ParallelHashJoinStream {
    pub fn new(parallelism: usize, build_implementation_version: JoinReplacement, join_type: JoinType) -> Self {
        Self {
            // Create the build implementation based on the chosen replacement
            build_implementation: Arc::new(BuildImplementation::new(build_implementation_version, parallelism)),
            // Create the probe lookup implementation based on the join type
            probe_lookup_implementation: Arc::new(ProbeLookupStreamImplementation::new(join_type, parallelism)),
        }
    }

    pub fn perform_streaming_join(
        &self,
        partition: usize,
        left: SendableRecordBatchStream,
        right: SendableRecordBatchStream,
        on: Vec<(PhysicalExprRef, PhysicalExprRef)>,
    ) -> SendableRecordBatchStream {
        let output_schema = self.probe_lookup_implementation.schema(left.schema(), right.schema());
        let probe_lookup_implementation = Arc::clone(&self.probe_lookup_implementation);
        let build_implementation = Arc::clone(&self.build_implementation);

        future_to_record_batch_stream(
            output_schema.clone(),
            async move {
                let (probe_expressions, build_expressions): (Vec<_>, Vec<_>) =
                    on.into_iter().unzip();
                let probe_lookup = PerformProbeLookup::new(
                    output_schema,
                    left,
                    probe_expressions,
                    probe_lookup_implementation,
                );
                let stream = build_implementation.build_right_side(
                    partition,
                    right,
                    &build_expressions,
                    probe_lookup
                ).await??;
                Ok(stream)
            }
        )
    }
}
