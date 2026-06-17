use std::fmt::Debug;
use std::pin::Pin;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::error::Result;
use datafusion_common::{DataFusionError, JoinType};
use datafusion_physical_expr::PhysicalExprRef;
use datafusion_physical_plan::joins::utils::JoinFilter;
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use futures::stream::iter;
use futures::StreamExt;
use crate::match_build_implementation;
use crate::operator::build_implementation::{BuildImplementation, BuildVersion, CooperativeBuildVersion};
use crate::operator::lookup_consumers::IndexLookupConsumer;

use crate::operator::probe_lookup_implementation::probe_lookup_implementation::ProbeLookupStreamImplementation;
use crate::parse_sql::JoinReplacement;
use crate::utils::future_to_record_batch_stream::future_to_record_batch_stream;
use crate::utils::index_lookup::IndexLookup;
use crate::utils::plain_record_batch_stream::{PlainRecordBatchStream, SendablePlainRecordBatchStream};

// struct PerformProbeLookup {
//     output_schema: SchemaRef,
//     probe_stream: SendableRecordBatchStream,
//     probe_id: Option<usize>,
//     probe_expressions: Vec<PhysicalExprRef>,
//     probe_stream_lookup: Arc<ProbeLookupStreamImplementation>,
//     build_expressions: Vec<PhysicalExprRef>,
//     filter: Option<JoinFilter>,
// }
//
// impl PerformProbeLookup {
//     pub fn new(
//         output_schema: SchemaRef,
//         probe_stream: SendableRecordBatchStream,
//         probe_id: Option<usize>,
//         probe_expressions: Vec<PhysicalExprRef>,
//         probe_stream_lookup: Arc<ProbeLookupStreamImplementation>,
//         build_expressions: Vec<PhysicalExprRef>,
//         filter: Option<JoinFilter>,
//     ) -> Self {
//         Self { output_schema, probe_stream, probe_id, probe_expressions, probe_stream_lookup, build_expressions, filter }
//     }
// }
//
// impl IndexLookupConsumer for PerformProbeLookup {
//     type R = Result<SendableRecordBatchStream, DataFusionError>;
//
//     fn call<Lookup>(
//         self,
//         index_lookup: Lookup,
//         build_side_records: RecordBatch,
//     ) -> Self::R
//         where Lookup: IndexLookup<u64> + Sync + Send + 'static {
//         self.probe_stream_lookup.streaming_probe_lookup(
//             self.output_schema,
//             self.probe_stream,
//             self.probe_id,
//             self.probe_expressions,
//             self.build_expressions,
//             self.filter,
//             build_side_records,
//             index_lookup,
//         )
//     }
// }

#[derive(Debug)]
pub struct ParallelHashJoinExecutor {
    build_implementation: Arc<BuildImplementation>,
    probe_lookup_implementation: Arc<ProbeLookupStreamImplementation>,
}

impl ParallelHashJoinExecutor {
    pub fn new(parallelism: usize, build_implementation_version: JoinReplacement, join_type: JoinType, input_schema: SchemaRef) -> Self {
        Self {
            // Create the build implementation based on the chosen replacement
            build_implementation: Arc::new(BuildImplementation::new(build_implementation_version, parallelism, input_schema)),
            // Create the probe lookup implementation based on the join type
            probe_lookup_implementation: Arc::new(ProbeLookupStreamImplementation::new(join_type, parallelism)),
        }
    }

    pub fn execute_streaming_join(
        &self,
        partition: usize,
        build_stream: SendableRecordBatchStream,
        probe_stream: SendableRecordBatchStream,
        probe_id: Option<usize>,
        on: Vec<(PhysicalExprRef, PhysicalExprRef)>,
        filter: Option<JoinFilter>,
    ) -> SendableRecordBatchStream {
        let output_schema = self.probe_lookup_implementation.schema(build_stream.schema(), probe_stream.schema());
        let probe_lookup_implementation = Arc::clone(&self.probe_lookup_implementation);
        let build_implementation = Arc::clone(&self.build_implementation);
        let (build_expressions, probe_expressions): (Vec<_>, Vec<_>) = on.into_iter().unzip();

        let fut = {
            let output_schema = output_schema.clone();
            async move {
                match_build_implementation!(
                    with_build_implementation,
                    build_implementation.as_ref(),
                    partition,
                    output_schema,
                    build_stream,
                    probe_stream,
                    build_expressions,
                    probe_expressions,
                    filter,
                    probe_id,
                    probe_lookup_implementation
                )
            }
        };

        future_to_record_batch_stream(output_schema, fut)
    }
}

async fn with_build_implementation<L>(
    build_implementation: &impl CooperativeBuildVersion<Map=L>,
    partition: usize,
    output_schema: SchemaRef,
    build_stream: SendableRecordBatchStream,
    mut probe_stream: SendableRecordBatchStream,
    build_expressions: Vec<PhysicalExprRef>,
    probe_expressions: Vec<PhysicalExprRef>,
    filter: Option<JoinFilter>,
    probe_id: Option<usize>,
    probe_stream_lookup: Arc<ProbeLookupStreamImplementation>
) -> Result<SendableRecordBatchStream>
where L: IndexLookup<u64> + Sync + Send + 'static
{
    let waiting = build_implementation.build_lookup_map(partition, build_stream, &build_expressions).await?;

    // We have built as much of the lookup map as we can, now we are waiting on the other threads
    // to finish. In the meantime, we can start the probe side of the join.
    let first_probe_item = probe_stream.next().await;

    // We can't actually do anything with the probe item until the build side is finished, so now
    // we have to wait for it.
    let (lookup_map, build_side_records) = waiting.await?;

    // Now we can start the probe side for real, but we need to remember to include the first item
    let probe_stream = match first_probe_item {
        None => probe_stream,
        Some(item) => Box::pin(RecordBatchStreamAdapter::new(
            output_schema.clone(),
            iter([item]).chain(probe_stream),
        )),
    };
    probe_stream_lookup.streaming_probe_lookup(
        output_schema,
        probe_stream,
        probe_id,
        probe_expressions,
        build_expressions,
        filter,
        build_side_records,
        lookup_map,
    )
}
