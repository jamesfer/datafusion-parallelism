use datafusion::arrow::datatypes::SchemaRef;
use datafusion::execution::SendableRecordBatchStream;
use datafusion_common::DataFusionError;
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use futures::TryFutureExt;
use futures_core::TryFuture;

use crate::utils::plain_record_batch_stream::PlainRecordBatchStream;

pub fn future_to_record_batch_stream<F, Ok>(
    schema: SchemaRef,
    future: F,
) -> SendableRecordBatchStream
    where
        F: TryFuture<Ok = Ok, Error = DataFusionError> + Send + 'static,
        Ok: PlainRecordBatchStream
{
    Box::pin(
        RecordBatchStreamAdapter::new(
            schema,
            future.try_flatten_stream(),
        ),
    )
}
