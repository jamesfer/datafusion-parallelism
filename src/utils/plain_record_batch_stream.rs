use std::pin::Pin;
use datafusion::arrow::array::RecordBatch;
use datafusion_common::DataFusionError;
use futures_core::Stream;

pub trait PlainRecordBatchStream: Stream<Item=Result<RecordBatch, DataFusionError>> + Send {}

impl <T> PlainRecordBatchStream for T
    where T: Stream<Item=Result<RecordBatch, DataFusionError>> + Send {}

pub type SendablePlainRecordBatchStream =
    Pin<Box<dyn PlainRecordBatchStream<Item=Result<RecordBatch, DataFusionError>>>>;
