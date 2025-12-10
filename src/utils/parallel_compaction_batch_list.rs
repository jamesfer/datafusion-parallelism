use std::time::{Duration, SystemTime};
use async_std::prelude::FutureExt;
use datafusion::arrow;
use datafusion::arrow::array::{ArrayRef, RecordBatch};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion_common::DataFusionError;
use tokio::time::timeout;

pub struct ParallelCompactionBatchList {
    schema: SchemaRef,
    receivers: flume::Receiver<(usize, flume::Receiver<(usize, ArrayRef)>)>,
    senders: Vec<flume::Sender<(usize, ArrayRef)>>,
    completed_columns_sender: tokio::sync::broadcast::Sender<(usize, ArrayRef)>,
    completed_columns: tokio::sync::broadcast::Receiver<(usize, ArrayRef)>,
}

impl ParallelCompactionBatchList {
    pub fn new_copies(schema: SchemaRef, copies: usize) -> Vec<Self> {
        let (senders, receivers) = schema.fields().iter()
            .map(|field| flume::unbounded())
            .unzip::<_, _, Vec<_>, Vec<_>>();

        let (receivers_sender, receivers_receiver) = flume::bounded::<(usize, flume::Receiver<(usize, ArrayRef)>)>(schema.fields.len());
        for (column_index, receiver) in receivers.into_iter().enumerate() {
            receivers_sender.send((column_index, receiver)).unwrap();
        }
        drop(receivers_sender);

        let completed_columns_sender = tokio::sync::broadcast::Sender::new(schema.fields.len());

        (0..copies).into_iter()
            .map(|_| Self {
                schema: schema.clone(),
                receivers: receivers_receiver.clone(),
                senders: senders.clone(),
                completed_columns_sender: completed_columns_sender.clone(),
                completed_columns: completed_columns_sender.subscribe(),
            })
            .collect()
    }

    pub fn append(&self, index: usize, batch: RecordBatch) {
        batch.columns().iter()
            .zip(self.senders.iter())
            .for_each(|(column, sender)| {
                sender.send((index, column.clone())).unwrap();
            });
    }

    pub async fn compact(mut self) -> Result<RecordBatch, DataFusionError> {
        // Drop the senders since we are completely finished with them
        drop(self.senders);

        // Try to participate in the compaction
        loop {
            let result = timeout(Duration::from_secs(20), self.receivers.recv_async())
                .await
                .map_err(|_| DataFusionError::Internal("Possible deadlock while waiting for column to compact".to_string()))?;
            match result {
                Ok((index, column_receiver)) => {
                    let start = SystemTime::now();

                    // Compact the columns in the receiver
                    let mut unsorted_chunks = Vec::new();
                    loop {
                        let result = timeout(Duration::from_secs(20), column_receiver.recv_async())
                            .await
                            .map_err(|_| DataFusionError::Internal("Possible deadlock while waiting for contents of column for compaction".to_string()))?;
                        match result {
                            Ok((chunk_index, column_chunk)) => {
                                unsorted_chunks.push((chunk_index, column_chunk));
                            }
                            Err(_disconnected) => {
                                break;
                            }
                        }
                    }
                    unsorted_chunks.sort_by_key(|(chunk_index, _)| *chunk_index);
                    let columns = unsorted_chunks.iter().map(|(_, column_chunk)| column_chunk.as_ref()).collect::<Vec<_>>();

                    let compact_start = SystemTime::now();
                    let compacted = arrow::compute::concat(columns.as_slice())?;
                    let end = SystemTime::now();
                    let duration = end.duration_since(start).unwrap();
                    let compacted_duration = end.duration_since(compact_start).unwrap();
                    // println!("Compacted column {} in {:?} (compaction {:?}) from {} chunks, rows {}, size {}", self.schema.field(index), duration, compacted_duration, unsorted_chunks.len(), compacted.len(), compacted.get_array_memory_size());

                    self.completed_columns_sender.send((index, compacted)).unwrap();
                }
                Err(_disconnected) => {
                    break;
                }
            }
        }

        // Drop the completed columns sender as no more columns need to be compacted
        drop(self.completed_columns_sender);

        // Collect the compacted columns into a record batch
        let mut unsorted_columns = Vec::with_capacity(self.schema.fields().len());
        loop {
            let result = timeout(Duration::from_secs(20), self.completed_columns.recv())
                .await
                .map_err(|_| DataFusionError::Internal("Possible deadlock while waiting for all completed columns".to_string()))?;
            match result {
                Ok((index, column)) => {
                    unsorted_columns.push((index, column));
                }
                Err(_disconnected) => {
                    break;
                }
            }
        }

        unsorted_columns.sort_by_key(|(index, _)| *index);
        let columns = unsorted_columns.into_iter().map(|(_, column)| column).collect::<Vec<_>>();
        Ok(RecordBatch::try_new(self.schema.clone(), columns)?)
    }
}
