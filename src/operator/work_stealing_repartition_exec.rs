use std::any::Any;
use std::collections::VecDeque;
use std::fmt::{Debug, Formatter};
use std::future::Future;
use std::ops::DerefMut;
use std::pin::{pin, Pin};
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::task::{ready, Context, Poll};
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion_common::DataFusionError;
use datafusion_physical_expr::{EquivalenceProperties, Partitioning};
use datafusion_physical_plan::{DisplayAs, DisplayFormatType, ExecutionMode, ExecutionPlan, ExecutionPlanProperties, PlanProperties};
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use flume::{RecvError, SendError, TryRecvError};
use futures::{FutureExt, StreamExt, TryFutureExt, TryStreamExt};
use futures::task::SpawnExt;
use futures_core::Stream;
use tokio::sync::{Mutex, OnceCell, Semaphore};
use tokio::task::{JoinHandle, JoinSet, LocalSet};
use crate::utils::abort_on_drop::AbortOnDrop;
use crate::utils::local_runtime_reference::{get_local_runtime, NotifyHandle};
// struct LimitedWorker {
//     receiver: flume::Receiver<RecordBatch>,
//     sender: flume::Sender<RecordBatch>,
//     limit: usize,
// }
//
// impl LimitedWorker {
//     async fn push(&self, batch: RecordBatch) -> Result<(), SendError<T>> {
//         self.sender.send_async(batch)
//             .await?;
//         Ok(())
//     }
//
//     async fn pop(&self) -> Result<RecordBatch, RecvError> {
//         let batch = self.receiver.recv_async()
//             .await?;
//         Ok(batch)
//     }
//
//     fn pop_many(&self, n: usize) -> Result<Vec<RecordBatch>, RecvError> {
//         let mut batches = Vec::with_capacity(n);
//         for _ in 0..n {
//             match self.receiver.try_recv() {
//                 Ok(batch) => batches.push(batch),
//                 // Exit the loop early if the queue is empty
//                 Err(TryRecvError::Empty) => break,
//                 Err(TryRecvError::Disconnected) => {
//                     if batches.is_empty() {
//                         return Err(RecvError::Disconnected);
//                     } else {
//                         break;
//                     }
//                 },
//             }
//         }
//         Ok(batches)
//     }
// }

struct LimitedWorkerStream {
    worker: flume::Receiver<RecordBatch>,
    next_future: Option<Pin<Box<dyn Future<Output=Result<RecordBatch, RecvError>> + Send + Sync>>>,
}

// impl LimitedWorkerStream {
//     fn new(worker: flume::Receiver<RecordBatch>) -> Self {
//         Self {
//             worker,
//             next_future: None,
//         }
//     }
// }
//
// impl Stream for LimitedWorkerStream {
//     type Item = RecordBatch;
//
//     fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
//         println!("Poll next");
//
//
//         let mut fut = match self.next_future.take() {
//             None => Box::pin(self.worker.recv_async()),
//             Some(fut) => fut
//         };
//
//         // Try to poll the future
//         let value = match fut.as_mut().poll(cx) {
//             Poll::Ready(value) => value,
//             Poll::Pending => {
//                 // Save the current future to be polled later
//                 self.next_future = Some(fut);
//                 return Poll::Pending;
//             }
//         };
//
//         match value {
//             Ok(value) => {
//                 println!("Worker received batch");
//                 Poll::Ready(Some(value))
//             },
//             Err(_) => {
//                 println!("Worker stream reached recv error");
//                 Poll::Ready(None)
//             },
//         }
//         //
//         // loop {
//         //     let mut fut = tokio::time::timeout(std::time::Duration::from_secs(1), fut);
//         //     let value = match pin!(fut).poll(cx) {
//         //         Poll::Ready(v) => v,
//         //         Poll::Pending => {
//         //             println!("Local stream not ready, senders: {}", self.worker.sender_count());
//         //             return Poll::Pending;
//         //         },
//         //     };
//         //     match value {
//         //         Ok(Ok(value)) => {
//         //             println!("Worker received batch {}", self.i);
//         //             return Poll::Ready(Some(value));
//         //         },
//         //         Err(_) => {
//         //             println!("timeout waiting {}", self.i);
//         //         },
//         //         Ok(Err(_)) => {
//         //             println!("Worker stream reached recv error");
//         //             return Poll::Ready(None);
//         //         },
//         //     }
//         // }
//     }
// }

fn make_flume_stream<T>(flume: flume::Receiver<T>) -> impl Stream<Item=T> {
    futures::stream::unfold(flume, |flume| async {
        match flume.recv_async().await {
            Ok(value) => Some((value, flume)),
            // Stream finished
            Err(_) => None
        }
    })
}

fn make_stealer_stream(steal_queues: Vec<flume::Receiver<RecordBatch>>) -> impl Stream<Item=Vec<RecordBatch>> {
    let steal_queues = steal_queues.into_iter().collect::<VecDeque<_>>();
    futures::stream::unfold(steal_queues, |mut steal_queues| async {
        // Get the next available worker
        let steal_queue = match steal_queues.pop_front() {
            Some(queue) => queue,
            None => return None,
        };

        match pop_many(&steal_queue, 5) {
            Ok(batches) => {
                // Return the worker to the back of the queue
                steal_queues.push_back(steal_queue);
                Some((batches, steal_queues))
            },
            // If the queue is disconnected, don't add it back to the list, and continue looping
            // looking for things to steal
            Err(_) => Some((vec![], steal_queues)),
        }
    })
}

fn pop_many(receiver: &flume::Receiver<RecordBatch>, n: usize) -> Result<Vec<RecordBatch>, RecvError> {
    let mut batches = Vec::with_capacity(n);
    for _ in 0..n {
        match receiver.try_recv() {
            Ok(batch) => batches.push(batch),
            // Exit the loop early if the queue is empty
            Err(TryRecvError::Empty) => break,
            Err(TryRecvError::Disconnected) => {
                if batches.is_empty() {
                    return Err(RecvError::Disconnected);
                } else {
                    break;
                }
            },
        }
    }
    Ok(batches)
}

// struct StealingStream {
//     steal_queues: VecDeque<flume::Receiver<RecordBatch>>,
// }
//
// impl StealingStream {
//     fn pop_many(receiver: &flume::Receiver<RecordBatch>, n: usize) -> Result<Vec<RecordBatch>, RecvError> {
//         let mut batches = Vec::with_capacity(n);
//         for _ in 0..n {
//             match receiver.try_recv() {
//                 Ok(batch) => batches.push(batch),
//                 // Exit the loop early if the queue is empty
//                 Err(TryRecvError::Empty) => break,
//                 Err(TryRecvError::Disconnected) => {
//                     if batches.is_empty() {
//                         return Err(RecvError::Disconnected);
//                     } else {
//                         break;
//                     }
//                 },
//             }
//         }
//         Ok(batches)
//     }
// }
//
// impl Stream for StealingStream {
//     type Item = Vec<RecordBatch>;
//
//     fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
//         loop {
//             // Get the next available worker
//             let steal_queue = match self.steal_queues.pop_front() {
//                 Some(queue) => queue,
//                 None => return Poll::Ready(None),
//             };
//
//             match Self::pop_many(&steal_queue, 5) {
//                 Ok(batches) => {
//                     // Return the worker to the back of the queue
//                     self.steal_queues.push_back(steal_queue);
//                     return Poll::Ready(Some(batches));
//                 },
//                 // If the queue is empty, don't add it back to the list, and continue looping looking
//                 // for things to steal
//                 Err(_) => drop(steal_queue),
//             }
//         }
//     }
// }

struct WorkStealingState {
    local_sender: flume::Sender<RecordBatch>,
    local_receiver: flume::Receiver<RecordBatch>,
    steal_queues: Vec<flume::Receiver<RecordBatch>>,
}

struct WorkStealingSharedState {
    states: Vec<Option<WorkStealingState>>,
}

type WorkStealingLazyState = Arc<OnceCell<Mutex<WorkStealingSharedState>>>;

pub struct WorkStealingRepartitionExec {
    id: usize,
    input: Arc<dyn ExecutionPlan>,
    properties: PlanProperties,
    state: WorkStealingLazyState,
}

impl WorkStealingRepartitionExec {
    pub fn new(id: usize, input: Arc<dyn ExecutionPlan>) -> Self {
        let properties = Self::compute_properties(input.schema(), &input);
        Self {
            id,
            input,
            properties,
            state: Arc::new(OnceCell::new()),
        }
    }

    pub fn id(&self) -> usize {
        self.id
    }

    fn parallelism(&self) -> usize {
        self.properties().output_partitioning().partition_count()
    }

    fn compute_properties(schema: SchemaRef, input: &Arc<dyn ExecutionPlan>) -> PlanProperties {
        PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::RoundRobinBatch(input.properties().output_partitioning().partition_count()),
            ExecutionMode::Bounded,
        )
    }
}

impl Debug for WorkStealingRepartitionExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("WorkStealingRepartitionExec id={}", self.id))
    }
}

impl DisplayAs for WorkStealingRepartitionExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        f.write_fmt(format_args!("WorkStealingRepartitionExec id={}", self.id))
    }
}

impl ExecutionPlan for WorkStealingRepartitionExec {
    fn name(&self) -> &str {
        "WorkStealingRepartitionExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(self: Arc<Self>, children: Vec<Arc<dyn ExecutionPlan>>) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let input = children[0].clone();
        let properties = WorkStealingRepartitionExec::compute_properties(self.schema().clone(), &input);
        Ok(Arc::new(WorkStealingRepartitionExec {
            id: self.id,
            input,
            properties,
            state: self.state.clone(),
        }))
    }

    fn execute(&self, partition: usize, context: Arc<TaskContext>) -> Result<SendableRecordBatchStream, DataFusionError> {
        let id = self.id;
        let parallelism = self.parallelism();
        let tasks = Arc::new(Mutex::new(vec![]));

        let input_stream = self.input.execute(partition, context)?;
        let input_schema = input_stream.schema();

        let stream = {
            let lazy_state = Arc::clone(&self.state);
            let tasks_clone = Arc::clone(&tasks);

            futures::stream::once(async move {
                let state = lazy_state.get_or_init(|| async {
                    Mutex::new(create_shared_state(parallelism))
                }).await;

                // Extract this partitions queues from the shared state
                let WorkStealingState {
                    local_sender,
                    local_receiver,
                    steal_queues
                } = {
                    let mut shared_state = state.lock().await;
                    shared_state.states[partition].take()
                        .ok_or(DataFusionError::Internal("State for partition already taken".to_string()))
                }?;

                // Start local task to write all the input into a queue. It will be aborted on drop
                {
                    let handle = tokio::task::spawn(async move {
                        // TODO forward errors
                        let _ = consume_input(id, partition, input_stream, local_sender).await;
                        ()
                    });
                    tasks_clone.lock().await.push(AbortOnDrop::new(handle));

                    // // This must be in a block to ensure the local set is not used across an await
                    // let handle = {
                    //     let local_set = LocalSet::new();
                    //     local_set.spawn_local(async move {
                    //         println!("Starting task");
                    //         // TODO forward errors
                    //         let _ = consume_input(id, partition, input_stream, local_sender).await;
                    //         ()
                    //     });
                    //
                    //     get_local_runtime(move |local_runtime| {
                    //         println!("Registered task");
                    //         local_runtime.register(local_set)
                    //     })
                    // };
                    // tasks_clone.lock().await.push(handle);
                }

                // Create a stream of the contents of the local queue
                Ok::<_, DataFusionError>(
                    futures::stream::once(async move {
                        // println!("Output stream starting (id {}) {}", id, partition);
                        futures::stream::empty()
                    }).flatten()
                        .chain(make_flume_stream(local_receiver))
                        .chain(futures::stream::once(async move {
                            // println!("Local worker complete (id {}) {}", id, partition);
                            futures::stream::empty()
                        }).flatten())
                        .chain(
                            make_stealer_stream(steal_queues)
                                .flat_map(|vec| futures::stream::iter(vec))
                        )
                        .chain(futures::stream::once(async move {
                            // println!("Stealing stream complete (id {}) {}", id, partition);
                            futures::stream::empty()
                        }).flatten())
                        .map(|batch| Ok(batch))
                )
            }).try_flatten()
        };

        // println!("Returning stream with tasks");
        Ok(Box::pin(TempStream {
            stream: Box::pin(RecordBatchStreamAdapter::new(input_schema, stream)),
            abort_helper: tasks,
        }))
    }
}

struct PrintOnDrop {
    id: usize,
}

impl Drop for PrintOnDrop {
    fn drop(&mut self) {
        println!("Dropping PrintOnDrop (id {})", self.id);
    }
}

struct TempStream<S> {
    stream: Pin<Box<RecordBatchStreamAdapter<S>>>,
    abort_helper: Arc<Mutex<Vec<AbortOnDrop<()>>>>,
}

impl <S: Stream<Item=Result<RecordBatch, DataFusionError>>> RecordBatchStream for TempStream<S> {
    fn schema(&self) -> SchemaRef {
        self.stream.schema()
    }
}

impl <S: Stream<Item=Result<RecordBatch, DataFusionError>>> Stream for TempStream<S> {
    type Item = Result<RecordBatch, DataFusionError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.stream.as_mut().poll_next(cx)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.stream.size_hint()
    }
}

fn create_shared_state(parallelism: usize) -> WorkStealingSharedState {
    // TODO this creates 1 too many steal queues
    let (local_queues, all_steal_queues): (Vec<_>, Vec<_>) = (0..parallelism).into_iter()
        .map(|_| {
            let (local_sender, local_receiver) = flume::bounded(10);
            let steal_queues = (0..parallelism).into_iter()
                .map(|_| local_receiver.clone())
                .collect::<Vec<_>>();
            ((local_sender, local_receiver), steal_queues)
        })
        .unzip();

    let states = local_queues
        .into_iter()
        .enumerate()
        .map(|(i, (local_sender, local_receiver))| {
            let steal_queues = all_steal_queues.iter()
                .enumerate()
                // Skip the steal queue for the current local queue
                .filter(|(j, _)| i != *j)
                // TODO avoid cloning all the queues here
                .map(|(_, steal_queues)| steal_queues[i].clone())
                .collect::<Vec<_>>();
            Some(WorkStealingState {
                local_sender,
                local_receiver,
                steal_queues,
            })
        })
        .collect::<Vec<_>>();

    WorkStealingSharedState {
        states,
    }
}

async fn consume_input(
    id: usize,
    partition: usize,
    stream: SendableRecordBatchStream,
    destination: flume::Sender<RecordBatch>,
) -> Result<(), DataFusionError> {
    // println!("Consume input starting (id {}) {}", id, partition);
    stream.try_for_each(|batch| async {
        destination.send_async(batch)
            .await
            .map_err(|_| DataFusionError::Internal("Failed to send batch".to_string()))
    })
        .await
        .map_err(|e| DataFusionError::Internal("Failed to write batch to destination".to_string()))
}
