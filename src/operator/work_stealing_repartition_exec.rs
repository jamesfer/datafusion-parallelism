use std::any::Any;
use std::collections::VecDeque;
use std::fmt::{Debug, Formatter};
use std::future::Future;
use std::ops::DerefMut;
use std::pin::{pin, Pin};
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::task::{ready, Context, Poll};
use std::time::{Duration, SystemTime};
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
use tokio::time::timeout;
use crate::utils::abort_on_drop::AbortOnDrop;

async fn consume_input(
    id: usize,
    partition: usize,
    mut stream: SendableRecordBatchStream,
    destination: flume::Sender<RecordBatch>,
) -> Result<(), DataFusionError> {
    while let Some(batch) = stream.next().await {
        destination.send_async(batch?).await
            .map_err(|_| DataFusionError::Internal("Failed to send batch to internal repartition queue".to_string()))?;
    }
    Ok(())
}

fn make_flume_stream<T>(flume: flume::Receiver<T>, id: usize, partition: usize) -> impl Stream<Item=T> {
    futures::stream::unfold(flume, |flume| async {
        match flume.recv_async().await {
            // Return the value and continue unfolding
            Ok(value) => Some((value, flume)),
            // Sender closed
            Err(_) => None,
        }
    })
}

fn make_stealer_stream(steal_queues: Vec<flume::Receiver<RecordBatch>>) -> impl Stream<Item=Vec<RecordBatch>> {
    let steal_queues = steal_queues.into_iter().collect::<VecDeque<_>>();
    futures::stream::unfold((SystemTime::now(), steal_queues), |mut input| async move {
        let (previous_steal_time, mut steal_queues) = input;

        // The outer loop will continue until we find something to steal, or we have exhausted all
        // queues
        while !steal_queues.is_empty() {
            // The inner loop will attempt to steal once from each queue
            let initial_steal_queues = steal_queues.len();
            let mut steal_attempts = 0;
            while steal_attempts < initial_steal_queues {
                // Get the next available worker
                let steal_queue = match steal_queues.pop_front() {
                    Some(queue) => queue,
                    // No more workers to steal from, finish the stream
                    None => return None,
                };

                steal_attempts += 1;
                let (batches, state) = pop_many(&steal_queue, 5);

                // If the receiver is still alive, add them back to the queue
                if state == ReceiverState::Alive {
                    steal_queues.push_back(steal_queue);
                }

                if batches.len() > 0 {
                    return Some((batches, (SystemTime::now(), steal_queues)))
                }
            }

            // If we have tried each queue at least once, sleep for a small amount of time, and then
            // try again
            // println!("Stealing failed, sleeping. Attempts: {}, remaining queues: {}", steal_attempts, steal_queues.len());
            // tokio::time::sleep(Duration::from_millis(50)).await;
            tokio::task::yield_now().await;
        }

        None
    })
}

#[derive(Debug, PartialEq, Eq)]
enum ReceiverState {
    Alive,
    Disconnected,
}

fn pop_many(receiver: &flume::Receiver<RecordBatch>, n: usize) -> (Vec<RecordBatch>, ReceiverState) {
    let mut batches = Vec::with_capacity(n);
    let mut receiver_state = ReceiverState::Alive;
    for _ in 0..n {
        match receiver.try_recv() {
            Ok(batch) => batches.push(batch),
            // Exit the loop early if the queue is empty
            Err(TryRecvError::Empty) => break,
            Err(TryRecvError::Disconnected) => {
                receiver_state = ReceiverState::Disconnected;
                break;
            },
        }
    }

    (batches, receiver_state)
}

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
                        match timeout(Duration::from_secs(20), consume_input(id, partition, input_stream, local_sender)).await {
                            Ok(Ok(_)) => {}
                            Ok(Err(err)) => {
                                eprintln!("Error consuming input: {}, (id {}, partition {})", err, id, partition);
                            },
                            Err(_) => {
                                eprintln!("Possible deadlock while consuming input in work-stealing repartition, (id {}, partition {})", id, partition);
                            }
                        };
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
                        // println!("Output stream starting (id {}, partition {})", id, partition);
                        futures::stream::empty()
                    }).flatten()
                        .chain(make_flume_stream(local_receiver, id, partition))
                        .chain(futures::stream::once(async move {
                            // println!("Local worker complete (id {}, partition {})", id, partition);
                            futures::stream::empty()
                        }).flatten())
                        .chain(
                            make_stealer_stream(steal_queues)
                                .flat_map(|vec| futures::stream::iter(vec))
                        )
                        .chain(futures::stream::once(async move {
                            // println!("Stealing stream complete (id {}, partition {})", id, partition);
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
