use datafusion::common::Result;
use datafusion_common::internal_datafusion_err;
use flume::TryRecvError;
use futures::StreamExt;
use futures_core::Stream;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

pub struct OwnedState<S, I> {
    source_stream: S,
    active_stealers_count: Arc<AtomicUsize>,
    work_sharing_queue: flume::Sender<I>,
    parallelism: usize,
}

impl <S, I> OwnedState<S, I>
where S: Stream<Item = Result<I>> + Unpin + Send + 'static,
{
    pub async fn get_work(&mut self) -> Result<Option<I>> {
        loop {
            // Wait for the source to produce an item
            let Some(next) = self.source_stream.next().await else {
                return Ok(None)
            };
            let item = next?;

            let active_stealers_count = self.active_stealers_count.load(Ordering::Relaxed);
            let ideal_queue_size = Self::ideal_queued_item_count(active_stealers_count, self.parallelism);

            // If there is not enough work for the others, put it in the queue
            if self.work_sharing_queue.len() < ideal_queue_size {
                self.work_sharing_queue.send(item)
                    .map_err(|_| internal_datafusion_err!("Failed to share item in worker sharing queue because all receivers were dropped"))?;
            } else {
                // Otherwise, we can return it for this thread to process
                return Ok(Some(item));
            }
        }
    }

    fn ideal_queued_item_count(active_stealers_count: usize, parallelism: usize) -> usize {
        // Some random heuristic
        active_stealers_count * parallelism / 2
    }
}

pub struct PrecursorState<I> {
    active_stealers_count: Arc<AtomicUsize>,
    work_sharing_queue: flume::Sender<I>,
    work_stealing_receiver: flume::Receiver<I>,
    parallelism: usize,
}

impl <I> PrecursorState<I> {
    pub fn create(parallelism: usize) -> Vec<PrecursorState<I>> {
        let (sender, receiver) = flume::unbounded();
        let active_stealers_count = Arc::new(AtomicUsize::new(0));
        std::iter::repeat_n((sender, receiver, active_stealers_count), parallelism)
            .map(|(sender, receiver, active_stealers_count)| {
                PrecursorState {
                    active_stealers_count,
                    work_sharing_queue: sender,
                    work_stealing_receiver: receiver,
                    parallelism,
                }
            })
            .collect()
    }


    pub fn with_stream<S>(self, source_stream: S) -> FullState<S, I> {
        FullState {
            owned_state: Some(OwnedState {
                source_stream,
                active_stealers_count: self.active_stealers_count.clone(),
                work_sharing_queue: self.work_sharing_queue,
                parallelism: self.parallelism,
            }),
            active_stealers_count: self.active_stealers_count,
            work_stealing_queue: self.work_stealing_receiver,
        }
    }
}


pub struct FullState<S, I> {
    owned_state: Option<OwnedState<S, I>>,
    active_stealers_count: Arc<AtomicUsize>,
    work_stealing_queue: flume::Receiver<I>,
}

impl <S, I> FullState<S, I>
where S: Stream<Item = Result<I>> + Unpin + Send + 'static,
{
    pub async fn next(&mut self) -> Result<Option<I>> {
        // Try to get some work from our own task first
        if let Some(owned_state) = &mut self.owned_state {
            let our_work = owned_state.get_work().await?;
            match our_work {
                Some(item) => return Ok(Some(item)),
                None => {
                    // If there was no more items in our local task it is finished
                    self.owned_state = None;
                    self.active_stealers_count.fetch_add(1, Ordering::Relaxed);
                }
            }
        };

        match self.work_stealing_queue.recv_async().await {
            Ok(item) => Ok(Some(item)),
            Err(_) => Ok(None),
        }

        // // The outer loop will continue until we find something to steal, or we have exhausted all
        // // queues
        // while !self.work_stealing_queues.is_empty() {
        //     // The inner loop will attempt to steal once from each queue
        //     let mut steal_attempts = self.work_stealing_queues.len();
        //     while steal_attempts > 0 {
        //         // Get the next available worker
        //         let Some(next_queue) = self.work_stealing_queues.pop_front() else {
        //             // No more workers to steal from, finish the stream
        //             return Ok(None);
        //         };
        //
        //         steal_attempts -= 1;
        //         let (batches, maybe_queue) = read_many(next_queue, 5);
        //
        //         // If the receiver is still alive, add them back to the queue
        //         if let Some(steal_queue) = maybe_queue {
        //             self.work_stealing_queues.push_back(steal_queue);
        //         }
        //
        //         if batches.len() > 0 {
        //             return Ok(Some(batches));
        //         }
        //     }
        //
        //     // If we have tried each queue at least once, sleep for a small amount of time, and then
        //     // try again
        //     // println!("Stealing failed, sleeping. Attempts: {}, remaining queues: {}", steal_attempts, steal_queues.len());
        //     tokio::time::sleep(Duration::from_millis(1)).await;
        //     // tokio::task::yield_now().await;
        // }
        //
        // Ok(None)
    }
}

fn read_many<I>(receiver: flume::Receiver<I>, n: usize) -> (Vec<I>, Option<flume::Receiver<I>>) {
    let mut items = Vec::with_capacity(n);
    while items.len() < n {
        match receiver.try_recv() {
            Ok(batch) => items.push(batch),
            // Exit the loop early if the queue is empty
            Err(TryRecvError::Empty) => break,
            // Consume the receiver if it is disconnected
            Err(TryRecvError::Disconnected) => return (items, None),
        }
    }

    (items, Some(receiver))
}
