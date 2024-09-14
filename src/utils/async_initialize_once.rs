use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::OnceCell;
use crate::utils::once_notify::OnceNotify;

pub struct AsyncInitializeOnce<T> {
    started: AtomicBool,
    complete: OnceNotify,
    value: OnceCell<T>,
}

impl <T> AsyncInitializeOnce<T> {
    pub fn new() -> Self {
        Self {
            started: AtomicBool::new(false),
            complete: OnceNotify::new(),
            value: OnceCell::new(),
        }
    }

    /// Ensures that an operation is only run once, all subsequent calls to the method will wait
    /// for the first one to complete. The first call to this method will return an Ok result
    /// containing a future that evaluates the given operation and returns the result. All
    /// subsequent calls will return an Err future that will wait for the result to be ready.
    pub fn run_once<F, Fut>(&self, operation: F) -> Result<impl Future<Output=&T>, impl Future<Output=&T>>
        where
            F: FnOnce() -> Fut,
            Fut: Future<Output=T>,
    {
        self.run_first(operation).ok_or_else(|| self.get())
    }

    /// Ensures that an operation is only run once, all subsequent calls to the method will wait
    /// for the first one to complete. The first call to this method will return an Ok result
    /// containing a future that evaluates the given operation and returns the result. All
    /// subsequent calls will return an Err future that will wait for the result to be ready.
    pub fn run_first<F, Fut>(&self, operation: F) -> Option<impl Future<Output=&T>>
        where
            F: FnOnce() -> Fut,
            Fut: Future<Output=T>,
    {
        // Check if the operation has already started
        // TODO these atomic operations could probably use a looser ordering
        match self.started.compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst) {
            // If successful, we are the first thread to attempt to perform the operation
            Ok(_) => Some(async move {
                let value = self.value.get_or_init(operation).await;

                // Trigger the notification
                self.complete.notify();

                // Return the value wrapped in an immediate future
                value
            }),
            Err(_) => None
        }
    }

    pub async fn get(&self) -> &T {
        // Return a future that can be polled to wait for the operation to complete
        self.complete.wait().await;

        self.value.get()
            .expect("Complete notification was triggered without setting the value")
    }

    pub fn into_inner(self) -> Option<T> {
        self.value.into_inner()
    }
}
