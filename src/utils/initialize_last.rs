use std::cell::{OnceCell, UnsafeCell};
use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use crate::utils::once_notify::OnceNotify;

pub struct InitializeLast<T> {
    notify: OnceNotify,
    parallelism: usize,
    counter: AtomicUsize,
    values: UnsafeCell<Option<Vec<Option<T>>>>
}

unsafe impl <T: Send> Send for InitializeLast<T> {}
unsafe impl <T: Sync> Sync for InitializeLast<T> {}

impl <T> InitializeLast<T> {
    pub fn new(parallelism: usize) -> Self {
        Self {
            notify: OnceNotify::new(),
            parallelism,
            counter: AtomicUsize::new(0),
            values: UnsafeCell::new(None)
        }
    }

    pub fn initialize_or_wait<'a, F>(&'a self, build: F) -> Result<T, impl Future<Output=T> + 'a>
    where
        F: FnOnce() -> T,
        T: Clone,
    {
        let this_index = self.counter.fetch_add(1, Ordering::Relaxed);
        if this_index >= self.parallelism {
            panic!("Too many users of Barrier");
        }

        if this_index + 1 == self.parallelism {
            // We are last, so it is our responsibility to run the callback
            let value = build();

            // Create a clone of the value for each parallelism, minus 1 since we don't need one
            let copies = vec![Some(value.clone()); self.parallelism - 1];

            let values = unsafe { &mut* self.values.get() };
            let _ = values.insert(copies);

            // Notify waiting threads that the value is ready
            self.notify.notify();

            return Ok(value);
        }

        // Return a future to wait for the value to appear
        Err(self.take(this_index))
    }

    async fn take(&self, index: usize) -> T {
        self.notify.wait().await;

        let values = unsafe { &mut * self.values.get() }
            .as_mut()
            .expect("Internal notify triggered, but the values array was not filled");

        let value = values[index]
            .take()
            .expect("Value at this index was already consumed");

        value
    }
}
