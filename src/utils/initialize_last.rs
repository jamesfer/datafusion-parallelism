use std::cell::{OnceCell, UnsafeCell};
use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use crate::utils::once_notify::OnceNotify;

pub struct InitializeLast<T> {
    notify: OnceNotify,
    parallelism: usize,
    counter: AtomicUsize,
    value: UnsafeCell<Option<T>>
}

unsafe impl <T: Send> Send for InitializeLast<T> {}
unsafe impl <T: Sync> Sync for InitializeLast<T> {}

impl <T> InitializeLast<T> {
    pub fn new(parallelism: usize) -> Self {
        Self {
            notify: OnceNotify::new(),
            parallelism,
            counter: AtomicUsize::new(0),
            value: UnsafeCell::new(None)
        }
    }

    pub fn initialize_or_wait<F>(&self, build: F) -> Result<&T, impl Future<Output=&T>>
    where F: FnOnce() -> T
    {
        let counter = self.counter.fetch_add(1, Ordering::Relaxed) + 1;
        if counter == self.parallelism {
            // We are last, initialise the value
            let value = unsafe { &mut* self.value.get() };
            let t = build();
            let t = value.insert(t);
            self.notify.notify();

            return Ok(t);
        } else if counter > self.parallelism {
            panic!("Too many users of Barrier");
        }

        Err(self.get())
    }

    pub async fn get(&self) -> &T {
        self.notify.wait().await;
        let option = unsafe { &* self.value.get() };
        option.as_ref().unwrap()
    }
}
