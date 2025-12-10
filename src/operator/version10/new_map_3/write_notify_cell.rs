use std::cell::UnsafeCell;
use std::sync::Arc;
use tokio::sync::{Semaphore};

struct State<T> {
    // TODO replace with once cell
    value: UnsafeCell<Option<T>>,
    write_complete: Semaphore,
}

pub struct WriteNotifyCell<T> {
    state: Arc<State<T>>
}

unsafe impl <T: Send> Send for WriteNotifyCell<T> {}
unsafe impl <T: Sync> Sync for WriteNotifyCell<T> {}

impl <T> WriteNotifyCell<T> {
    pub fn new() -> Self {
        Self {
            state: Arc::new(State {
                value: UnsafeCell::new(None),
                write_complete: Semaphore::new(0),
            }),
        }
    }

    pub fn get_reader(&self) -> ReadNotifyCell<T> {
        ReadNotifyCell { state: Arc::clone(&self.state) }
    }

    pub fn write(mut self, value: T) {
        let raw = self.state.value.get();

        // SAFETY: No reads of the value are allowed until the semaphore is triggered. Therefore,
        // the function signature ensures we are the only writer of the value.
        unsafe { *raw = Some(value) };

        self.state.write_complete.close();
    }
}

#[derive(Clone)]
pub struct ReadNotifyCell<T> {
    state: Arc<State<T>>,
}

unsafe impl <T: Send> Send for ReadNotifyCell<T> {}
unsafe impl <T: Sync> Sync for ReadNotifyCell<T> {}

impl <T> ReadNotifyCell<T> {
    pub async fn read(&self) -> &T {
        // Wait for the value to be written
        match self.state.write_complete.acquire().await {
            // The semaphore should never provide an actual permit
            Ok(_) => unreachable!(),
            Err(_) => {},
        };

        let raw = self.state.value.get();

        // SAFETY: Once the semaphore completes, the value is guaranteed to have been written, and
        // the value will only ever be written once.
        let option = unsafe { &*raw };

        option.as_ref().expect("Value was not written")
    }
}
