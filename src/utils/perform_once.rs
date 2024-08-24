use std::sync::atomic::{AtomicBool, Ordering};

pub struct PerformOnce {
    started: AtomicBool
}

impl PerformOnce {
    pub fn new() -> Self {
        Self { started: AtomicBool::new(false) }
    }

    pub fn run_once<F, T>(&self, operation: F) -> Result<T, ()>
        where F: FnOnce() -> T
    {
        // Check if the operation has already started
        // TODO these atomic operations could probably use a looser ordering
        match self.started.compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst) {
            // If successful, we are the first thread to attempt to perform the operation
            Ok(_) => Ok(operation()),
            // Otherwise, another thread has already started, so we can return a result to wait on
            Err(_) => Err(()),
        }
    }
}
