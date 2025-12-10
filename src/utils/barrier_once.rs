use crate::utils::once_notify::OnceNotify;
use std::sync::atomic::{AtomicI64, Ordering};

pub struct BarrierOnce {
    notify: OnceNotify,
    counter: AtomicI64,
}

impl BarrierOnce {
    pub fn new(parallelism: usize) -> BarrierOnce {
        assert!(parallelism > 0, "Parallelism must be greater than 0");
        let parallelism_i64: i64 = parallelism.try_into().expect("Parallelism must be less than i64::MAX");
        Self {
            notify: OnceNotify::new(),
            // The counter starts at one less than parallelism because each time we call fetch_sub
            // the counter returns the previous value
            counter: AtomicI64::new(parallelism_i64 - 1),
        }
    }
    
    pub fn mark_complete(&self) -> bool {
        let counter = self.counter.fetch_sub(1, Ordering::Relaxed);
        if counter == 0 {
            self.notify.notify();
            return true;
        } else if counter < 0 {
            panic!("Too many users of Barrier");
        }
        false
    }
    
    pub async fn wait_complete(&self) {
        self.notify.wait().await
    }
}
