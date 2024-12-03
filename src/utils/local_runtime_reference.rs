use std::cell::{OnceCell, RefCell};
use std::sync::{Arc, OnceLock};
use tokio::task::LocalSet;

thread_local! {
    static LOCAL_RUNTIME: OnceLock<LocalRuntimeReference> = OnceLock::new();
}

pub fn get_local_runtime<F, R>(f: F) -> R
    where F: FnOnce(&LocalRuntimeReference) -> R
{
    LOCAL_RUNTIME.with(|local_runtime| {
        f(local_runtime.get_or_init(|| LocalRuntimeReference::new()))
    })
}


pub struct LocalRuntimeReference {
    local_set: LocalSet,
}

impl LocalRuntimeReference {
    pub fn new() -> Self {
        Self {
            local_set: LocalSet::new(),
        }
    }

    pub fn register(&self, local_set: LocalSet) -> NotifyHandle {
        let notify = Arc::new(tokio::sync::Notify::new());

        // Spawn a task waiting to cancel the local set
        self.local_set.spawn_local({
            let notify = notify.clone();
            async move {
                println!("Waiting for notify");
                // Wait until notified
                notify.notified().await;
                // Drop the local set
                drop(local_set);
            }
        });

        NotifyHandle::new(notify)
    }
}

pub struct NotifyHandle {
    notify: Arc<tokio::sync::Notify>,
}

impl NotifyHandle {
    pub fn new(notify: Arc<tokio::sync::Notify>) -> Self {
        Self {
            notify
        }
    }

    pub fn notify(&self) {
        self.notify.notify_waiters();
    }
}

impl Drop for NotifyHandle {
    fn drop(&mut self) {
        println!("Notify handle dropped");
        self.notify();
    }
}
