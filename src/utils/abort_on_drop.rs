use std::ops::Deref;
use tokio::task::JoinHandle;

pub struct AbortOnDrop<T> {
    handle: JoinHandle<T>,
}

impl<T> AbortOnDrop<T> {
    pub fn new(handle: JoinHandle<T>) -> Self {
        Self { handle }
    }
}

impl <T> Deref for AbortOnDrop<T> {
    type Target = JoinHandle<T>;

    fn deref(&self) -> &Self::Target {
        &self.handle
    }
}

impl <T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        self.handle.abort();
    }
}
