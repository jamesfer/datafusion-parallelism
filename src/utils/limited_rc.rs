use std::ops::Deref;
use std::sync::Arc;

/// Creates a limited set of copies of a value and prevents more being created accidentally.
/// Useful for values that need to be consumed after the final reference is dropped.
#[derive(Debug)]
pub struct LimitedRc<T> {
    inner: Arc<T>
}

impl <T> LimitedRc<T> {
    pub fn new_copies(value: T, count: usize) -> Vec<LimitedRc<T>> {
        let inner = Arc::new(value);
        (0..count)
            .map(|i| Self {
                inner: Arc::clone(&inner),
            })
            .collect()
    }

    pub fn into_inner(self) -> Option<T> {
        Arc::into_inner(self.inner)
    }
}

impl <T> Deref for LimitedRc<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.inner.deref()
    }
}
