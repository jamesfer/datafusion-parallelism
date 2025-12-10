use std::fmt::Debug;
use std::sync::atomic::{AtomicUsize, Ordering};

pub trait AtomicOps<V> {
    fn fetch_or(&self, value: V, ordering: Ordering) -> V;
    fn store(&self, value: V, ordering: Ordering);
    fn swap(&self, value: V, ordering: Ordering) -> V;
    unsafe fn from_ptr<'a>(ptr: *mut V) -> &'a Self;
}

impl AtomicOps<usize> for AtomicUsize {
    fn fetch_or(&self, value: usize, ordering: Ordering) -> usize {
        self.fetch_or(value, ordering)
    }

    fn store(&self, value: usize, ordering: Ordering) {
        self.store(value, ordering);
    }

    fn swap(&self, value: usize, ordering: Ordering) -> usize {
        self.swap(value, ordering)
    }

    unsafe fn from_ptr<'a>(ptr: *mut usize) -> &'a Self {
        AtomicUsize::from_ptr(ptr)
    }
}

pub trait AsAtomic: Sized + Debug {
    type AtomicT: AtomicOps<Self>;
}

impl AsAtomic for usize {
    type AtomicT = AtomicUsize;
}
