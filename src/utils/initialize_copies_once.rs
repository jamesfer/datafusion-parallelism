use std::fmt::{Debug, Formatter};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::OnceLock;
use crossbeam::atomic::AtomicCell;
use crate::utils::limited_rc::LimitedRc;

pub struct InitializeCopiesOnce<T>
    where T: Clone
{
    copies: usize,
    values: OnceLock<Vec<AtomicCell<Option<LimitedRc<T>>>>>,
    read_index: AtomicUsize,
}

impl <T: Clone> Debug for InitializeCopiesOnce<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("InitializeCopiesOnce")
    }
}

impl <T> InitializeCopiesOnce<T>
    where T: Clone
{
    pub fn new(copies: usize) -> Self {
        Self {
            copies,
            values: OnceLock::new(),
            read_index: AtomicUsize::new(0),
        }
    }

    pub fn get_clone_or_initialize<F>(&self, operation: F) -> Result<LimitedRc<T>, String>
        where F: FnOnce() -> T
    {
        let values = self.values.get_or_init(|| {
            let value = operation();
            let clones = LimitedRc::new_copies(value, self.copies);
            clones.into_iter().map(|clone| AtomicCell::new(Some(clone))).collect()
        });
        let read_index = self.read_index.fetch_add(1, Ordering::Relaxed);
        match values.get(read_index) {
            None => Err(format!("Too many threads tried to call initialize. Expected {}, this thread is number {}", self.copies, read_index + 1)),
            Some(cell) => match cell.swap(None) {
                None => unreachable!(),
                Some(clone) => Ok(clone)
            }
        }
    }
}
