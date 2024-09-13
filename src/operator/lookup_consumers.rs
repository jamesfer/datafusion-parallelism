use datafusion::arrow::record_batch::RecordBatch;
use crate::utils::index_lookup::IndexLookup;

pub trait IndexLookupConsumer {
    type R;

    fn call<Lookup>(self, index_lookup: Lookup, record_batch: RecordBatch) -> Self::R
        where Lookup: IndexLookup<u64> + Sync + Send + 'static;
}

pub trait IndexLookupBorrower {
    type R<'t>;

    fn call<'a, Lookup>(self, index_lookup: &'a Lookup, record_batch: &'a RecordBatch) -> Self::R<'a>
        where Lookup: IndexLookup<u64> + Sync + Send + 'static;
}

pub trait IndexLookupProvider {
    fn consume<C: IndexLookupConsumer>(self, consumer: C) -> C::R;
    fn borrow<C: IndexLookupBorrower>(&self, borrower: C) -> C::R<'_>;
}

pub struct SimpleIndexLookupProvider<Lookup: IndexLookup<u64> + Sync + Send + 'static> {
    lookup: Lookup,
    record_batch: RecordBatch,
}

impl<Lookup: IndexLookup<u64> + Sync + Send + 'static> SimpleIndexLookupProvider<Lookup> {
    pub fn new(lookup: Lookup, record_batch: RecordBatch) -> Self {
        Self { lookup, record_batch }
    }
}

impl <Lookup: IndexLookup<u64> + Sync + Send + 'static> IndexLookupProvider for SimpleIndexLookupProvider<Lookup> {
    fn consume<C: IndexLookupConsumer>(self, consumer: C) -> C::R {
        consumer.call(self.lookup, self.record_batch)
    }

    fn borrow<B: IndexLookupBorrower>(&self, borrower: B) -> B::R<'_> {
        borrower.call(&self.lookup, &self.record_batch)
    }
}
