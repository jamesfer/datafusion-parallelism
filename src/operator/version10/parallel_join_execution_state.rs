use crate::operator::version10::new_map_3::new_map_3::{ReadOnlyTable, WriteOnlyTable};
use crate::utils::barrier_once::BarrierOnce;
use crate::utils::initialize_last::InitializeLast;
use crossbeam::atomic::AtomicCell;
use datafusion::arrow::array::{Array, ArrayRef, RecordBatch};
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion_common::{internal_datafusion_err, DataFusionError};
use std::cell::UnsafeCell;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::fmt::{Debug, Formatter};
use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::iter;

#[derive(Clone)]
pub struct SharedOverflowBuffer(pub Arc<UnsafeCell<Vec<usize>>>);

unsafe impl Sync for SharedOverflowBuffer {}
unsafe impl Send for SharedOverflowBuffer {}

struct SharedCompactState {
    // Tracks how many rows have been processed globally
    offset_counter: AtomicUsize,
    // Tracks the total number of batches that have been processed globally. Only updated during
    // compaction.
    batch_count: AtomicUsize,
    // Contains all the processed batches of each thread. When publishing, each thread should only
    // write to their instance's index, to prevent conflicts.
    completed_batches: UnsafeCell<Vec<Vec<(usize, RecordBatch)>>>,
    // The unified overflow buffer for all threads. Only allocated once all threads have started
    // compaction, and we know the total size of the results.
    full_overflow_buffer: InitializeLast<SharedOverflowBuffer>,
    // Barrier that is completed once all threads have written their local overflow buffers to the
    // shared overflow buffer. Should be triggered once per thread.
    buffers_written_to_shared_overflow: BarrierOnce,
    // Barrier that is completed once all threads have written the duplicate entries generated from
    // compacting the index map to the shared overflow buffer. Should be triggered once per thread.
    duplicates_written_to_shared_overflow: BarrierOnce,
    // Counter tracking the next column that needs to be concatenated cooperatively
    next_column_to_compact: AtomicUsize,
    // Stores all concatenated columns. Each thread should only write to the index of the column
    // they are concatenating, as decided by the above counter.
    concatenated_arrays: UnsafeCell<Vec<Option<ArrayRef>>>,
    // Barrier that is completed once all columns have been concatenated. Should be triggered once
    // per column.
    all_arrays_concatenated: BarrierOnce,
}

unsafe impl Sync for SharedCompactState {}
unsafe impl Send for SharedCompactState {}

pub struct JoinStateInstance {
    instance_index: usize,
    column_schema: SchemaRef,
    hash_index_map: WriteOnlyTable<usize>,
    shared_compact_state: Arc<SharedCompactState>,
    // Local state data
    record_batches: Vec<(usize, RecordBatch)>,
    overflow_buffers: Vec<(usize, Vec<usize>)>,
}

// The JoinStateInstance uses some UnsafeCells, but it is safe to share them between threads
unsafe impl Sync for JoinStateInstance {}
unsafe impl Send for JoinStateInstance {}

impl Debug for JoinStateInstance {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("SharedJoinState {...}")
    }
}

impl JoinStateInstance {
    fn new(
        instance_index: usize,
        column_schema: SchemaRef,
        hash_index_map: WriteOnlyTable<usize>,
        shared_compact_state: Arc<SharedCompactState>,
    ) -> Self {
        Self {
            instance_index,
            column_schema,
            hash_index_map,
            shared_compact_state,
            record_batches: vec![],
            overflow_buffers: vec![],
        }
    }

    pub fn add(&mut self, hashes: Vec<u64>, record_batch: RecordBatch) {
        let batch_size = hashes.len();
        
        // Find the offset of this record batch
        let offset = self.shared_compact_state.offset_counter.fetch_add(batch_size, Ordering::Relaxed);

        // Store the record batch in our list for lookups later
        self.record_batches.push((offset, record_batch));

        // Insert all the hashes in the index map with their new offset
        let mut overflow_buffer = vec![0; batch_size];
        for (index, hash) in hashes.into_iter().enumerate() {
            if let Some(existing) = self.hash_index_map.insert(hash, offset + index + 1) {
                overflow_buffer[index] = existing;
            }
        }

        // Insert chunks in groups of 4
        // const CHUNK_SIZE: usize = 4;
        // for (index, chunk) in hashes.chunks(CHUNK_SIZE).enumerate() {
        //     let index = index * CHUNK_SIZE + offset + 1;
        //     let option1 = self.hash_index_map.insert(chunk[0], index + 0);
        //     let option2 = self.hash_index_map.insert(chunk[1], index + 1);
        //     let option3 = self.hash_index_map.insert(chunk[2], index + 2);
        //     let option4 = self.hash_index_map.insert(chunk[3], index + 3);
        //
        //     if let Some(existing) = option1 {
        //         overflow_buffer[index + 0] = existing;
        //     }
        //     if let Some(existing) = option2 {
        //         overflow_buffer[index + 1] = existing;
        //     }
        //     if let Some(existing) = option3 {
        //         overflow_buffer[index + 2] = existing;
        //     }
        //     if let Some(existing) = option4 {
        //         overflow_buffer[index + 3] = existing;
        //     }
        // }
        
        // Store the overflow buffer
        self.overflow_buffers.push((offset, overflow_buffer));
    }

    pub async fn compact(self) -> Result<(Arc<ReadOnlyTable<usize>>, SharedOverflowBuffer, RecordBatch), DataFusionError> {
        // Publish local data to the shared state
        Self::share_local_data(
            self.instance_index,
            self.record_batches,
            &self.shared_compact_state,
        );

        // Allocate space for the final overflow buffer
        let overflow_buffer_result = Self::allocate_final_overflow_buffer(&self.shared_compact_state);

        // Signal that we are no longer going to write to the index map, allowing other threads to
        // start compacting
        let index_map_compactor = self.hash_index_map.finish();

        // This promise is only fulfilled once all threads have reached this part, so by waiting for
        // this promise, we can safely assume that all the threads have shared their data.
        let shared_overflow_buffer = match overflow_buffer_result {
            Ok(value) => value,
            Err(future) => future.await,
        };

        // Write each of our overflow buffers to the output buffer. The overflow buffer is shared
        // between the threads, but we know that we are only going to write to certain areas of it,
        // and those writes will never overlap.
        let shared_overflow_buffer_future = Self::write_buffers_to_overflow_buffer(
            &self.overflow_buffers,
            shared_overflow_buffer,
            &self.shared_compact_state.buffers_written_to_shared_overflow,
        );

        // Compact index lookup table
        let (duplicate_elements, read_only_index_map_future) = index_map_compactor.participate_in_compaction().await;

        // Cooperatively concatenated each of the record batch columns
        let batch_count = self.shared_compact_state.batch_count.load(Ordering::Relaxed);
        let arrow_arrays_concatenated_future = if batch_count == 0 {
            Err(())
        } else {
            Ok(Self::cooperatively_concatenate_arrow_arrays(
                &self.column_schema,
                self.shared_compact_state.clone(),
            )?)
        };

        // Write duplicate values discovered during compaction to destination overflow buffer
        let shared_compact_state = self.shared_compact_state.clone();
        let duplicates_written_to_shared_overflow_buffer_future = async move {
            Self::write_duplicate_entries_to_overflow(
                shared_overflow_buffer_future.await,
                duplicate_elements,
                shared_compact_state,
            )
        };

        // Combine completed columns into record batch locally
        let shared_compact_state = self.shared_compact_state.clone();
        let full_record_batch_future = async move {
            match arrow_arrays_concatenated_future {
                Ok(future) => {
                    future.await;
                    Self::build_record_batch_locally(
                        self.column_schema.clone(),
                        &shared_compact_state,
                    )
                },
                Err(_) => {
                    Ok(RecordBatch::new_empty(self.column_schema.clone()))
                }
            }
        };


        tokio::try_join!(
            async { Ok(read_only_index_map_future.await) },
            async { Ok(duplicates_written_to_shared_overflow_buffer_future.await.await) },
            full_record_batch_future
        )
    }

    fn share_local_data(instance_index: usize, record_batches: Vec<(usize, RecordBatch)>, shared_compact_state: &SharedCompactState) {
        // Publish our local record batches by writing to the shared list
        shared_compact_state.batch_count.fetch_add(record_batches.len(), Ordering::Relaxed);

        // SAFETY: we only mutate the array at our specific index, so multiple writes never overlap
        let completed_batches = unsafe { &mut *shared_compact_state.completed_batches.get() };
        completed_batches[instance_index] = record_batches;
    }

    fn build_record_batch_locally(
        column_schema: SchemaRef,
        shared_compact_state: &SharedCompactState,
    ) -> Result<RecordBatch, DataFusionError> {
        let concatenated_arrays = unsafe { &* shared_compact_state.concatenated_arrays.get() };
        let concatenated_arrays = concatenated_arrays.iter()
            .map(|maybe_array| maybe_array.as_ref().cloned().ok_or_else(|| internal_datafusion_err!("Array not completed")))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(RecordBatch::try_new(column_schema, concatenated_arrays)?)
    }

    fn write_buffers_to_overflow_buffer<'a>(
        overflow_buffers: &'a [(usize, Vec<usize>)],
        shared_overflow_buffer: SharedOverflowBuffer,
        buffers_written_to_shared_overflow: &'a BarrierOnce,
    ) -> impl Future<Output=SharedOverflowBuffer> + use<'a> {
        let mut_overflow_buffer = unsafe { &mut *shared_overflow_buffer.0.get() };
        for (offset, buffer) in overflow_buffers {
            let start = offset + 1;
            let end = start + buffer.len();
            mut_overflow_buffer[start..end].copy_from_slice(buffer);
        }
        // Mark this thread as complete
        buffers_written_to_shared_overflow.mark_complete();

        async move {
            // Wait for all threads to be finished with the buffer before continuing
            buffers_written_to_shared_overflow.wait_complete().await;
            shared_overflow_buffer
        }
    }

    fn cooperatively_concatenate_arrow_arrays(
        column_schema: &Schema,
        shared_compact_state: Arc<SharedCompactState>,
    ) -> Result<impl Future<Output=()>, DataFusionError> {
        let schema_fields_length = column_schema.fields.len();
        let mut column_index = shared_compact_state.next_column_to_compact.fetch_add(1, Ordering::Relaxed);
        if column_index < column_schema.fields.len() {
            let batch_count = shared_compact_state.batch_count.load(Ordering::Relaxed);

            // Order the record batches using k-way merge sort. This is duplicate work conducted by
            // every thread, but it doesn't seem like a large amount of work, so maybe this is OK.
            // The other approach would be to have one thread sort all the results together and
            // share the result somehow
            let completed_batches = unsafe { &* shared_compact_state.completed_batches.get() };
            let ordered_batches = Self::k_way_merge_sort(completed_batches, |item| item.0, batch_count);

            // Loop over all the columns to compact
            while column_index < schema_fields_length {
                let ordered_arrays = ordered_batches.iter()
                    .map(|(_, batch)| batch.column(column_index))
                    .collect::<Vec<_>>();

                let arrays_to_concat: Vec<&dyn Array> = ordered_arrays
                    .iter()
                    .map(|array| array.as_ref())
                    .collect();
                let concatenated_array = datafusion::arrow::compute::concat(&arrays_to_concat)?;

                let concatenated_arrays = unsafe { &mut* shared_compact_state.concatenated_arrays.get() };
                concatenated_arrays[column_index] = Some(concatenated_array);

                // Mark this column as complete
                shared_compact_state.all_arrays_concatenated.mark_complete();

                column_index = shared_compact_state.next_column_to_compact.fetch_add(1, Ordering::Relaxed);
            }
        }

        Ok(async move {
            // Wait for all the columns to be completed before continuing
            shared_compact_state.all_arrays_concatenated.wait_complete().await;
        })
    }

    fn write_duplicate_entries_to_overflow(
        shared_overflow_buffer: SharedOverflowBuffer,
        duplicate_elements: Vec<(usize, usize)>,
        shared_compact_state: Arc<SharedCompactState>,
    ) -> impl Future<Output=SharedOverflowBuffer> {
        let mut_overflow_buffer = unsafe { &mut *shared_overflow_buffer.0.get() };
        for (replaced_value, new_value) in duplicate_elements {
            mut_overflow_buffer[new_value] = replaced_value;
        }
        shared_compact_state.duplicates_written_to_shared_overflow.mark_complete();

        async move {
            shared_compact_state.duplicates_written_to_shared_overflow.wait_complete().await;
            shared_overflow_buffer
        }
    }

    fn k_way_merge_sort<T, F>(vecs: &[Vec<T>], key_fn: F, reserved_size: usize) -> Vec<T>
    where 
        T: Clone,
        F: Fn(&T) -> usize,
    {
        // (sort_key, vec_index, element_index)
        let mut heap: BinaryHeap<Reverse<(usize, usize, usize)>> = BinaryHeap::with_capacity(vecs.len());
        let mut result = Vec::with_capacity(reserved_size);
        
        // Initialize heap with first element from each non-empty vec
        for (vec_idx, vec) in vecs.iter().enumerate() {
            if let Some(first_item) = vec.first() {
                let key = key_fn(first_item);
                heap.push(Reverse((key, vec_idx, 0))); // (sort_key, vec_index, element_index)
            }
        }
        
        // Extract minimum and advance iterators
        while let Some(Reverse((_, vec_idx, elem_idx))) = heap.pop() {
            result.push(vecs[vec_idx][elem_idx].clone());
            
            // Add next element from the same vec if it exists
            if elem_idx + 1 < vecs[vec_idx].len() {
                let next_item = &vecs[vec_idx][elem_idx + 1];
                let next_key = key_fn(next_item);
                heap.push(Reverse((next_key, vec_idx, elem_idx + 1)));
            }
        }
        
        result
    }

    fn allocate_final_overflow_buffer(
        shared_compact_state: &Arc<SharedCompactState>,
    ) -> Result<SharedOverflowBuffer, impl Future<Output=SharedOverflowBuffer> + use <'_>> {
        // let cloned = shared_compact_state.clone();
        let value = shared_compact_state.full_overflow_buffer.initialize_or_wait(move || {
            // We're the last instance - allocate the Vec
            let total_size = shared_compact_state.offset_counter.load(Ordering::Relaxed);

            // The overflow buffer needs to be one larger than the actual size of the data as
            // indexes start at 1, so 0 is a placeholder for the end of the chain.
            let overflow_size = total_size + 1;
            SharedOverflowBuffer(Arc::new(UnsafeCell::new(vec![0; overflow_size])))
        });

        match value {
            Ok(buffer) => Ok(buffer.clone()),
            Err(future) => Err(async move {
                future.await.clone()
            })
        }
    }
}

pub struct JoinStateInstances {
    states: Vec<AtomicCell<Option<JoinStateInstance>>>,
}

impl JoinStateInstances {
    pub fn new(parallelism: usize, column_schema: SchemaRef) -> Self {
        let join_map = WriteOnlyTable::new();
        let shared_compact_state = Arc::new(SharedCompactState {
            offset_counter: AtomicUsize::new(0),
            batch_count: AtomicUsize::new(0),
            full_overflow_buffer: InitializeLast::new(parallelism),
            buffers_written_to_shared_overflow: BarrierOnce::new(parallelism),
            // Needs to be initialised with 1 entry per thread
            completed_batches: UnsafeCell::new(vec![Vec::new(); parallelism]),
            next_column_to_compact: AtomicUsize::new(0),
            // Needs to be initialised with 1 entry per column
            concatenated_arrays: UnsafeCell::new(vec![None; column_schema.fields().len()]),
            duplicates_written_to_shared_overflow: BarrierOnce::new(parallelism),
            all_arrays_concatenated: BarrierOnce::new(column_schema.fields().len()),
        });

        let states = (0..parallelism).into_iter()
            .zip(iter::repeat_n((join_map, shared_compact_state, column_schema), parallelism))
            .map(|(instance_index, (hash_index_map, shared_compact_state, column_schema))| AtomicCell::new(Some(JoinStateInstance::new(
                instance_index,
                column_schema,
                hash_index_map,
                shared_compact_state,
            ))))
            .collect::<Vec<_>>();
        Self { states }
    }

    pub fn take(&self, index: usize) -> Option<JoinStateInstance> {
        self.states.get(index).map(|cell| cell.take()).flatten()
    }
}

impl Debug for JoinStateInstances {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("ParallelJoinExecutionState {...}")
    }
}
