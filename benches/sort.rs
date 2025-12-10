use std::cmp::Ordering;
use std::collections::{BTreeSet, BinaryHeap};
use std::time::Duration;
use criterion::{criterion_group, criterion_main, Criterion};
use datafusion::arrow;
use datafusion::arrow::array::{Array, ArrayRef, PrimitiveArray, UInt32Array, UInt64Builder};
use datafusion::arrow::compute::SortOptions;
use datafusion::arrow::datatypes::UInt64Type;
use rand::random;

fn total_sort(inputs: Vec<Vec<u64>>) -> Vec<u64> {
    let mut merged = Vec::new();
    for input in inputs {
        merged.extend(input);
    }
    merged.sort_unstable();
    merged
}

fn btree_set(inputs: Vec<Vec<u64>>) -> BTreeSet<u64> {
    let mut btree_set = BTreeSet::new();
    for input in inputs {
        for value in input {
            btree_set.insert(value);
        }
    }

    btree_set
}

fn pre_merge(mut random_inputs: &mut Vec<Vec<u64>>) {
    for input in random_inputs.iter_mut() {
        input.sort_unstable();
    }
}

fn merge_sort_divide_conquer(mut inputs: Vec<Vec<u64>>) -> Vec<u64> {
    pre_merge(&mut inputs);
    let mut ranges = inputs.iter()
        .map(|vec| 0..vec.len())
        .scan(0, |mut offset, range| {
            let size = range.end - range.start;
            let current_offset = *offset;
            *offset += size;
            Some(range.start + current_offset..range.end + current_offset)
        })
        .collect::<Vec<_>>();
    let mut destination = inputs.into_iter().flatten().collect::<Vec<_>>();

    // Start merging vecs in pairs
    while ranges.len() > 1 {
        ranges = ranges.chunks(2)
            .map(|chunk| {
                if chunk.len() < 2 {
                    return chunk[0].clone();
                }

                let left = &chunk[0];
                let right = &chunk[1];
                assert_eq!(left.end, right.start);

                // Merge sort left and right. We hope the algorithm can detect that left and right
                // are sorted 🤞
                let mut slice = &mut destination[left.start..right.end];
                slice.sort();

                return left.start..right.end;
            })
            .collect::<Vec<_>>();
    }

    destination
}

struct StoredVec {
    current_index: usize,
    data: Vec<u64>,
}

impl StoredVec {
    fn new(data: Vec<u64>) -> Option<Self> {
        if data.is_empty() {
            None
        } else {
            Some(StoredVec {
                current_index: 0,
                data,
            })
        }
    }

    fn increment(mut self) -> Option<Self> {
        self.current_index += 1;
        if self.current_index < self.data.len() {
            Some(self)
        } else {
            None
        }
    }
}

impl Eq for StoredVec {}

impl PartialEq<Self> for StoredVec {
    fn eq(&self, other: &Self) -> bool {
        self.data[self.current_index] == other.data[other.current_index]
    }
}

impl Ord for StoredVec {
    fn cmp(&self, other: &Self) -> Ordering {
        // Need to reverse the ordering so the BinaryHeap becomes a MinHeap
        self.data[self.current_index].cmp(&other.data[other.current_index]).reverse()
    }
}

impl PartialOrd<Self> for StoredVec {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

fn k_way_merge(mut inputs: Vec<Vec<u64>>) -> Vec<u64>{
    pre_merge(&mut inputs);

    let total_length = inputs.iter().map(|vec| vec.len()).sum::<usize>();
    let mut h = BinaryHeap::new();

    // Insert all the vecs into the binary heap so the one with the smallest value is on top
    for vec in inputs {
        match StoredVec::new(vec) {
            None => {}
            Some(stored_vec) => {
                h.push(stored_vec);
            }
        }
    }

    let mut destination = vec![0; total_length];
    let mut write_index = 0;
    while let Some(next) = h.pop() {
        destination[write_index] = next.data[next.current_index];
        write_index += 1;

        match next.increment() {
            None => {}
            Some(new_stored_vec) => {
                h.push(new_stored_vec);
            }
        }
    }

    destination
}

struct ArrowStoredVec {
    current_index: usize,
    indices: UInt32Array,
    data: PrimitiveArray<UInt64Type>,
    has_nulls: bool,
}

impl ArrowStoredVec {
    fn new(indices: UInt32Array, data: PrimitiveArray<UInt64Type>) -> Option<Self> {
        if data.is_empty() {
            None
        } else {
            Some(ArrowStoredVec {
                current_index: 0,
                indices,
                has_nulls: data.null_count() > 0,
                data,
            })
        }
    }

    fn increment(mut self) -> Option<Self> {
        self.current_index += 1;
        if self.current_index < self.data.len() {
            Some(self)
        } else {
            None
        }
    }
}

impl Eq for ArrowStoredVec {}

impl PartialEq<Self> for ArrowStoredVec {
    fn eq(&self, other: &Self) -> bool {
        // Null == null for now
        if self.has_nulls || other.has_nulls {
            let x = self.data.is_null(self.current_index);
            let x1 = other.data.is_null(other.current_index);
            if x && x1 {
                return true;
            }
            if x || x1 {
                return false;
            }
        }

        let left = self.data.value(self.current_index);
        let right = other.data.value(self.current_index);
        left == right
    }
}

impl Ord for ArrowStoredVec {
    fn cmp(&self, other: &Self) -> Ordering {
        // Nulls go last for now
        if self.has_nulls || other.has_nulls {
            let left_is_null = self.data.is_null(self.current_index);
            let right_is_null = other.data.is_null(other.current_index);
            if left_is_null && right_is_null {
                return Ordering::Equal;
            }
            if left_is_null {
                return Ordering::Greater;
            }
            if right_is_null {
                return Ordering::Less;
            }
        }

        let left = self.data.value(self.current_index);
        let right = other.data.value(self.current_index);
        left.cmp(&right)
    }
}

impl PartialOrd<Self> for ArrowStoredVec {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

fn arrow_k_way_merge(inputs: Vec<PrimitiveArray<UInt64Type>>) -> (Vec<u64>, Vec<bool>) {
    let total_length = inputs.iter().map(|array| array.len()).sum::<usize>();
    let mut h = BinaryHeap::new();

    for array in inputs {
        let indices = arrow::compute::sort_to_indices(&array, Some(SortOptions { descending: false, nulls_first: false }), None).unwrap();
        h.push(ArrowStoredVec::new(indices, array).unwrap());
    }

    let mut destination = vec![0; total_length];
    let mut nulls = vec![false; total_length];
    let mut write_index = 0;
    while let Some(next) = h.pop() {
        if next.has_nulls {
            if next.data.is_null(next.current_index) {
                nulls[write_index] = true;
            } else {
                destination[write_index] = next.data.value(next.current_index);
            }
        } else {
            destination[write_index] = next.data.value(next.current_index);
        }
        write_index += 1;

        match next.increment() {
            None => {},
            Some(next) => h.push(next),
        }
    }

    (destination, nulls)
}

fn arrow_concat_and_sort(inputs: Vec<PrimitiveArray<UInt64Type>>) -> ArrayRef {
    let input_refs = inputs.iter().map(|a| -> &dyn Array { a }).collect::<Vec<_>>();
    let all = arrow::compute::concat(input_refs.as_slice()).unwrap();
    arrow::compute::sort(
        all.as_ref(),
        Some(SortOptions { descending: false, nulls_first: false }),
    ).unwrap()
}

fn arrow_divide_and_conquer(inputs: Vec<PrimitiveArray<UInt64Type>>) -> ArrayRef {
    // Sort each array
    let mut sorted = inputs.into_iter()
        .map(|array| {
            arrow::compute::sort(
                &array,
                Some(SortOptions { descending: false, nulls_first: false }),
            ).unwrap()
        })
        .collect::<Vec<_>>();

    // Sort the smaller arrays together in gradually increasing pairs
    while sorted.len() > 1 {
        sorted = sorted.chunks(2)
            .map(|chunk| {
                if chunk.len() == 1 {
                    return chunk[0].clone();
                }

                let together = arrow::compute::concat(
                    chunk.iter().map(|a| -> &dyn Array { a }).collect::<Vec<_>>().as_slice(),
                ).unwrap();
                arrow::compute::sort(
                    &together,
                    Some(SortOptions { descending: false, nulls_first: false }),
                ).unwrap()
            })
            .collect::<Vec<_>>();
    }

    assert_eq!(sorted.len(), 1);
    sorted.pop().unwrap()
}


fn arrow_single_sort(input: &dyn Array) -> ArrayRef {
    arrow::compute::sort(input, None).unwrap()
}

fn stable_single_sort(mut input: Vec<u64>) -> Vec<u64> {
    input.sort();
    input
}

fn unstable_single_sort(mut input: Vec<u64>) -> Vec<u64> {
    input.sort_unstable();
    // input.sort_unstable_by_key()
    input
}

fn make_config() -> Criterion {
    Criterion::default()
        .warm_up_time(Duration::from_secs(5))
        .measurement_time(Duration::from_secs(30))
        .sample_size(50)
}

fn criterion_benchmark(c: &mut Criterion) {
    let vecs = 128;
    let size = 8192;
    let input = (0..vecs).into_iter()
        .map(|_| (0..size).into_iter()
            .map(|_| random::<u64>())
            .collect::<Vec<_>>())
        .collect::<Vec<_>>();

    // c.bench_function("stable", |bencher| {
    //     bencher.iter(|| stable_single_sort(input[0].clone()));
    // });
    //
    // c.bench_function("unstable", |bencher| {
    //     bencher.iter(|| unstable_single_sort(input[0].clone()));
    // });
    //
    // c.bench_function("arrow", |bencher| {
    //     let array = arrow::array::UInt64Array::from(input[0].clone());
    //     bencher.iter(|| arrow_single_sort(&array));
    // });

    // c.bench_function("total_sort", |bencher| {
    //     bencher.iter(|| total_sort(input.clone()));
    // });

    let arrow_input = input.iter()
        .map(|vec| arrow::array::UInt64Array::from(vec.clone()))
        .collect::<Vec<_>>();
    // Make some entries null
    let arrow_input = arrow_input.into_iter()
        .map(|array| {
            let mut builder = array.into_builder().unwrap();
            builder.append_null();
            let mut validity_slice = builder.validity_slice_mut().unwrap();
            for i in 0..validity_slice.len() {
                if random::<f64>() < 0.05 {
                    validity_slice[i] = 0; // Sets all 8 bits to false
                }
            }
            builder.finish()
        })
        .collect::<Vec<_>>();

    c.bench_function("arrow_concat_and_sort", |bencher| {
        bencher.iter(|| arrow_concat_and_sort(arrow_input.clone()));
    });

    c.bench_function("arrow_k_way_merge", |bencher| {
        let input = input.iter()
            .map(|vec| arrow::array::UInt64Array::from(vec.clone()))
            .collect::<Vec<_>>();
        bencher.iter(|| arrow_k_way_merge(input.clone()));
    });

    c.bench_function("arrow_divide_and_conquer", |bencher| {
        let input = input.iter()
            .map(|vec| arrow::array::UInt64Array::from(vec.clone()))
            .collect::<Vec<_>>();
        bencher.iter(|| arrow_divide_and_conquer(input.clone()));
    });

    // c.bench_function("btree_set", |bencher| {
    //     bencher.iter(|| btree_set(input.clone()));
    // });

    // c.bench_function("merge_sort_divide_conquer", |bencher| {
    //     bencher.iter(|| merge_sort_divide_conquer(input.clone()));
    // });
    //
    // c.bench_function("k_way_merge", |bencher| {
    //     bencher.iter(|| k_way_merge(input.clone()));
    // });
}

criterion_main!(benches);
criterion_group! {
    name = benches;
    config = make_config();
    targets = criterion_benchmark
}
