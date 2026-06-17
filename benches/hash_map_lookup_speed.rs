#![feature(iter_array_chunks)]

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use criterion::{black_box, criterion_group, criterion_main, Bencher, BenchmarkId, Criterion, Throughput};
use criterion::BatchSize::LargeInput;
use datafusion_parallelism::operator::version10::new_map_3::new_map_3::{ReadOnlyTable, WriteOnlyTable};
use hashbrown::raw::RawTable;
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};
use rand::distributions::Uniform;
use tokio::runtime::{Builder, Runtime};
use datafusion_parallelism::operator::version10::new_map_3::group::group8_reserve_zero::Group8ReserveZero;
use datafusion_parallelism::operator::version10::new_map_3::group::group8_spread_empty::Group8SpreadEmpty;
use datafusion_parallelism::operator::version10::new_map_3::group::group8_swiss_plus::Group8SwissPlus;
use datafusion_parallelism::operator::version10::new_map_3::group::group_strategy::{BulkGroupStrategyN, GroupStrategy, IterableGroupStrategy};
use datafusion_parallelism::operator::version10::new_map_3::group::probe_hybrid::{HybridProbeSequence, HybridProbeSequenceSlide};
use datafusion_parallelism::operator::version10::new_map_3::group::probe_sequence::{ProbeSequence, ProbeSequenceBulkN};
use datafusion_parallelism::operator::version10::new_map_3::group::probe_swiss::SwissTableProbeSeq;

const PARTITIONS: usize = 8;
const RNG_SEED: u64 = 0x42;

#[derive(Clone, Copy)]
struct Scenario {
    fake_partitioning: bool,
    hit_rate: f64,
    size: usize,
    lookup_count: usize,
}

impl Scenario {
    fn name(&self) -> String {
        format!(
            "{}_of_{}/{}{}",
            self.lookup_count,
            self.size,
            self.hit_rate,
            if self.fake_partitioning { "/partitioned" } else { "" },
        )
    }

    fn entries(&self) -> usize {
        self.size
    }

    fn partitioned_entries(&self) -> usize {
        if self.fake_partitioning {
            self.size / PARTITIONS
        } else {
            self.size
        }
    }
}

fn configure_criterion() -> Criterion {
    Criterion::default()
        .warm_up_time(Duration::from_secs(4))
        .measurement_time(Duration::from_secs(10))
}

criterion_group! {
    name = benches;
    config = configure_criterion();
    targets = criterion_benchmark
}
criterion_main!(benches);

fn criterion_benchmark(c: &mut Criterion) {
    let scenarios = vec![
        // Scenario {
        //     fake_partitioning: false,
        //     hit_rate: 0.5,
        //     size: 1 << 16,
        //     lookup_count: 1 << 18,
        // },
        Scenario {
            fake_partitioning: false,
            hit_rate: 0.25,
            size: 1 << 16,
            lookup_count: 1 << 18,
        },
        // Scenario {
        //     fake_partitioning: false,
        //     hit_rate: 0.75,
        //     size: 1 << 16,
        //     lookup_count: 1 << 18,
        // },
        // Scenario {
        //     fake_partitioning: false,
        //     hit_rate: 0.95,
        //     size: 1 << 16,
        //     lookup_count: 1 << 18,
        // },
        // Scenario {
        //     fake_partitioning: true,
        //     hit_rate: 0.5,
        //     size: 1 << 16,
        //     lookup_count: 1 << 18,
        // },
        // Scenario {
        //     fake_partitioning: true,
        //     hit_rate: 0.75,
        //     size: 1 << 16,
        //     lookup_count: 1 << 18,
        // },
        // Scenario {
        //     fake_partitioning: true,
        //     hit_rate: 0.95,
        //     size: 1 << 16,
        //     lookup_count: 1 << 18,
        // },
    ];


    let runtime = Builder::new_current_thread().enable_all().build().unwrap();
    // TODO rename
    let mut group = c.benchmark_group("compare_lookup_time");

    for scenario in scenarios {
        group.throughput(Throughput::Elements((scenario.lookup_count) as u64));

        group.bench_function(BenchmarkId::new("hashbrown", scenario.name()), |b| {
            let mut rng = StdRng::seed_from_u64(RNG_SEED);
            b.iter_batched_ref(
                || {
                    let entries = generate_entries(&mut rng, scenario.partitioned_entries());
                    let raw_table = build_raw_table(&entries);
                    let lookup_keys = generate_lookup_keys(
                        &entries,
                        scenario.lookup_count,
                        scenario.hit_rate,
                        &mut rng,
                    );
                    assert_eq!(lookup_keys.len(), scenario.lookup_count);
                    let output = vec![0usize; lookup_keys.len()];
                    (raw_table, lookup_keys, output)
                },
                |(table, lookup_keys, output)| {
                    for (i, key) in lookup_keys.iter().enumerate() {
                        if let Some(value) = lookup_raw(&table, *key) {
                            output[i] = value;
                        }
                    }
                },
                LargeInput,
            );
        });

        if scenario.fake_partitioning == false {
            group.bench_function(BenchmarkId::new("new_map_3 (reserve zero, hybrid probe)", scenario.name()), |b| {
                new_map_benchmark::<Group8ReserveZero, HybridProbeSequence<8>>(&runtime, scenario, b);
            });

            // group.bench_function(BenchmarkId::new("bulk 4 new_map_3 (reserve zero, hybrid probe)", scenario.name()), |b| {
            //     new_bulk_map_benchmark::<4, Group8ReserveZero, HybridProbeSequence<8>>(&runtime, scenario, b);
            // });
            //
            // group.bench_function(BenchmarkId::new("bulk 8 new_map_3 (reserve zero, hybrid probe)", scenario.name()), |b| {
            //     new_bulk_map_benchmark::<8, Group8ReserveZero, HybridProbeSequence<8>>(&runtime, scenario, b);
            // });
            //
            // group.bench_function(BenchmarkId::new("bulk 16 new_map_3 (reserve zero, hybrid probe)", scenario.name()), |b| {
            //     new_bulk_map_benchmark::<16, Group8ReserveZero, HybridProbeSequence<8>>(&runtime, scenario, b);
            // });

            // group.bench_function(BenchmarkId::new("new_map_3 (spread empty, hybrid probe)", scenario.name()), |b| {
            //     new_map_benchmark::<Group8SpreadEmpty, HybridProbeSequence<8>>(&runtime, scenario, b);
            // });
            //
            // group.bench_function(BenchmarkId::new("new_map_3 (top7, hybrid probe)", scenario.name()), |b| {
            //     new_map_benchmark::<Group8SwissPlus, HybridProbeSequence<8>>(&runtime, scenario, b);
            // });
            //
            // group.bench_function(BenchmarkId::new("new_map_3 (reserve zero, hybrid probe slide 8)", scenario.name()), |b| {
            //     new_map_benchmark::<Group8ReserveZero, HybridProbeSequenceSlide<8, 8, 8>>(&runtime, scenario, b);
            // });
            //
            // group.bench_function(BenchmarkId::new("new_map_3 (spread empty, hybrid probe slide 8)", scenario.name()), |b| {
            //     new_map_benchmark::<Group8SpreadEmpty, HybridProbeSequenceSlide<8, 8, 8>>(&runtime, scenario, b);
            // });
            //
            // group.bench_function(BenchmarkId::new("new_map_3 (top7, hybrid probe slide 8)", scenario.name()), |b| {
            //     new_map_benchmark::<Group8SwissPlus, HybridProbeSequenceSlide<8, 8, 8>>(&runtime, scenario, b);
            // });
            //
            // group.bench_function(BenchmarkId::new("new_map_3 (reserve zero, swiss probe)", scenario.name()), |b| {
            //     new_map_benchmark::<Group8ReserveZero, SwissTableProbeSeq<8>>(&runtime, scenario, b);
            // });
            //
            // group.bench_function(BenchmarkId::new("new_map_3 (spread empty, swiss probe)", scenario.name()), |b| {
            //     new_map_benchmark::<Group8SpreadEmpty, SwissTableProbeSeq<8>>(&runtime, scenario, b);
            // });
            //
            // group.bench_function(BenchmarkId::new("new_map_3 (top7, swiss probe)", scenario.name()), |b| {
            //     new_map_benchmark::<Group8SwissPlus, SwissTableProbeSeq<8>>(&runtime, scenario, b);
            // });
        }
    }

    group.finish();
}

fn new_map_benchmark<G: GroupStrategy + IterableGroupStrategy + 'static, P: ProbeSequence + 'static>(runtime: &Runtime, scenario: Scenario, b: &mut Bencher) {
    let mut rng = StdRng::seed_from_u64(RNG_SEED);
    b.iter_batched_ref(
        || {
            let entries = generate_entries(&mut rng, scenario.entries());
            let write_only_table = build_write_only_table::<G, P>(&runtime, &entries);
            let lookup_keys = generate_lookup_keys(
                &entries,
                scenario.lookup_count,
                scenario.hit_rate,
                &mut rng,
            );
            assert_eq!(lookup_keys.len(), scenario.lookup_count);
            let output = vec![0usize; lookup_keys.len()];
            (write_only_table, lookup_keys, output)
        },
        |(write_only_table, lookup_keys, output)| {
            for (i, key) in lookup_keys.iter().enumerate() {
                if let Some(value) = write_only_table.get(*key) {
                    output[i] = *value;
                }
            }
        },
        LargeInput,
    );
}

fn new_bulk_map_benchmark<const N: usize, G: BulkGroupStrategyN + IterableGroupStrategy + 'static, P: ProbeSequence + ProbeSequenceBulkN + 'static>(runtime: &Runtime, scenario: Scenario, b: &mut Bencher) {
    assert_eq!(scenario.lookup_count % N, 0);

    let mut rng = StdRng::seed_from_u64(RNG_SEED);
    b.iter_batched_ref(
        || {
            let entries = generate_entries(&mut rng, scenario.entries());
            let read_only_table = build_write_only_table::<G, P>(&runtime, &entries);
            let lookup_keys = generate_lookup_keys(
                &entries,
                scenario.lookup_count,
                scenario.hit_rate,
                &mut rng,
            );
            assert_eq!(lookup_keys.len(), scenario.lookup_count);
            let output: Vec<Option<usize>> = vec![None; lookup_keys.len()];
            (read_only_table, lookup_keys, output)
        },
        |(read_only_table, lookup_keys, output)| {
            for (keys, output) in lookup_keys.iter()
                .copied()
                .array_chunks::<N>()
                .zip(output.iter_mut().array_chunks::<N>()) {
                read_only_table.get_in_bulk_static_n::<N>(&keys, output);
            }
            // for (i, key) in lookup_keys.iter().enumerate() {
            //     if let Some(value) = read_only_table.get(*key) {
            //         output[i] = *value;
            //     }
            // }
        },
        LargeInput,
    );
}

fn generate_entries(rng: &mut StdRng, count: usize) -> Vec<(u64, usize)> {
    let mut entries = Vec::with_capacity(count);
    for _ in 0..count {
        entries.push((rng.gen::<u64>(), rng.gen::<usize>()));
    }
    entries
}

fn generate_lookup_keys(
    entries: &[(u64, usize)],
    lookup_count: usize,
    hit_rate: f64,
    rng: &mut StdRng,
) -> Vec<u64> {
    let mut lookups = Vec::with_capacity(lookup_count);
    let clamped_hit_rate = hit_rate.clamp(0.0, 1.0);
    let mut hit_count = ((lookup_count as f64) * clamped_hit_rate).round() as usize;
    hit_count = hit_count.min(lookup_count);

    // Add the hits to the vec
    for idx in rng.sample_iter(Uniform::new(0, entries.len())).take(hit_count) {
        lookups.push(entries[idx].0);
    }

    // Add random misses
    let key_set: HashSet<u64> = entries.iter().map(|(hash, _)| *hash).collect();
    while lookups.len() < lookup_count {
        let candidate = rng.gen::<u64>();
        if !key_set.contains(&candidate) {
            lookups.push(candidate);
        }
    }

    lookups.shuffle(rng);
    lookups
}

fn build_write_only_table<G, P>(runtime: &Runtime, entries: &[(u64, usize)]) -> Arc<ReadOnlyTable<usize, G, P>>
where G: GroupStrategy + IterableGroupStrategy + 'static,
    P: ProbeSequence + 'static,
{
    let mut table = WriteOnlyTable::<_, G, P>::new();
    for &(hash, value) in entries {
        let _ = table.insert(hash, value);
    }
    runtime.block_on(table.compact())
}

fn build_raw_table(entries: &[(u64, usize)]) -> RawTable<(u64, usize)> {
    let mut table = RawTable::with_capacity(entries.len());
    for &(hash, value) in entries {
        match table.find_or_find_insert_slot(
            hash,
            |(existing, _)| existing == &hash,
            |(existing, _)| *existing,
        ) {
            Ok(bucket) => unsafe {
                bucket.as_mut().1 = value;
            },
            Err(slot) => unsafe {
                table.insert_in_slot(hash, slot, (hash, value));
            },
        }
    }
    table
}

#[inline]
fn lookup_raw(table: &RawTable<(u64, usize)>, hash: u64) -> Option<usize> {
    match table.get(hash, |(existing, _)| existing == &hash) {
        None => None,
        Some((_, value)) => Some(*value)
    }
}
