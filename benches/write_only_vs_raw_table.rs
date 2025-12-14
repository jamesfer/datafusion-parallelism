use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use datafusion_parallelism::operator::version10::new_map_3::new_map_3::{ReadOnlyTable, WriteOnlyTable};
use hashbrown::raw::RawTable;
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};
use tokio::runtime::{Builder, Runtime};

const TOTAL_KEYS: usize = 1 << 17;
const LOOKUP_COUNT: usize = 1 << 6;
const HIT_RATE: f64 = 0.5;
const PARTITIONS: usize = 8;
const RNG_SEED: u64 = 0x42;

#[derive(Clone, Copy)]
struct Scenario {
    fake_partitioning: bool,
    hit_rate: f32,
}

impl Scenario {
    fn name(&self) -> String {
        format!(
            "{}/{}",
            if self.fake_partitioning { "partitioned" } else { "equal_sizes" },
            self.hit_rate,
        )
    }

    fn partitioned_entries(&self) -> usize {
        if self.fake_partitioning {
            TOTAL_KEYS / PARTITIONS
        } else {
            TOTAL_KEYS
        }
    }
}

const SCENARIOS: [Scenario; 6] = [
    Scenario {
        fake_partitioning: false,
        hit_rate: 0.5,
    },
    Scenario {
        fake_partitioning: false,
        hit_rate: 0.75,
    },
    Scenario {
        fake_partitioning: false,
        hit_rate: 0.95,
    },
    Scenario {
        fake_partitioning: true,
        hit_rate: 0.5,
    },
    Scenario {
        fake_partitioning: true,
        hit_rate: 0.75,
    },
    Scenario {
        fake_partitioning: true,
        hit_rate: 0.95,
    },
];

fn configure_criterion() -> Criterion {
    Criterion::default()
        .warm_up_time(Duration::from_secs(2))
        .measurement_time(Duration::from_secs(5))
}

criterion_group! {
    name = benches;
    config = configure_criterion();
    targets = criterion_benchmark
}
criterion_main!(benches);

fn criterion_benchmark(c: &mut Criterion) {
    let runtime = Builder::new_current_thread().enable_all().build().unwrap();
    // TODO rename
    let mut group = c.benchmark_group("WriteOnly_vs_RawTable");

    let mut rng = StdRng::seed_from_u64(RNG_SEED);
    let entries = generate_entries(&mut rng, TOTAL_KEYS);

    for scenario in SCENARIOS {
        group.throughput(Throughput::Elements(LOOKUP_COUNT as u64));

        group.bench_function(BenchmarkId::new("hashbrown", scenario.name()), |b| {
            let mut rng = StdRng::seed_from_u64(RNG_SEED);
            let raw_entries = &entries[..scenario.partitioned_entries()];
            let raw_table = build_raw_table(raw_entries);
            let lookup_keys = generate_lookup_keys(raw_entries, LOOKUP_COUNT, HIT_RATE, &mut rng);
            assert_eq!(lookup_keys.len(), LOOKUP_COUNT);

            b.iter(|| {
                let mut acc = 0usize;
                for key in lookup_keys.iter() {
                    if let Some(value) = lookup_raw(&raw_table, *key) {
                        acc = acc.wrapping_add(value);
                    }
                }
                black_box(acc);
            });
        });

        if scenario.fake_partitioning == false {
            group.bench_function(BenchmarkId::new("new_map_3", scenario.name()), |b| {
                let mut rng = StdRng::seed_from_u64(RNG_SEED);
                let write_only_table = build_write_only_table(&runtime, &entries);
                let lookup_keys = generate_lookup_keys(&entries, LOOKUP_COUNT, HIT_RATE, &mut rng);
                assert_eq!(lookup_keys.len(), LOOKUP_COUNT);

                b.iter(move || {
                    let mut acc = 0usize;
                    for key in lookup_keys.iter() {
                        if let Some(value) = write_only_table.get(*key) {
                            acc = acc.wrapping_add(*value);
                        }
                    }
                    black_box(acc);
                });
            });
        }
    }

    group.finish();
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

    for _ in 0..hit_count {
        let idx = rng.gen_range(0..entries.len());
        lookups.push(entries[idx].0);
    }

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

fn build_write_only_table(runtime: &Runtime, entries: &[(u64, usize)]) -> Arc<ReadOnlyTable<usize>> {
    let mut table = WriteOnlyTable::new();
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

fn lookup_raw(table: &RawTable<(u64, usize)>, hash: u64) -> Option<usize> {
    table
        .get(hash, |(existing, _)| existing == &hash)
        .map(|(_, value)| *value)
}
