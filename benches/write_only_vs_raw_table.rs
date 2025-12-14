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
const LOOKUP_COUNT: usize = 1 << 12;
const HIT_RATE: f64 = 0.7;
const PARTITIONS: usize = 8;
const RNG_SEED: u64 = 0xfeed_babe_d00d_beef;

#[derive(Clone, Copy)]
struct Scenario {
    name: &'static str,
    raw_table_entries: usize,
}

const SCENARIOS: [Scenario; 2] = [
    Scenario {
        name: "equal_writes",
        raw_table_entries: TOTAL_KEYS,
    },
    Scenario {
        name: "hash_partitioned",
        raw_table_entries: TOTAL_KEYS / PARTITIONS,
    },
];

fn criterion_benchmark(c: &mut Criterion) {
    let runtime = Builder::new_current_thread().enable_all().build().unwrap();
    let mut rng = StdRng::seed_from_u64(RNG_SEED);

    let entries = generate_entries(&mut rng, TOTAL_KEYS);
    let write_only_table = build_write_only_table(&runtime, &entries);
    let write_lookup_keys = Arc::new(generate_lookup_keys(&entries, LOOKUP_COUNT, HIT_RATE, &mut rng));

    let mut group = c.benchmark_group("WriteOnly_vs_RawTable");

    for scenario in SCENARIOS {
        group.throughput(Throughput::Elements(LOOKUP_COUNT as u64));
        let raw_entries = &entries[..scenario.raw_table_entries];
        let raw_table = Arc::new(build_raw_table(raw_entries));
        let raw_lookup_keys = Arc::new(generate_lookup_keys(raw_entries, LOOKUP_COUNT, HIT_RATE, &mut rng));

        let write_table_clone = Arc::clone(&write_only_table);
        let write_lookup_clone = Arc::clone(&write_lookup_keys);
        group.bench_function(BenchmarkId::new("write_only_table", scenario.name), move |b| {
            b.iter(|| {
                let mut acc = 0usize;
                for key in write_lookup_clone.iter() {
                    if let Some(value) = write_table_clone.get(*key) {
                        acc = acc.wrapping_add(*value);
                    }
                }
                black_box(acc);
            });
        });

        let raw_table_clone = Arc::clone(&raw_table);
        let raw_lookup_clone = Arc::clone(&raw_lookup_keys);
        group.bench_function(BenchmarkId::new("raw_table", scenario.name), move |b| {
            b.iter(|| {
                let mut acc = 0usize;
                for key in raw_lookup_clone.iter() {
                    if let Some(value) = lookup_raw(&raw_table_clone, *key) {
                        acc = acc.wrapping_add(value);
                    }
                }
                black_box(acc);
            });
        });
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
    if entries.is_empty() {
        return (0..lookup_count).map(|_| rng.gen::<u64>()).collect();
    }

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

fn configure_criterion() -> Criterion {
    Criterion::default()
        .warm_up_time(Duration::from_secs(5))
        .measurement_time(Duration::from_secs(15))
}

criterion_group! {
    name = benches;
    config = configure_criterion();
    targets = criterion_benchmark
}
criterion_main!(benches);
