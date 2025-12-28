use datafusion_parallelism::operator::version10::new_map_3::group::probe_hybrid::{HybridProbeSequence, HybridProbeSequenceSlide};
use datafusion_parallelism::operator::version10::new_map_3::group::probe_sequence::ProbeSequence;
use datafusion_parallelism::operator::version10::new_map_3::group::probe_swiss::SwissTableProbeSeq;
use futures::stream::FuturesUnordered;
use futures::TryStreamExt;
use rand::distributions::Uniform;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

const GROUP_SIZE: usize = 8;

#[derive(Debug, Clone, Copy)]
enum TagMethod {
    Top7Bits,
    Top8Bits,
    Top8BitsReserveZero,
    Top8BitsSplitZero,
}

impl TagMethod {
    fn name(&self) -> &str {
        match self {
            TagMethod::Top7Bits => "Top 7 bits (hash >> 57)",
            TagMethod::Top8Bits => "Top 8 bits (hash >> 56)",
            TagMethod::Top8BitsReserveZero => "Top 8 bits, reserve 0 (max(1, hash >> 56))",
            TagMethod::Top8BitsSplitZero => "Top 8 bits, split 0",
        }
    }

    fn compute_tag(&self, hash: u64) -> u8 {
        match self {
            TagMethod::Top7Bits => (hash >> 57) as u8,
            TagMethod::Top8Bits => (hash >> 56) as u8,
            TagMethod::Top8BitsReserveZero => ((hash >> 56) as u8).max(1),
            TagMethod::Top8BitsSplitZero => {
                let top_bits = (hash >> 46) as u16;                 // take 16 bits
                let raw = top_bits % 255;
                raw as u8
            }
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum ProbeTagMethod {
    Tag,
    Top8Bits,
    Next8Bits,
}

impl ProbeTagMethod {
    fn name(&self) -> &str {
        match self {
            ProbeTagMethod::Tag => "Tag",
            ProbeTagMethod::Top8Bits => "Top8Bits",
            ProbeTagMethod::Next8Bits => "Next8Bits",
        }
    }

    fn get_probe_tag(&self, tag: u8, hash: u64) -> u8 {
        match self {
            ProbeTagMethod::Tag => tag,
            ProbeTagMethod::Top8Bits => (hash >> 56) as u8,
            ProbeTagMethod::Next8Bits => (hash >> 48) as u8,
        }
    }
}


struct FakeHashMap {
    buckets: Vec<Option<u64>>,
    capacity_mask: usize,
    size: usize,
}

impl FakeHashMap {
    fn new(capacity: usize) -> Self {
        assert!(capacity.is_power_of_two(), "Capacity must be power of 2");
        assert!(capacity >= GROUP_SIZE, "Capacity must be >= GROUP_SIZE");

        Self {
            buckets: vec![None; capacity],
            capacity_mask: capacity - 1,
            size: 0,
        }
    }

    fn insert<P: ProbeSequence>(&mut self, hash: u64, tag_method: TagMethod, probe_tag_method: ProbeTagMethod) -> usize {
        let tag = tag_method.compute_tag(hash);
        let probe_bits = probe_tag_method.get_probe_tag(tag, hash);
        let mut index = P::start_index(hash, self.capacity_mask);
        let mut stride = 0;
        let mut probe_length = 0;

        loop {
            // Try to find empty slot in the next GROUP_SIZE entries
            for offset in 0..GROUP_SIZE {
                let bucket_index = (index + offset) & self.capacity_mask;
                if self.buckets[bucket_index].is_none() {
                    self.buckets[bucket_index] = Some(hash);
                    self.size += 1;
                    return probe_length;
                }
            }

            // No empty slot found, move to next probe location
            index = P::next(index, &mut stride, hash, probe_bits, self.capacity_mask);
            probe_length += 1;
        }
    }

    fn len(&self) -> usize {
        self.size
    }

    fn get<P: ProbeSequence>(&self, hash: u64, tag_method: TagMethod, probe_tag_method: ProbeTagMethod) -> LookupStats {
        let search_tag = tag_method.compute_tag(hash);
        let probe_bits = probe_tag_method.get_probe_tag(search_tag, hash);
        let mut index = P::start_index(hash, self.capacity_mask);
        let mut stride = 0;

        let mut tag_comparisons = 0;
        let mut hash_comparisons = 0;

        loop {
            // Check if the current group has any empty slots
            let mut has_empty_slot = false;

            // Check all entries in the current group
            for offset in 0..GROUP_SIZE {
                let bucket_index = (index + offset) & self.capacity_mask;

                match self.buckets[bucket_index] {
                    None => {
                        has_empty_slot = true;
                    }
                    Some(stored_hash) => {
                        let stored_tag = tag_method.compute_tag(stored_hash);

                        // If tags match, we need to do a full hash comparison
                        if stored_tag == search_tag {
                            // Check if it's actually the same hash
                            if stored_hash == hash {
                                return LookupStats {
                                    found: true,
                                    tag_comparisons,
                                    hash_comparisons,
                                };
                            } else {
                                hash_comparisons += 1;
                            }
                        }
                        tag_comparisons += 1;
                    }
                }
            }

            // If we found an empty slot, the value is not in the map
            if has_empty_slot {
                return LookupStats {
                    found: false,
                    tag_comparisons,
                    hash_comparisons,
                };
            }

            // Move to next probe location
            index = P::next(index, &mut stride, hash, probe_bits, self.capacity_mask);
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct LookupStats {
    found: bool,
    tag_comparisons: usize,
    hash_comparisons: usize,
}

async fn benchmark_probe_sequence<P: ProbeSequence>(
    capacity: usize,
    load_factor: f64,
    tag_method: TagMethod,
    probe_tag_method: ProbeTagMethod,
) -> f64 {
    const TASKS: usize = 8;
    const ITERATIONS: usize = 1000;

    let x = (0..TASKS).into_iter()
        .map(|task| {
            tokio::spawn(async move {
                let mut sum_avg_probe_length = 0.0;

                for iteration in 0..ITERATIONS {
                    let seed = task * ITERATIONS + iteration;
                    let mut rng = StdRng::seed_from_u64(seed as u64);

                    let mut map = FakeHashMap::new(capacity);
                    let target_size = (capacity as f64 * load_factor) as usize;

                    // Find unique random hashes
                    // let mut hashes = HashSet::new();
                    // while hashes.len() < target_size {
                    //     hashes.insert(rng.gen::<u64>());
                    // }
                    // let mut hashes = hashes.into_iter().collect::<Vec<u64>>();
                    // hashes.shuffle(&mut rng);

                    let mut total_probe_length = 0;
                    // for hash in hashes {
                    //     let probe_length = map.insert::<P>(hash, tag_method, probe_tag_method);
                    //     total_probe_length += probe_length;
                    // }

                    let mut previous_size = 0;
                    while map.len() < target_size {
                        // let hash = random_state.hash_one(rng.gen::<u64>());
                        let hash = rng.gen::<u64>();
                        let probe_length = map.insert::<P>(hash, tag_method, probe_tag_method);

                        if previous_size != map.len() {
                            previous_size = map.len();
                            // Inserted a unique key
                            total_probe_length += probe_length;
                        }
                    }

                    let avg_probe_length = total_probe_length as f64 / target_size as f64;
                    sum_avg_probe_length += avg_probe_length;
                }

                sum_avg_probe_length
            })
        })
        .collect::<FuturesUnordered<_>>()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();

    let sum_avg_probe_length: f64 = x.into_iter().sum();
    sum_avg_probe_length / ITERATIONS as f64 / TASKS as f64
}

fn benchmark_lookup_efficiency<P: ProbeSequence>(
    capacity: usize,
    load_factor: f64,
    tag_method: TagMethod,
    probe_tag_method: ProbeTagMethod,
) -> (f64, f64) {
    const ITERATIONS: usize = 30;

    let mut sum_avg_hash_comparisons = 0.0;
    let mut sum_miss_avg_hash_comparisons = 0.0;

    for iteration in 0..ITERATIONS {
        let mut rng = StdRng::seed_from_u64(42 + iteration as u64);
        let mut map = FakeHashMap::new(capacity);
        let target_size = (capacity as f64 * load_factor) as usize;
        let mut inserted_hashes = Vec::new();

        // Insert values
        let mut previous_len = 0;
        while map.len() < target_size {
            let hash = rng.gen::<u64>();
            map.insert::<P>(hash, tag_method, probe_tag_method);
            if map.len() != previous_len {
                // Hash was unique
                inserted_hashes.push(hash);
                previous_len = map.len();
            }
        }

        // Benchmark successful lookups
        let mut total_hash_comparisons = 0;
        let mut successful_lookups = 0;

        for &hash in &inserted_hashes {
            let stats = map.get::<P>(hash, tag_method, probe_tag_method);
            assert!(stats.found, "Should find all inserted values");
            total_hash_comparisons += stats.hash_comparisons;
            successful_lookups += 1;
        }

        // Benchmark unsuccessful lookups (values not in the map)
        let rng_miss = StdRng::seed_from_u64(12345 + iteration as u64);
        let mut miss_hash_comparisons = 0;

        let missed_keys = rng_miss.sample_iter(Uniform::new_inclusive(0, u64::MAX))
            .filter(|key| !inserted_hashes.contains(key))
            .take(target_size)
            .collect::<Vec<_>>();

        for hash in missed_keys {
            let stats = map.get::<P>(hash, tag_method, probe_tag_method);
            miss_hash_comparisons += stats.hash_comparisons;
        }

        let avg_hash_comparisons = total_hash_comparisons as f64 / successful_lookups as f64;
        let miss_avg_hash_comparisons = if target_size > 0 {
            miss_hash_comparisons as f64 / target_size as f64
        } else {
            0.0
        };

        sum_avg_hash_comparisons += avg_hash_comparisons;
        sum_miss_avg_hash_comparisons += miss_avg_hash_comparisons;
    }

    let final_avg_hash_comparisons = sum_avg_hash_comparisons / ITERATIONS as f64;
    let final_miss_avg_hash_comparisons = sum_miss_avg_hash_comparisons / ITERATIONS as f64;

    (final_avg_hash_comparisons, final_miss_avg_hash_comparisons)
}

// Macro to help run probe benchmarks in a loop
macro_rules! run_probe_scenarios {
    ($capacity:expr, $load_factors:expr, [$(($probe_type:ty, $name:expr, $tag_method:expr)),* $(,)?]) => {
        $(
            bench_probe::<$probe_type>($name, $tag_method, $capacity, $load_factors).await;
        )*
    };
}

// Macro to help run lookup benchmarks in a loop
macro_rules! run_lookup_scenarios {
    ($capacity:expr, $load_factors:expr, [$(($probe_type:ty, $name:expr, $tag_method:expr)),* $(,)?]) => {
        $(
            bench_lookup::<$probe_type>($name, $tag_method, $capacity, $load_factors).await;
        )*
    };
}

async fn bench_probe<P: ProbeSequence>(
    name: &str,
    tag_method: TagMethod,
    capacity: usize,
    load_factors: &[f64],
) -> Vec<(f64, f64)> {
    println!("\n{} (GROUP_SIZE={})", name, P::GROUP_SIZE);
    println!("Tag method: {}", tag_method.name());

    let mut results = Vec::new();

    for &load_factor in load_factors {
        let avg_probe_length = benchmark_probe_sequence::<P>(
            capacity,
            load_factor,
            tag_method,
            ProbeTagMethod::Tag,
        ).await;

        println!(
            "Load factor: {:.0}% | Avg probe length: {:.8}",
            load_factor * 100.0,
            avg_probe_length
        );

        results.push((load_factor, avg_probe_length));
    }

    results
}

async fn bench_lookup<P: ProbeSequence>(
    name: &str,
    tag_method: TagMethod,
    capacity: usize,
    load_factors: &[f64],
) -> Vec<(f64, f64, f64)> {
    println!("\n{} (GROUP_SIZE={})", name, P::GROUP_SIZE);
    println!("Tag method: {}", tag_method.name());

    let mut results = Vec::new();

    for &load_factor in load_factors {
        let (successful_avg, unsuccessful_avg) = benchmark_lookup_efficiency::<P>(
            capacity,
            load_factor,
            tag_method,
            ProbeTagMethod::Tag,
        );

        println!("Load factor {:.0}%", load_factor * 100.0);
        println!("  Successful lookups:   {:.8}", successful_avg);
        println!("  Unsuccessful lookups: {:.8}", unsuccessful_avg);

        results.push((load_factor, successful_avg, unsuccessful_avg));
    }

    results
}

#[tokio::main]
async fn main() {
    validate_split_zero_reserved_value();
    validate_explicit_tests();
    validate_probes_visit_every_bucket_once();

    const CAPACITY_POWER: usize = 15;
    let capacity = 1 << CAPACITY_POWER;
    let load_factors = vec![0.5, 0.75, 0.95];

    println!("Probe Sequence Comparison");
    println!("========================\n");

    // Part 1: Insertion performance (probe length)
    println!("\n{}", "█".repeat(70));
    println!("PART 1: INSERTION PERFORMANCE (Probe Chain Length)");
    println!("{}", "█".repeat(70));

    run_probe_scenarios!(capacity, &load_factors, [
        (SwissTableProbeSeq<8>, "SwissTable probe", TagMethod::Top7Bits),
        (HybridProbeSequence<8>, "Hybrid probe", TagMethod::Top7Bits),
        (HybridProbeSequence<8>, "Hybrid probe", TagMethod::Top8Bits),
        (HybridProbeSequence<8>, "Hybrid probe", TagMethod::Top8BitsReserveZero),
        (HybridProbeSequence<8>, "Hybrid probe", TagMethod::Top8BitsSplitZero),
        // The tag method shouldn't matter at all here. Changing the slide amount shouldn't change the
        // results at all
        (HybridProbeSequenceSlide<8, 7, 0>, "Hybrid probe [take 7]", TagMethod::Top8Bits),
        (HybridProbeSequenceSlide<8, 8, 0>, "Hybrid probe [take 8]", TagMethod::Top8Bits),
        (HybridProbeSequenceSlide<8, 16, 0>, "Hybrid probe [take 16]", TagMethod::Top8Bits),
    ]);

    // Part 2: Lookup performance (false positive rate)
    println!("\n\n{}", "█".repeat(70));
    println!("PART 2: LOOKUP PERFORMANCE (False Positive Rate)");
    println!("{}", "█".repeat(70));

    run_lookup_scenarios!(capacity, &load_factors, [
        (SwissTableProbeSeq<8>, "SwissTable probe", TagMethod::Top7Bits),
        (SwissTableProbeSeq<8>, "SwissTable probe", TagMethod::Top8Bits),
        (SwissTableProbeSeq<8>, "SwissTable probe", TagMethod::Top8BitsReserveZero),
        (SwissTableProbeSeq<8>, "SwissTable probe", TagMethod::Top8BitsSplitZero),
        (HybridProbeSequence<8>, "Hybrid probe", TagMethod::Top7Bits),
        (HybridProbeSequence<8>, "Hybrid probe", TagMethod::Top8Bits),
        (HybridProbeSequence<8>, "Hybrid probe", TagMethod::Top8BitsReserveZero),
        (HybridProbeSequence<8>, "Hybrid probe", TagMethod::Top8BitsSplitZero),
        // The tag method shouldn't matter much here
        (HybridProbeSequenceSlide<8, 7, 0>, "Hybrid probe [take 7]", TagMethod::Top8Bits),
        (HybridProbeSequenceSlide<8, 8, 0>, "Hybrid probe [take 8]", TagMethod::Top8Bits),
        (HybridProbeSequenceSlide<8, 8, 8>, "Hybrid probe [take 8, skip 8]", TagMethod::Top8Bits),
        (HybridProbeSequenceSlide<8, 16, 8>, "Hybrid probe [take 16, skip 8]", TagMethod::Top8Bits),
        (HybridProbeSequenceSlide<8, { 64 - 8 - CAPACITY_POWER }, 8>, "Hybrid probe [take max, skip 8]", TagMethod::Top8Bits),
    ]);

    // for (tag_method, probe_tag_method) in &scenarios {
    //     println!("\n{}", "=".repeat(70));
    //     println!("TAG METHOD: {}", tag_method.name());
    //     println!("PROBE BITS: {}", probe_tag_method.name());
    //     println!("{}", "=".repeat(70));
    //
    //     benchmark_lookup_efficiency::<SwissTableProbeSeq<GROUP_SIZE>>(
    //         "SwissTableProbeSeq",
    //         capacity,
    //         &load_factors,
    //         *tag_method,
    //         *probe_tag_method,
    //     );
    //
    //     benchmark_lookup_efficiency::<HybridProbeSequence<GROUP_SIZE>>(
    //         "HybridProbeSequence",
    //         capacity,
    //         &load_factors,
    //         *tag_method,
    //         *probe_tag_method,
    //     );
    // }
}

// Ensure that the split zero method has a reserved value
fn validate_split_zero_reserved_value() {
    println!("Validate split zero tag method has a reserved value");
    let tag_method = TagMethod::Top8BitsSplitZero;

    for i in 0..u16::MAX {
        let hash = (i as u64) << 46;
        assert_ne!(tag_method.compute_tag(hash), 0b1111_1111, "Split zero tag method should never return u8::MAX. Failed for hash 0x{:016x}", hash);
    }
}

fn validate_probes_visit_every_bucket_once() {
    println!("Validate probes visit every bucket once");

    for buckets in [1 << 6, 1 << 10, 1 << 15] {
        println!("- Buckets: {}", buckets);
        for tag_method in [
            TagMethod::Top7Bits,
            TagMethod::Top8Bits,
            TagMethod::Top8BitsReserveZero,
            TagMethod::Top8BitsSplitZero,
        ] {
            println!("  - TagMethod: {}", tag_method.name());

            println!("    - ProbeSequence: SwissTableProbeSeq<8>");
            validate_probe_visits_every_bucket_once::<SwissTableProbeSeq<8>>(buckets, tag_method);
            println!("    - ProbeSequence: SwissTableProbeSeq<16>");
            validate_probe_visits_every_bucket_once::<SwissTableProbeSeq<16>>(buckets, tag_method);
            println!("    - ProbeSequence: HybridProbeSequence<8>");
            validate_probe_visits_every_bucket_once::<HybridProbeSequence<8>>(buckets, tag_method);
            println!("    - ProbeSequence: HybridProbeSequence<16>");
            validate_probe_visits_every_bucket_once::<HybridProbeSequence<16>>(buckets, tag_method);
            println!("    - ProbeSequence: HybridProbeSequenceSlide<8, 8, 0>");
            validate_probe_visits_every_bucket_once::<HybridProbeSequenceSlide<8, 8, 0>>(buckets, tag_method);
            println!("    - ProbeSequence: HybridProbeSequenceSlide<8, 8, 8>");
            validate_probe_visits_every_bucket_once::<HybridProbeSequenceSlide<8, 8, 8>>(buckets, tag_method);
            println!("    - ProbeSequence: HybridProbeSequenceSlide<8, 16, 0>");
            validate_probe_visits_every_bucket_once::<HybridProbeSequenceSlide<8, 16, 0>>(buckets, tag_method);
            println!("    - ProbeSequence: HybridProbeSequenceSlide<8, 16, 8>");
            validate_probe_visits_every_bucket_once::<HybridProbeSequenceSlide<8, 16, 8>>(buckets, tag_method);
            println!("    - ProbeSequence: HybridProbeSequenceSlide<16, 16, 0>");
            validate_probe_visits_every_bucket_once::<HybridProbeSequenceSlide<16, 16, 0>>(buckets, tag_method);
            println!("    - ProbeSequence: HybridProbeSequenceSlide<16, 16, 16>");
            validate_probe_visits_every_bucket_once::<HybridProbeSequenceSlide<16, 16, 16>>(buckets, tag_method);
        }
    }
}

fn validate_probe_visits_every_bucket_once<P: ProbeSequence>(buckets: usize, tag_method: TagMethod) {
    const ITERATIONS: usize = 1000;

    let mut rng = StdRng::seed_from_u64(42);
    for _ in 0..ITERATIONS {
        let capacity = buckets * P::GROUP_SIZE;
        assert!(capacity.is_power_of_two());

        let mut visited = vec![false; capacity];
        let capacity_mask = capacity - 1;
        let hash = rng.gen::<u64>();
        let tag = tag_method.compute_tag(hash);

        // Check that each bucket hasn't been visited before
        let mut index = P::start_index(hash, capacity_mask);
        let mut stride = 0;
        for probe_index in 0..buckets {
            visit_buckets(index, &mut visited, hash, probe_index, P::GROUP_SIZE);
            index = P::next(index, &mut stride, hash, tag, capacity_mask);
        }

        // Check that every bucket has been visited
        for (i, visited) in visited.iter().enumerate() {
            assert!(visited, "Bucket {} not visited. Hash {}, tag: {}", i, hash, tag);
        }
    }
}

fn visit_buckets(offset: usize, visited: &mut [bool], hash: u64, probe_index: usize, group_size: usize) {
    for i in 0..group_size {
        let index = (offset + i) % visited.len();
        assert!(!visited[index], "Bucket {} visited more than once. Hash {}, probe_index: {}", index, hash, probe_index);
        visited[index] = true;
    }
}

fn validate_explicit_tests() {
    println!("Validate explicit probe scenarios");

    let capacity = 1 << 8;
    let hashes = [
        0x3300_0000_0000_0000u64,
        0x3300_0000_1000_0000u64,
        0x3300_0000_2000_0000u64,
        0x3300_0000_3000_0000u64,
        0x3300_0000_4000_0000u64,
        0x3300_0000_5000_0000u64,
        0x3300_0000_6000_0000u64,
        0x3300_0000_7000_0000u64,
        0x3300_0000_8000_0000u64,
        0x3300_0000_9000_0000u64,
        0x3300_0000_a000_0000u64,
        0x3300_0000_b000_0000u64,
        0x3300_0000_c000_0000u64,
        0x3300_0000_d000_0000u64,
        0x3300_0000_e000_0000u64,
        0x3300_0000_f000_0000u64,
    ];



    // Each of these hashes should be assigned to the same bucket since their lower bits are all the
    // same
    let mut map = FakeHashMap::new(capacity);
    for hash in &hashes[0..8] {
        map.insert::<HybridProbeSequenceSlide<8, 8, 0>>(*hash, TagMethod::Top7Bits, ProbeTagMethod::Tag);
    }

    // When looking for a similar hash with a different 7th bit, the normal tag method should cause
    // many hash comparisons
    let lookup = map.get::<HybridProbeSequenceSlide<8, 8, 0>>(
        0x3200_0000_0000_0000u64,
        TagMethod::Top7Bits,
        ProbeTagMethod::Tag,
    );
    assert_eq!(lookup.found, false);
    assert_eq!(lookup.hash_comparisons, 8);

    // Using more bits of the tag should cause fewer hash comparisons
    let lookup = map.get::<HybridProbeSequenceSlide<8, 8, 0>>(
        0x3200_0000_0000_0000u64,
        TagMethod::Top8Bits,
        ProbeTagMethod::Tag,
    );
    assert_eq!(lookup.found, false);
    assert_eq!(lookup.hash_comparisons, 0);


    // Using different bits for the probe and tag should reduce hash comparisons.
    // First we build the map using the top 8 bits for the probe, which causes all the hashes with
    // to be put into the same two buckets
    let mut map = FakeHashMap::new(capacity);
    for hash in &hashes {
        map.insert::<HybridProbeSequenceSlide<8, 8, 0>>(*hash, TagMethod::Top8Bits, ProbeTagMethod::Tag);
    }

    // A similar value with the same upper and lower bits will generate lots of hash comparisons
    let lookup = map.get::<HybridProbeSequenceSlide<8, 8, 0>>(
        0x3300_0000_ff00_0000u64,
        TagMethod::Top8Bits,
        ProbeTagMethod::Tag,
    );
    assert_eq!(lookup.found, false);
    assert_eq!(lookup.hash_comparisons, 16);
    // A value with different tag bits will have 0 hash comparisons
    let lookup = map.get::<HybridProbeSequenceSlide<8, 8, 0>>(
        0x4300_0000_ff00_0000u64,
        TagMethod::Top8Bits,
        ProbeTagMethod::Tag,
    );
    assert_eq!(lookup.found, false);
    assert_eq!(lookup.hash_comparisons, 0);


    // However, when using different bits from the tag and probe, there should be fewer hash comparisons
    let mut map = FakeHashMap::new(capacity);
    for hash in &hashes {
        map.insert::<HybridProbeSequenceSlide<8, 8, 32>>(*hash, TagMethod::Top7Bits, ProbeTagMethod::Tag);
    }
    let lookup = map.get::<HybridProbeSequenceSlide<8, 8, 32>>(
        0x3300_0000_ff00_0000u64,
        TagMethod::Top8Bits,
        ProbeTagMethod::Tag,
    );
    assert_eq!(lookup.found, false);
    assert_eq!(lookup.hash_comparisons, 8);
    // Values with different tag bits still get 0 hash comparisons
    let lookup = map.get::<HybridProbeSequenceSlide<8, 8, 0>>(
        0x4300_0000_ff00_0000u64,
        TagMethod::Top8Bits,
        ProbeTagMethod::Tag,
    );
    assert_eq!(lookup.found, false);
    assert_eq!(lookup.hash_comparisons, 0);



    // Inserting

    let mut map = FakeHashMap::new(capacity);
    for hash in &hashes {
        map.insert::<HybridProbeSequenceSlide<8, 8, 0>>(*hash, TagMethod::Top7Bits, ProbeTagMethod::Tag);
    }

    // Inserting a key with the same lower and upper bits should cause a long probe chain
    let probe_length = map.insert::<HybridProbeSequenceSlide<8, 8, 0>>(
        0x33ff_0000_0000_0000u64,
        TagMethod::Top7Bits,
        ProbeTagMethod::Tag,
    );
    assert_eq!(probe_length, 2);


    // However, a probe sequence that uses different bits should more easily find an empty bucket
    let mut map = FakeHashMap::new(capacity);
    for hash in &hashes {
        map.insert::<HybridProbeSequenceSlide<8, 8, 8>>(*hash, TagMethod::Top7Bits, ProbeTagMethod::Tag);
    }

    // Inserting a key with the same lower and upper bits should cause a long probe chain
    let probe_length = map.insert::<HybridProbeSequenceSlide<8, 8, 8>>(
        0x33ff_0000_0000_0000u64,
        TagMethod::Top7Bits,
        ProbeTagMethod::Tag,
    );
    assert_eq!(probe_length, 1);


    // A probe sequence that uses even more bits should cause short probe chains
    let mut map = FakeHashMap::new(capacity);
    for hash in &hashes {
        map.insert::<HybridProbeSequenceSlide<8, 16, 16>>(*hash, TagMethod::Top7Bits, ProbeTagMethod::Tag);
    }

    let probe_length = map.insert::<HybridProbeSequenceSlide<8, 16, 16>>(
        0x3300_0001_0000_0000u64,
        TagMethod::Top7Bits,
        ProbeTagMethod::Tag,
    );
    assert_eq!(probe_length, 1);

    // However, there is a limit. Here only the upper bit of the probe bits is different, and due
    // to the small size of the map, this doesn't make a difference to the next bucket
    let probe_length = map.insert::<HybridProbeSequenceSlide<8, 16, 16>>(
        0x3300_1000_0000_0000u64,
        TagMethod::Top7Bits,
        ProbeTagMethod::Tag,
    );
    assert_eq!(probe_length, 2);
}
