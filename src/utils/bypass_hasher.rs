use std::hash::{BuildHasher, Hash, Hasher};
use dashmap::{DashMap, ReadOnlyView};

pub trait ContainsHash {
    fn get_hash(&self) -> u64;
}

struct ContainsHashWrapper<K: ContainsHash> {
    inner: K
}

impl <T: ContainsHash> Hash for ContainsHashWrapper<T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u64(self.inner.get_hash());
    }
}

pub struct WithHash<T> {
    pub inner: T,
    pub hash: u64,
}

impl<T> WithHash<T> {
    pub fn new(inner: T, hash: u64) -> Self {
        Self { inner, hash }
    }
}

impl <T> Hash for WithHash<T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u64(self.hash);
    }
}

#[derive(Debug, Clone)]
pub struct BypassHasher;

impl BuildHasher for BypassHasher {
    type Hasher = BypassHasherInstance;

    fn build_hasher(&self) -> Self::Hasher {
        BypassHasherInstance { hash: None }
    }
}

pub struct BypassHasherInstance {
    hash: Option<u64>,
}

impl Hasher for BypassHasherInstance {
    fn finish(&self) -> u64 {
        self.hash.expect("Bypass hasher not written to")
    }

    fn write(&mut self, _bytes: &[u8]) {
        unimplemented!()
    }

    fn write_u64(&mut self, i: u64) {
        self.hash = Some(i);
    }
}

// pub struct RawDashMap<K: ContainsHash, V> {
//     inner: DashMap<WithHash<K>, V, BypassHasher>
// }

// pub type RawDashMap<K, V> = DashMap<WithHash<K>, V, BypassHasher>;
//
// pub fn new_raw_dash_map<K, V>() -> RawDashMap<K, V> {
//     DashMap::with_hasher(BypassHasher)
// }
//
// pub fn raw_dash_map_with_shard_amount<K, V>(shards: usize) -> RawDashMap<K, V> {
//     DashMap::with_hasher_and_shard_amount(BypassHasher, shards)
// }
//
// pub type RawReadOnlyView<K, V> = ReadOnlyView<WithHash<K>, V, BypassHasher>;

// impl <K: ContainsHash, V> RawDashMap<K, V> {
//     pub fn new() -> Self {
//         Self {
//             inner: DashMap::with_hasher(BypassHasher)
//         }
//     }
//
//     pub fn with_shard_amount(shards: usize) -> Self {
//         Self {
//             inner: DashMap::with_hasher_and_shard_amount(BypassHasher, shards)
//         }
//     }
//
//     // pub fn insert(&self, key: K, value: V) {
//     //     self.inner.insert(ContainsHashWrapper { inner: key }, value);
//     // }
//     //
//     // pub fn get(&self, key: &K) -> Option<V> {
//     //     self.inner.get(&ContainsHashWrapper { inner: key }).map(|v| v.value().clone())
//     // }
//
//     pub fn into_read_only(self) -> RawReadOnlyDashMap<K, V> {
//         RawReadOnlyDashMap { inner: self.inner.into_read_only() }
//     }
// }
//
// pub struct RawReadOnlyDashMap<K: ContainsHash, V> {
//     inner: ReadOnlyView<ContainsHashWrapper<K>, V, BypassHasher>,
// }
