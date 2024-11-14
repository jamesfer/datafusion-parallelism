use std::collections::HashMap;
use dashmap::{DashMap, ReadOnlyView};
use crate::utils::bypass_hasher::BypassHasher;

pub type SelfHashMap<V> = HashMap<u64, V, BypassHasher>;
pub type SelfHashDashMap<V> = DashMap<u64, V, BypassHasher>;
pub type SelfHashReadOnlyView<V> = ReadOnlyView<u64, V, BypassHasher>;

pub fn new_self_hash_map<V>() -> SelfHashMap<V> {
    HashMap::with_hasher(BypassHasher)
}

pub fn new_self_hash_map_with_capacity<V>(capacity: usize) -> SelfHashMap<V> {
    HashMap::with_capacity_and_hasher(capacity, BypassHasher)
}

pub fn new_self_dash_map<V>() -> SelfHashDashMap<V> {
    DashMap::with_hasher(BypassHasher)
}
