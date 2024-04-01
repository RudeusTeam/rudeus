// Copyright 2024 Rudeus Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::BTreeSet;
use std::hash::{DefaultHasher, Hash, Hasher};

pub type Mutex = parking_lot::Mutex<()>;
pub type MutexGuard<'a> = parking_lot::MutexGuard<'a, ()>;

pub struct LockPool {
    hash_power: usize,
    hash_mask: usize,
    mutex_pool: Vec<parking_lot::Mutex<()>>,
}

impl LockPool {
    pub fn new(hash_power: usize) -> Self {
        let hash_mask = (1 << hash_power) - 1;
        let mut mutex_pool = Vec::with_capacity(1 << hash_power);
        mutex_pool.extend((0..(1 << hash_power)).map(|_| Mutex::new(())));
        Self {
            hash_power,
            hash_mask,
            mutex_pool,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.hash_power == 0
    }

    pub fn len(&self) -> usize {
        1 << self.hash_power
    }

    pub fn lock<K: Hash>(&self, key: K) -> MutexGuard {
        self.mutex_pool[self.hash(key)].lock()
    }

    pub fn get<K: Hash>(&self, key: K) -> &'_ Mutex {
        &self.mutex_pool[self.hash(key)]
    }

    pub fn multi_get<K: Hash>(&self, keys: &[K]) -> Vec<&'_ Mutex> {
        let to_acquire_indexes: BTreeSet<usize> = keys.iter().map(|key| self.hash(key)).collect();
        let mut locks = Vec::with_capacity(to_acquire_indexes.len());
        locks.extend(
            to_acquire_indexes
                .into_iter()
                .map(|idx| &self.mutex_pool[idx]),
        );
        locks
    }

    fn hash<K: Hash>(&self, key: K) -> usize {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        (hasher.finish() as usize) & self.hash_mask
    }
}
