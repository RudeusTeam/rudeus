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

use common_base::bytes::Bytes;
use common_base::lock_pool;
use rocksdb::{AsColumnFamilyRef, WriteBatch, WriteOptions};
use snafu::OptionExt;
use strum::VariantArray;

use crate::error::{DatatypeMismatchedSnafu, KeyExpiredSnafu, KeyNotFoundSnafu, Result};
use crate::metadata::{self, Metadata, RedisType};
use crate::storage::{ColumnFamilyId, Storage};

pub trait Database {
    /// Lock type for a key
    type KeyLock;
    /// Guard type for a key lock
    type KeyLockGuard<'a>
    where
        Self: 'a;

    fn encode_namespace_prefix(&self, user_key: Bytes) -> Bytes {
        metadata::encode_namespace_key(self.namespace(), user_key)
    }

    fn namespace(&self) -> Bytes;

    /// [`get_metadata`] is a helper function to get metadata of a `ns_key` from the database. It will the "raw metadata"
    /// from underlying storage, and then parse the raw metadata
    fn get_metadata(
        &self,
        options: GetOptions,
        types: &[RedisType],
        ns_key: Bytes,
    ) -> Result<Option<metadata::Metadata>> {
        self.get_metadata_and_rest(options, types, ns_key)
            .map(|op| op.map(|(meta, _)| meta))
    }

    /// Get metadata and rest part of value of a `ns_key`.
    fn get_metadata_and_rest(
        &self,
        options: GetOptions,
        types: &[RedisType],
        ns_key: Bytes,
    ) -> Result<Option<(metadata::Metadata, Bytes)>> {
        let raw_data = self.get_raw_data(options, ns_key)?;
        if let Some(raw_data) = raw_data {
            Ok(self.validate_metadata(types, raw_data)?)
        } else {
            Ok(None)
        }
    }

    /// [`get_raw_metadata`] is a helper function to get the
    /// "raw metadata" from the storage engine without parsing
    /// it to [`Metadata`] type.
    ///
    fn get_raw_data(&self, options: GetOptions, ns_key: Bytes) -> Result<Option<Bytes>>;

    /// [`parse_metadata`] parse the [`Metadata`] from input bytes and return
    /// the rest part of the input bytes.
    /// if the input bytes is not a valid metadata, it will return an error.
    /// if the key is expired, it will return None.
    fn validate_metadata(
        &self,
        types: &[RedisType],
        input: Bytes,
    ) -> Result<Option<(Metadata, Bytes)>> {
        let mut reader = input.reader();
        let metadata = Metadata::decode_from(&mut reader)?;
        let rest = reader.into_inner();

        if metadata.expired() {
            return Ok(None);
        }

        if !types.contains(&metadata.datatype()) {
            return DatatypeMismatchedSnafu.fail();
        }

        if metadata.size() == 0 && !metadata.datatype().is_emptyable() {
            return DatatypeMismatchedSnafu.fail();
        }
        Ok(Some((metadata, rest)))
    }

    fn lock_key(&self, key: Bytes) -> Self::KeyLockGuard<'_>;

    fn get_write_batch(&self) -> WriteBatch;

    fn get_cf_ref(&self) -> impl AsColumnFamilyRef;

    fn write(&self, opts: &WriteOptions, batch: WriteBatch) -> Result<()>;

    fn delete(&self, key: Bytes) -> Result<()>;
}

/// [`Roxy`] is a wrapper of storage engine, it provides
/// some  common operations for redis commands.
pub struct Roxy<'s> {
    storage: &'s Storage,
    namespace: Bytes,
    column_family_id: ColumnFamilyId,
}

impl<'s> Roxy<'s> {
    pub fn new(storage: &'s Storage, namespace: Bytes, redis_type: RedisType) -> Self {
        Self {
            storage,
            namespace,
            column_family_id: redis_type.into(),
        }
    }
    pub fn get_cf_id(&self) -> ColumnFamilyId {
        self.column_family_id
    }
}

impl<'s> Database for Roxy<'s> {
    type KeyLock = lock_pool::Mutex;

    type KeyLockGuard<'a> = lock_pool::MutexGuard<'a>
    where Self: 'a;

    fn get_raw_data(&self, options: GetOptions, ns_key: Bytes) -> Result<Option<Bytes>> {
        let mut opts = rocksdb::ReadOptions::default();
        if options.with_snapshot {
            opts.set_snapshot(&self.storage.db().snapshot());
        }
        self.storage.get(&opts, self.column_family_id, &ns_key)
    }

    fn lock_key(&self, key: Bytes) -> Self::KeyLockGuard<'_> {
        self.storage.lock_key(key)
    }

    fn get_write_batch(&self) -> WriteBatch {
        self.storage.get_write_batch()
    }

    fn namespace(&self) -> Bytes {
        self.namespace.clone()
    }

    fn get_cf_ref(&self) -> impl AsColumnFamilyRef {
        self.storage.column_family_handler(self.column_family_id)
    }

    fn write(&self, opts: &WriteOptions, updates: WriteBatch) -> Result<()> {
        self.storage.write(opts, updates)
    }

    fn delete(&self, key: Bytes) -> Result<()> {
        let ns_key = self.encode_namespace_prefix(key);
        let _guard = self.lock_key(ns_key.clone());
        let metadata = self
            .get_metadata(GetOptions::default(), RedisType::VARIANTS, ns_key.clone())?
            .context(KeyNotFoundSnafu)?;
        if metadata.expired() {
            return KeyExpiredSnafu.fail();
        }

        self.storage
            .delete(&WriteOptions::default(), self.get_cf_ref(), ns_key)
    }
}

#[derive(Default)]
pub struct GetOptions {
    with_snapshot: bool,
}

impl GetOptions {
    pub fn new() -> Self {
        Self {
            with_snapshot: false,
        }
    }
}
