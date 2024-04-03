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

use std::io::Write;

use bytes::{BufMut, BytesMut};
use common_base::bytes::Bytes;
use common_base::timestamp::timestamp_ms;
use derive_builder::Builder;
use rocksdb::WriteOptions;
use snafu::ResultExt;

use crate::database::{Database, GetOptions};
use crate::error::{EncodeStringValueSnafu, Result};
use crate::metadata::{Metadata, RedisType};

pub struct StringPair {
    _key: Bytes,
    _value: Bytes,
}

#[derive(Clone, Copy, PartialEq, Eq, Default)]
pub enum StringSetType {
    #[default]
    NONE,
    NX,
    XX,
}

#[derive(Builder, Clone)]
pub struct StringSetArgs {
    ttl: u64,
    #[builder(default)]
    set_type: StringSetType,
    #[builder(default)]
    get: bool,
    #[builder(default)]
    keep_ttl: bool,
}

pub enum StringLCSType {
    NONE,
    LEN,
    IDX,
}

pub struct StringLCSArgs {
    _lcs_type: StringLCSType,
    _min_match_len: usize,
}

pub struct StringLCSRange {
    _start: usize,
    _end: usize,
}

pub struct StringLCSMatchedRange {
    _a: StringLCSRange,
    _b: StringLCSRange,
    _match_len: usize,
}

impl StringLCSMatchedRange {
    pub fn new(a: StringLCSRange, b: StringLCSRange, match_len: usize) -> Self {
        Self {
            _a: a,
            _b: b,
            _match_len: match_len,
        }
    }
}

pub struct StringLCSIdxResult {
    /// Matched ranges.
    _matches: Vec<StringLCSMatchedRange>,
    /// LCS length.
    _len: usize,
}

/// ```plaintext
/// ┌──────────────────────────────────┬────────────┬──────────┐
/// │             flags                │   expire   │  payload │
/// │unused(4bits)-> <- datatype(4bits)│  (8bytes)  │ (Nbytes) │
/// └──────────────────────────────────┴────────────┴──────────┘
/// ```
pub struct RedisString<Database> {
    database: Database,
}

impl<D> RedisString<D>
where
    D: Database,
{
    pub fn new(database: D) -> Self {
        Self { database }
    }

    fn get_value(&self, ns_key: Bytes) -> Result<Option<Bytes>> {
        Ok(self.get_value_and_expire(ns_key)?.map(|(value, _)| value))
    }

    fn get_value_and_expire(&self, ns_key: Bytes) -> Result<Option<(Bytes, u64)>> {
        let raw_value = self.get_raw_value(ns_key)?;
        if let Some(raw_value) = raw_value {
            let offset = Metadata::offset_after_expire();
            let value = raw_value.slice(offset..);
            let metadata = Metadata::decode_from(&mut raw_value.slice(..offset).reader())?;
            Ok(Some((value, metadata.expire())))
        } else {
            Ok(None)
        }
    }

    fn get_raw_value(&self, ns_key: Bytes) -> Result<Option<Bytes>> {
        let raw_metadata = self.database.get_raw_metadata(GetOptions::new(), ns_key)?;
        let raw_value = raw_metadata.clone();
        if let Some(raw_metadata) = raw_metadata {
            let _ = self
                .database
                .parse_metadata(&[RedisType::String], raw_metadata)?;
        }
        Ok(raw_value)
    }

    pub fn get(&self, user_key: Bytes) -> Result<Option<Bytes>> {
        let ns_key = self.database.encode_namespace_prefix(user_key);
        self.get_value(ns_key)
    }

    pub fn set(&self, user_key: Bytes, value: Bytes, args: StringSetArgs) -> Result<Option<Bytes>> {
        let mut expire = 0u64;
        let ns_key = self.database.encode_namespace_prefix(user_key);
        let _guard = self.database.lock_key(ns_key.clone());

        let need_old_value = args.set_type != StringSetType::NONE || args.get || args.keep_ttl;
        let (old_value, old_expire) = if need_old_value {
            match self.get_value_and_expire(ns_key.clone()) {
                Ok(Some((old_value, old_expire))) => (Some(old_value), old_expire),
                Err(err) if args.get => return Err(err),
                _ => (None, 0),
            }
        } else {
            (None, 0)
        };
        match args.set_type {
            StringSetType::NX if old_value.is_some() => {
                // if NX, and the key already exist: return Ok(None)
                return Ok(if args.get { old_value } else { None });
            }
            StringSetType::XX if old_value.is_none() => {
                // if XX, the key didn't exist before: return Ok(None)
                return Ok(if args.get { old_value } else { None });
            }
            _ => (),
        }

        // handle expire
        if args.keep_ttl {
            expire = old_expire;
        }
        if args.ttl > 0 {
            let now = timestamp_ms();
            expire = now + args.ttl;
        }

        let mut new_metadata = Metadata::new(RedisType::String, false);
        new_metadata.set_expire(expire);

        let mut new_raw_value_writer =
            BytesMut::with_capacity(Metadata::offset_after_expire() + value.len()).writer();
        new_metadata.encode_into(&mut new_raw_value_writer)?;
        new_raw_value_writer
            .write(value.as_ref())
            .context(EncodeStringValueSnafu)?;
        let raw_value: Bytes = new_raw_value_writer.into_inner().freeze().into();

        self.update_raw_value(ns_key, raw_value)?;

        Ok(if args.get { old_value } else { None })
    }

    fn default_write_opts(&self) -> WriteOptions {
        WriteOptions::new()
    }

    fn update_raw_value(&self, ns_key: Bytes, raw_value: Bytes) -> Result<()> {
        let mut batch = self.database.get_write_batch();
        batch.put_cf(&self.database.get_cf_ref(), &ns_key[..], &raw_value[..]);
        self.database.write(&self.default_write_opts(), batch)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::database::Roxy;
    use crate::storage::setup_test_storage_for_ut;

    #[test]
    fn test_set_and_get() {
        let storage = setup_test_storage_for_ut();
        let storage = Arc::new(storage);
        let redis_string_db = RedisString::new(Roxy::new(
            storage,
            Bytes::from("string".as_bytes()),
            RedisType::String,
        ));

        let user_key = Bytes::from("user_key".as_bytes());
        let value = Bytes::from("value".as_bytes());
        let args = StringSetArgsBuilder::default()
            .ttl(0)
            .get(true)
            .build()
            .unwrap();
        redis_string_db.set(user_key.clone(), value, args).unwrap();

        let result = redis_string_db.get(user_key).unwrap().unwrap();
        println!(
            "result: {}",
            String::from_utf8(Vec::from(&result[..])).unwrap()
        );
    }
}
