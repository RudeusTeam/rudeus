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
use derive_builder::Builder;
use rocksdb::WriteOptions;
use snafu::ResultExt;

use crate::database::{Database, GetOptions, Roxy};
use crate::error::{EncodeStringValueSnafu, Result};
use crate::metadata::{Metadata, RedisType};
use crate::storage::Storage;

pub struct StringPair {
    _key: Bytes,
    _value: Bytes,
}

#[derive(Clone, Copy, PartialEq, Eq, Default, strum::EnumString, Debug)]
#[strum(ascii_case_insensitive)]
#[strum(serialize_all = "UPPERCASE")]
pub enum StringSetType {
    #[default]
    NONE,
    NX,
    XX,
}

#[derive(Builder, Clone, Debug)]
pub struct StringSetArgs {
    #[builder(default)]
    pub expire: Option<u64>,
    #[builder(default)]
    pub set_type: StringSetType,
    #[builder(default)]
    pub get: bool,
    #[builder(default)]
    pub keep_ttl: bool,
}

impl StringSetArgs {
    pub fn set_type(&self) -> StringSetType {
        self.set_type
    }
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
pub struct RedisString<'s> {
    database: Roxy<'s>,
}

impl<'s> RedisString<'s> {
    pub fn new(storage: &'s Storage, namespace: Bytes) -> Self {
        Self {
            database: Roxy::new(storage, namespace, RedisType::String),
        }
    }

    fn get_value(&self, ns_key: Bytes) -> Result<Option<Bytes>> {
        Ok(self.get_value_and_expire(ns_key)?.map(|(value, _)| value))
    }

    fn get_value_and_expire(&self, ns_key: Bytes) -> Result<Option<(Bytes, u64)>> {
        Ok(self
            .get_metadata_and_raw_value(ns_key)?
            .map(|(meta, value)| (value, meta.expire())))
    }

    /// if the key is not exist, return `Ok(None)`
    ///
    /// if the key is expired, return `Ok(None)`
    ///
    /// if the related value is invalid, return an error
    fn get_metadata_and_raw_value(&self, ns_key: Bytes) -> Result<Option<(Metadata, Bytes)>> {
        let raw_data = self.database.get_raw_data(GetOptions::new(), ns_key)?;
        if let Some(raw_data) = raw_data {
            Ok(self
                .database
                .validate_metadata(&[RedisType::String], raw_data)?)
        } else {
            Ok(None)
        }
    }

    pub fn get(&self, user_key: impl Into<Bytes>) -> Result<Option<Bytes>> {
        let ns_key = self.database.encode_namespace_prefix(user_key.into());
        self.get_value(ns_key)
    }

    pub fn set(
        &self,
        user_key: impl Into<Bytes>,
        value: impl Into<Bytes>,
        args: &StringSetArgs,
    ) -> Result<Option<Bytes>> {
        let user_key: Bytes = user_key.into();
        let value: Bytes = value.into();
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
        if let Some(arg_expire) = args.expire {
            expire = arg_expire;
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

    pub fn db(&self) -> &Roxy {
        &self.database
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

    use common_telemetry::log::init_ut_logging;

    use super::*;
    use crate::storage::setup_test_storage_for_ut;

    #[test]
    fn test_set_and_get() {
        init_ut_logging();
        let storage = setup_test_storage_for_ut();
        let redis_string_db = RedisString::new(&storage, Bytes::from("ns".as_bytes()));

        let user_key = Bytes::from("user_key".as_bytes());
        let value = Bytes::from("value".as_bytes());
        let args = StringSetArgsBuilder::default()
            .expire(None)
            .get(true)
            .build()
            .unwrap();
        redis_string_db.set(user_key.clone(), value, &args).unwrap();

        let result = redis_string_db.get(user_key).unwrap().unwrap();
        println!(
            "result: {}",
            String::from_utf8(Vec::from(&result[..])).unwrap()
        );
    }
}
