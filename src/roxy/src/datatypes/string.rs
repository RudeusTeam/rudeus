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

use std::os::linux::raw;

use common_base::bytes::Bytes;
use derive_builder::Builder;

use crate::database::{Database, GetOptions};
use crate::error::Result;
use crate::metadata::{self, Metadata, RedisType};

pub struct StringPair {
    key: Bytes,
    value: Bytes,
}

#[derive(Clone, Copy)]
pub enum StringSetType {
    NONE,
    NX,
    XX,
}

#[derive(Builder, Clone)]
pub struct StringSetArgs {
    ttl: u64,
    set_type: StringSetType,
    get: bool,
    keep_ttl: bool,
}

pub enum StringLCSType {
    NONE,
    LEN,
    IDX,
}

pub struct StringLCSArgs {
    lcs_type: StringLCSType,
    min_match_len: usize,
}

pub struct StringLCSRange {
    start: usize,
    end: usize,
}

pub struct StringLCSMatchedRange {
    a: StringLCSRange,
    b: StringLCSRange,
    match_len: usize,
}

impl StringLCSMatchedRange {
    pub fn new(a: StringLCSRange, b: StringLCSRange, match_len: usize) -> Self {
        Self { a, b, match_len }
    }
}

struct StringLCSIdxResult {
    /// Matched ranges.
    matches: Vec<StringLCSMatchedRange>,
    /// LCS length.
    len: usize,
}

/// ```
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

    fn get_value(&self, ns_key: Bytes) -> Result<Bytes> {
        self.get_value_and_expire(ns_key).map(|(value, _)| value)
    }

    fn get_value_and_expire(&self, ns_key: Bytes) -> Result<(Bytes, u64)> {
        let raw_value = self.get_raw_value(ns_key)?;
        let offset = Metadata::offset_after_expire();
        let value = raw_value.slice(offset..);
        let metadata = Metadata::decode_from(&mut raw_value.slice(..offset).reader())?;
        Ok((value, metadata.expire()))
    }

    fn get_raw_value(&self, ns_key: Bytes) -> Result<Bytes> {
        let raw_metadata = self.database.get_raw_metadata(GetOptions::new(), ns_key)?;
        let raw_value = raw_metadata.clone();
        let _ = self
            .database
            .parse_metadata(&[RedisType::String], raw_metadata)?;
        Ok(raw_value)
    }

    pub fn get(&self, user_key: Bytes) -> Result<Bytes> {
        let ns_key = self.database.encode_namespace_prefix(user_key);
        self.get_value(ns_key)
    }

    pub fn set(&self, user_key: Bytes, value: Bytes) -> Result<()> {
        todo!()
    }
}
