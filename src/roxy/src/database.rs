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

use std::io::Seek;

use common_base::bytes::{Bytes, StringBytes};

use crate::error::{DatatypeMismatchedSnafu, KeyExpiredSnafu, Result};
use crate::metadata::{self, Metadata, RedisType};
use crate::storage::StorageRef;

pub trait Database {
    fn encode_namespace_prefix(&self, user_key: Bytes) -> Bytes;

    /// Get metadata of a `ns_key`.
    fn get_metadata(
        &self,
        options: GetOptions,
        types: &[RedisType],
        ns_key: Bytes,
    ) -> Result<metadata::Metadata> {
        self.get_metadata_and_rest(options, types, ns_key)
            .map(|(meta, _)| meta)
    }

    /// Get metadata and rest part of value of a `ns_key`.
    fn get_metadata_and_rest(
        &self,
        options: GetOptions,
        types: &[RedisType],
        ns_key: Bytes,
    ) -> Result<(metadata::Metadata, Bytes)> {
        let raw_metadata = self.get_raw_metadata(options, ns_key)?;
        self.parse_metadata(types, raw_metadata)
    }

    fn get_raw_metadata(&self, options: GetOptions, ns_key: Bytes) -> Result<Bytes>;

    fn parse_metadata(&self, types: &[RedisType], input: Bytes) -> Result<(Metadata, Bytes)> {
        let mut reader = input.reader();
        let metadata = Metadata::decode_from(&mut reader)?;
        let rest_offset = reader.stream_position().unwrap() as usize;
        let rest = reader.into_inner().slice(rest_offset..);
        if metadata.expired() {
            return KeyExpiredSnafu.fail();
        }

        if !types.contains(&metadata.datatype()) {
            return DatatypeMismatchedSnafu.fail();
        }

        if metadata.size() == 0 && !metadata.datatype().is_emptyable() {
            return DatatypeMismatchedSnafu.fail();
        }
        Ok((metadata, rest))
    }
}

/// Database is a wrapper of storage engine, it provides
/// some  common operations for redis commands.
pub struct Roxy {
    storage: StorageRef,
    namespace: StringBytes,
}

impl Roxy {
    pub fn new(storage: StorageRef, namespace: StringBytes) -> Self {
        Self { storage, namespace }
    }
}

impl Database for Roxy {
    fn encode_namespace_prefix(&self, user_key: Bytes) -> Bytes {
        metadata::encode_namespace_key(self.namespace.clone(), user_key)
    }

    fn get_raw_metadata(&self, options: GetOptions, ns_key: Bytes) -> Result<Bytes> {
        todo!()
    }
}

#[derive(Default)]
pub struct GetOptions;

impl GetOptions {
    pub fn new() -> Self {
        Self
    }
}
