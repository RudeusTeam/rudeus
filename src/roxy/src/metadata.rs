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

use std::sync::atomic::{AtomicU64, Ordering};

use binrw::{binrw, BinRead, BinWrite};
use bytes::{BufMut, BytesMut};
use common_base::bytes::{Bytes, StringBytes};
use common_base::timestamp::timestamp_ms;
use snafu::ResultExt;
use strum::{Display, VariantArray};

use crate::error::{EncodeMetadataSnafu, InvalidMetadataInputSnafu, Result};

static VERSION_COUNTER: AtomicU64 = AtomicU64::new(0);
const VERSION_COUNTER_BITS: usize = 11;
const VERSION_COUNTER_MASK: u64 = (1 << VERSION_COUNTER_BITS) - 1;

const REDIS_TYPE_MASK: u8 = 0x0F;
#[binrw]
#[brw(repr(u8))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, VariantArray, Display)]
#[strum(serialize_all = "lowercase")]
pub enum RedisType {
    None = 0,
    String = 1,
    Hash = 2,
    List = 3,
    Set = 4,
    ZSet = 5,
    Bitmap = 6,
    Sortedint = 7,
    Stream = 8,
    BloomFilter = 9,
    Json = 10,
    Search = 11,
}

impl RedisType {
    pub fn is_single_kv(&self) -> bool {
        *self == RedisType::String || *self == RedisType::Json
    }
    pub fn is_emptyable(&self) -> bool {
        self.is_single_kv() || *self == RedisType::Stream || *self == RedisType::BloomFilter
    }
}

impl From<RedisType> for u8 {
    fn from(val: RedisType) -> Self {
        val as u8
    }
}

impl From<&u8> for RedisType {
    fn from(v: &u8) -> Self {
        match v & REDIS_TYPE_MASK {
            0 => RedisType::None,
            1 => RedisType::String,
            2 => RedisType::Hash,
            3 => RedisType::List,
            4 => RedisType::Set,
            5 => RedisType::ZSet,
            6 => RedisType::Bitmap,
            7 => RedisType::Sortedint,
            8 => RedisType::Stream,
            9 => RedisType::BloomFilter,
            10 => RedisType::Json,
            11 => RedisType::Search,
            _ => panic!("invalid RedisTypeId"),
        }
    }
}

impl From<u8> for RedisType {
    fn from(v: u8) -> Self {
        Self::from(&v)
    }
}

/// namespace key format:
/// +------------------+-----------+----------+
/// | namespace length | namespace | user key |
/// | 1byte: n         | nbytes    | m bytes  |
/// +------------------+-----------+----------+

pub struct NamespaceKey {
    pub namespace_len: u8,
    pub namespace: StringBytes,
    pub user_key: Bytes,
}

/// flag and expire is a common field at the beginning of a value's metadata.

/// +------------------------------+--------+
/// |            flags             | expire |
/// |    unused   |    datatype    | 8bytes |
/// +------------------------------+--------+

#[binrw]
#[brw(little)]
pub struct VersionAndSize {
    version: u64,
    size: u64,
}

#[binrw]
#[brw(little)]
pub struct Metadata {
    flag: u8,
    expire: u64,
    #[brw(if(!RedisType::from(flag).is_single_kv()))]
    version: u64,
    #[brw(if(!RedisType::from(flag).is_single_kv()))]
    size: u64,
}

impl Metadata {
    pub fn new(datatype: RedisType, generate_version: bool) -> Self {
        let version = if generate_version {
            Self::generate_version()
        } else {
            0
        };
        let size = 0;
        Self {
            flag: datatype.into(),
            expire: 0,
            version,
            size,
        }
    }

    pub fn offset_after_expire() -> usize {
        std::mem::size_of::<u8>() + std::mem::size_of::<u64>()
    }

    pub fn decode_from<R>(reader: &mut R) -> Result<Self>
    where
        R: std::io::Read + std::io::Seek,
    {
        Metadata::read(reader).context(InvalidMetadataInputSnafu)
    }

    pub fn encode_into<W>(&self, writer: &mut W) -> Result<()>
    where
        W: std::io::Write + std::io::Seek,
    {
        self.write(writer).context(EncodeMetadataSnafu)
    }

    pub fn datatype(&self) -> RedisType {
        self.flag.into()
    }

    pub fn version(&self) -> u64 {
        self.version
    }

    pub fn size(&self) -> u64 {
        self.size
    }

    pub fn expire(&self) -> u64 {
        self.expire
    }

    pub fn expired_at(&self, expire_ts: u64) -> bool {
        if !self.datatype().is_emptyable() && self.size == 0 {
            return true;
        }
        self.expire != 0 && self.expire < expire_ts
    }

    pub fn expired(&self) -> bool {
        self.expired_at(timestamp_ms())
    }

    fn generate_version() -> u64 {
        // TODO: figure out how to get timestamp safely
        let ts = timestamp_ms();
        let counter = VERSION_COUNTER.fetch_add(1, Ordering::SeqCst);
        ts << VERSION_COUNTER_BITS | (counter & VERSION_COUNTER_MASK)
    }
}

pub fn encode_namespace_key(namespace: StringBytes, user_key: Bytes) -> Bytes {
    let mut buf = BytesMut::with_capacity(1 + namespace.len() + user_key.len());
    buf.put_u8(namespace.len() as u8);
    buf.put_slice(namespace.as_utf8().as_bytes());
    buf.put_slice(user_key.as_ref());
    buf.freeze().into()
}

#[cfg(test)]
mod tests {

    use std::io::Cursor;

    use binrw::io::NoSeek;
    use bytes::Buf;

    use super::*;

    #[test]
    fn test_encode_namespace_key() {
        let namespace: StringBytes = "namespace".into();
        let user_key: Bytes = b"user_key"[..].into();
        let encoded_key = encode_namespace_key(namespace.clone(), user_key.clone());
        assert_eq!(encoded_key.len(), 1 + namespace.len() + user_key.len());
        assert_eq!(encoded_key, b"\x09namespaceuser_key"[..])
    }

    #[test]
    fn test_decode_metadata() {
        let original_metadata = Metadata::new(RedisType::String, true);
        let mut writer = Cursor::new(Vec::new());
        assert!(original_metadata.write(&mut writer).is_ok());
        let data = writer.into_inner();

        let mut reader = NoSeek::new(data.reader());
        let decoded_metadata = Metadata::decode_from(&mut reader).unwrap();
        assert_eq!(decoded_metadata.flag, original_metadata.flag);
        assert_eq!(decoded_metadata.expire, original_metadata.expire);
        assert_eq!(decoded_metadata.version(), 0); // skipped since it's a string type
        assert_eq!(decoded_metadata.size(), 0);
    }

    #[test]
    fn test_encode_metadata() {
        let metadata = Metadata::new(RedisType::String, false);
        let mut writer = NoSeek::new(BytesMut::new().writer());
        let _ = metadata.write(&mut writer);
        assert_eq!(
            writer.into_inner().into_inner().freeze(),
            &[0x01, 0, 0, 0, 0, 0, 0, 0, 0][..]
        );

        let metadata = Metadata::new(RedisType::List, false);
        let mut writer = NoSeek::new(BytesMut::new().writer());
        let _ = metadata.write(&mut writer);
        assert_eq!(
            writer.into_inner().into_inner().freeze(),
            &[
                0x03, //flag
                0, 0, 0, 0, 0, 0, 0, 0, // expire
                0, 0, 0, 0, 0, 0, 0, 0, // version
                0, 0, 0, 0, 0, 0, 0, 0, // size
            ][..]
        )
    }
}
