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

use std::sync::Once;

use bytes::Bytes;
use common_base::bytes::StringBytes;
use common_base::resp3;
use common_base::timestamp::timestamp_ms;
use once_cell::sync::Lazy;
use redis_protocol::resp3::types::{BytesFrame, FrameKind};
use roxy::database::Database;
use roxy::datatypes::string::{RedisString, StringSetArgs, StringSetArgsBuilder, StringSetType};
use roxy::storage::Storage;
use snafu::ResultExt;

use super::{Command, CommandInstance, CommandTypeInfo, GlobalCommandTable};
use crate::error::{FailInStorageSnafu, InvalidCmdSyntaxSnafu, Result};
use crate::parser::{
    chain, expire, key, keyword, optional, string, value_of_type, ExpireOption, Parser, Tokens,
};
use crate::{command_type_stub, register};

pub struct SetArgs {
    key: Bytes,
    value: StringBytes,
    set_args: StringSetArgs,
}

/// [`SET`] is an instance of `SET` command
pub struct Set {
    args: Option<SetArgs>,
}

impl CommandInstance for Set {
    fn parse(&mut self, input: &[Bytes]) -> Result<()> {
        let mut set_args_builder = StringSetArgsBuilder::default();
        let mut tokens = Tokens::new(input);

        let (key, value) = chain(key(), string())
            .parse(&mut tokens)
            .context(InvalidCmdSyntaxSnafu { cmd_id: Self::id() })?;

        // TODO: Add permutation parser
        let set_type = optional(value_of_type::<StringSetType>())
            .parse(&mut tokens)
            .unwrap();
        set_args_builder.set_type(set_type.unwrap_or(StringSetType::NONE));

        let get = optional(keyword("GET"))
            .parse(&mut tokens)
            .unwrap()
            .is_some();
        set_args_builder.get(get);

        let expire = optional(expire()).parse(&mut tokens).unwrap();
        if let Some(expire) = expire {
            match expire {
                ExpireOption::Expire(expire) => set_args_builder.expire(Some(expire)),
                ExpireOption::KeepTTL => set_args_builder.keep_ttl(true),
            };
        }

        let set_args = set_args_builder.build().unwrap();
        tokens
            .expect_eot()
            .context(InvalidCmdSyntaxSnafu { cmd_id: Self::id() })?;
        self.args = Some(SetArgs {
            key: key.into(),
            value,
            set_args,
        });
        Ok(())
    }

    fn execute(&mut self, storage: &Storage, namespace: Bytes) -> Result<BytesFrame> {
        let db = RedisString::new(storage, namespace.into());
        let args = self.args.take().unwrap();
        if args
            .set_args
            .expire
            .is_some_and(|expire| expire < timestamp_ms())
        {
            db.db()
                .delete(args.key.clone().into())
                .context(FailInStorageSnafu { cmd_id: Self::id() })?;
            return Ok(resp3::ok());
        }

        db.set(args.key, args.value, &args.set_args)
            .map(|opt_old| match opt_old {
                Some(old) => (FrameKind::BlobString, old).try_into().unwrap(),
                None if args.set_args.get => resp3::ok(),
                None => BytesFrame::Null,
            })
            .context(FailInStorageSnafu { cmd_id: Self::id() })
    }
}

impl CommandTypeInfo for Set {
    fn new() -> Self {
        Self { args: None }
    }

    command_type_stub! { id: "Set" }
}

pub struct Get {
    key: Bytes,
}

impl CommandInstance for Get {
    fn parse(&mut self, input: &[Bytes]) -> Result<()> {
        let key = key()
            .parse(&mut Tokens::new(input))
            .context(InvalidCmdSyntaxSnafu { cmd_id: Self::id() })?;
        self.key = key.into();
        Ok(())
    }

    fn execute(&mut self, storage: &Storage, namespace: Bytes) -> Result<BytesFrame> {
        RedisString::new(storage, namespace.into())
            .get(self.key.clone())
            .map(|opt_value| {
                opt_value
                    .map(|value| (FrameKind::BlobString, value).try_into().unwrap())
                    .unwrap_or_else(|| BytesFrame::Null)
            })
            .context(FailInStorageSnafu { cmd_id: Self::id() })
    }
}

impl CommandTypeInfo for Get {
    fn new() -> Self {
        Self { key: Bytes::new() }
    }

    command_type_stub! { id: "Get" }
}

register! {Set, Get}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use super::*;
    use crate::commands::test_utility::resp3_encode_command;

    #[test]
    fn test_valid_set_cmd_parse() {
        let input = resp3_encode_command("SeT key value nX");
        let mut set_cmd = Set::new();
        assert!(set_cmd.parse(&input[1..]).is_ok());
        let args = set_cmd.args.as_ref().unwrap();
        assert_eq!(args.key, &b"key"[..]);
        assert_eq!(args.value.as_utf8(), "value");
        assert_eq!(args.set_args.set_type(), StringSetType::NX);
    }

    #[test]
    fn test_invalid_set_cmd_parse() {
        let input = resp3_encode_command("SeT key value invalid");
        let mut set_cmd = Set::new();
        println!(
            "{}",
            set_cmd.parse(&input[1..]).unwrap_err().source().unwrap()
        );
        assert!(set_cmd.parse(&input[..]).is_err());
    }
}
