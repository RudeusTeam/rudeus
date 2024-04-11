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

use bytes::Bytes;
use common_base::bytes::StringBytes;
use redis_protocol::resp3::types::{BytesFrame, FrameKind};
use roxy::datatypes::string::{RedisString, StringSetArgs, StringSetArgsBuilder, StringSetType};
use roxy::storage::Storage;
use snafu::ResultExt;

use super::{Command, CommandId, CommandInstance};
use crate::error::{InvalidCmdSyntaxSnafu, Result};
use crate::parser::{
    chain, key, keyword, optional, string, ttl, value_of_type, Parser, TTLOption, Tokens,
};

pub struct SetArgs {
    key: Bytes,
    value: StringBytes,
    set_args: StringSetArgs,
}

/// [`SET`] is an instance of `SET` command
pub struct Set {
    cmd: Command,
    args: Option<SetArgs>,
}

impl Set {
    pub fn new() -> Self {
        Self {
            cmd: Command { id: CommandId::SET },
            args: None,
        }
    }
}

impl CommandInstance for Set {
    fn get_attr(&self) -> &Command {
        &self.cmd
    }

    fn parse(&mut self, input: Vec<Bytes>) -> Result<()> {
        let mut set_args_builder = StringSetArgsBuilder::default();
        let mut tokens = Tokens::new(&input[..]);
        // skip the command name which is already ensured by strum
        tokens.advance();

        let (key, value) = chain(key(), string())
            .parse(&mut tokens)
            .context(InvalidCmdSyntaxSnafu { cmd_id: self.id() })?;

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

        let ttl = optional(ttl()).parse(&mut tokens).unwrap();
        if let Some(ttl) = ttl {
            match ttl {
                TTLOption::TTL(ttl) => set_args_builder.ttl(ttl),
                TTLOption::KeepTTL => set_args_builder.keep_ttl(true),
            };
        }

        let set_args = set_args_builder.build().unwrap();
        tokens
            .expect_eot()
            .context(InvalidCmdSyntaxSnafu { cmd_id: self.id() })?;
        self.args = Some(SetArgs {
            key: key.into(),
            value,
            set_args,
        });
        Ok(())
    }

    fn execute(mut self, storage: &Storage, namespace: Bytes) -> BytesFrame {
        let db = RedisString::new(storage, namespace.into());
        let args = self.args.take().unwrap();
        // TODO: handle ttl

        let (Ok(res) | Err(res)) = db
            .set(args.key, args.value, &args.set_args)
            .map(|opt_old| match opt_old {
                Some(old) if args.set_args.get => (FrameKind::BlobString, old).try_into().unwrap(),
                Some(_) => (FrameKind::SimpleString, "OK").try_into().unwrap(),
                None => BytesFrame::Null,
            })
            .map_err(|err| {
                (FrameKind::SimpleError, err.to_string())
                    .try_into()
                    .unwrap()
            });
        res
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use super::*;
    use crate::commands::test_utility::resp3_encode_command;

    #[test]
    fn test_valid_set_cmd_parse() {
        let input = resp3_encode_command("SeT key value nX");
        let mut set_cmd = Set::new();
        assert!(set_cmd.parse(input).is_ok());
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
            set_cmd.parse(input.clone()).unwrap_err().source().unwrap()
        );
        assert!(set_cmd.parse(input).is_err());
    }
}
