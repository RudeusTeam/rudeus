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
use redis_protocol::resp3::types::BytesFrame;
use roxy::datatypes::string::{StringSetArgs, StringSetArgsBuilder, StringSetType};
use snafu::{ResultExt, Snafu};

use super::{Command, CommandAttr, CommandId};
use crate::error::{ParseError, UnexpectedTokenSnafu};
use crate::parser::{chain, key, keyword, optional, right, string, value_of_type, Parser, Tokens};

pub struct Set {
    attr: CommandAttr,
}

#[derive(Debug, Snafu)]
pub enum StringCommandError {
    #[snafu(display("invalid 'SET' command"))]
    InvalidSyntax { source: ParseError },
}

#[derive(Debug)]
pub struct SetArgs {
    key: Bytes,
    value: StringBytes,
    args: StringSetArgs,
}

impl Set {
    pub fn new() -> Self {
        Self {
            attr: CommandAttr {
                cmd_id: CommandId::SET,
            },
        }
    }
}

impl Command for Set {
    type Error = StringCommandError;
    type Args = SetArgs;

    fn get_attr(&self) -> &CommandAttr {
        &self.attr
    }

    fn parse(&self, _input: Vec<Bytes>) -> Result<Self::Args, Self::Error> {
        let mut tokens = Tokens::new(&_input[..]);
        let (key, value) = right(keyword("SET"), chain(key(), string()))
            .parse(&mut tokens)
            .context(InvalidSyntaxSnafu)?;
        // TODO: Add permutation parser
        let set_type = optional(value_of_type::<StringSetType>())
            .parse(&mut tokens)
            .context(InvalidSyntaxSnafu)?;
        let string_set_args = StringSetArgsBuilder::default()
            .set_type(set_type.unwrap_or(StringSetType::NONE))
            .build()
            .unwrap();

        if tokens.is_eot() {
            Ok(SetArgs {
                key: key.into(),
                value,
                args: string_set_args,
            })
        } else {
            UnexpectedTokenSnafu {
                token: String::from_utf8_lossy(&tokens.current().unwrap()[..]),
            }
            .fail()
            .context(InvalidSyntaxSnafu)
        }
    }

    fn execute(&self) -> Result<BytesFrame, Self::Error> {
        unimplemented!()
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
        let set_cmd = Set::new();
        let args = set_cmd.parse(input).unwrap();
        assert_eq!(args.key, &b"key"[..]);
        assert_eq!(args.value.as_utf8(), "value");
        assert_eq!(args.args.set_type(), StringSetType::NX);
    }

    #[test]
    fn test_invalid_set_cmd_parse() {
        let input = resp3_encode_command("SeT key value invalid");
        let set_cmd = Set::new();
        println!(
            "{}",
            set_cmd.parse(input.clone()).unwrap_err().source().unwrap()
        );
        assert!(set_cmd.parse(input).is_err());
    }
}
