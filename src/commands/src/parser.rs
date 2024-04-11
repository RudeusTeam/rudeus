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

//! A internal mutable parser combinator library for parsing redis command.

mod alt;

use bytes::Bytes;

pub type Token = Bytes;

#[derive(Debug, Snafu)]
pub enum ParseError {
    InvalidSyntax {
        command_name: String,
    },
    #[snafu(display("Expect keyword: {}, actual: {}", expected, actual))]
    MismatchedKeyword {
        expected: String,
        actual: String,
    },
    #[snafu(display("Expect value of type: {}, got value: {}", typ, token))]
    InvalidValueOfType {
        typ: String,
        token: String,
    },
    #[snafu(display("Expect a key"))]
    MissKey,
    #[snafu(display("{}", message))]
    InvalidArgument {
        message: String,
    },

    #[snafu(display("Unexpected token: {}", token))]
    UnexpectedToken {
        token: String,
    },
    #[snafu(display("TTL option '{}' need a following valid time", ttl_type))]
    TTLMissTime {
        #[snafu(source(from(ParseError, Box::new)))]
        source: Box<ParseError>,
        ttl_type: String,
    },
}
pub struct Tokens<'a> {
    tokens: &'a [Token],
    cursor: usize,
}

impl<'a> Tokens<'a> {
    pub fn new(tokens: &'a [Token]) -> Self {
        Self { tokens, cursor: 0 }
    }

    pub fn advance(&mut self) {
        self.cursor += 1;
    }

    /// check if the cursor is at the end of the tokens
    pub fn is_eot(&self) -> bool {
        self.cursor >= self.tokens.len()
    }

    /// if the [`cursor`] is not at the end of the [`tokens`], return an error
    pub fn expect_eot(&self) -> ParseResult<()> {
        if self.is_eot() {
            Ok(())
        } else {
            UnexpectedTokenSnafu {
                token: String::from_utf8_lossy(self.current().unwrap()),
            }
            .fail()
        }
    }

    pub fn current(&self) -> Option<&Token> {
        self.tokens.get(self.cursor)
    }

    /// consume the current token and return the value
    pub fn yield_value<T>(&mut self, val: T) -> ParseResult<T> {
        self.advance();
        Ok(val)
    }

    /// get the current token
    pub fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.cursor)
    }
}

pub type ParseResult<'a, Output> = Result<Output, ParseError>;

pub trait Parser<'a> {
    type Output;
    fn parse(&self, s: &mut Tokens<'a>) -> ParseResult<'a, Self::Output>;
}

impl<'a, F, T> Parser<'a> for F
where
    F: Fn(&mut Tokens<'a>) -> ParseResult<'a, T>,
{
    type Output = T;
    fn parse(&self, s: &mut Tokens<'a>) -> ParseResult<'a, Self::Output> {
        self(s)
    }
}

mod operand {
    use std::rc::Rc;
    use std::str::FromStr;

    use common_base::bytes::{Bytes, StringBytes};
    use common_base::timestamp::timestamp_ms;
    use snafu::{OptionExt as _, ResultExt};

    use super::{
        alt, optional, InvalidValueOfTypeSnafu, MismatchedKeywordSnafu, MissKeySnafu, Parser,
        TTLMissTimeSnafu, Tokens,
    };

    /// expect a keyword(ignore case)
    pub fn keyword<'a>(ident: &str) -> impl Parser<'a, Output = StringBytes> {
        let ident: Rc<StringBytes> = Rc::new(ident.into());
        move |s: &mut Tokens<'a>| match s.peek() {
            Some(tk) => {
                if tk.eq_ignore_ascii_case(ident.as_utf8().as_bytes()) {
                    s.yield_value((*ident).clone())
                } else {
                    MismatchedKeywordSnafu {
                        expected: ident.to_string(),
                        actual: String::from_utf8_lossy(tk.as_ref()),
                    }
                    .fail()
                }
            }
            None => MismatchedKeywordSnafu {
                expected: ident.to_string(),
                actual: "",
            }
            .fail(),
        }
    }

    pub fn value_of_type<'a, T: FromStr>() -> impl Parser<'a, Output = T> {
        move |s: &mut Tokens<'a>| match s.peek() {
            Some(first) => {
                let first = StringBytes::new(first.clone());
                let value = first
                    .as_utf8()
                    .parse()
                    .ok()
                    .context(InvalidValueOfTypeSnafu {
                        typ: std::any::type_name::<T>(),
                        token: first.to_string(),
                    })?;
                s.yield_value(value)
            }
            None => InvalidValueOfTypeSnafu {
                typ: std::any::type_name::<T>(),
                token: "",
            }
            .fail(),
        }
    }

    pub fn key<'a>() -> impl Parser<'a, Output = Bytes> {
        move |s: &mut Tokens<'a>| match s.peek() {
            Some(first) => s.yield_value(first.clone().into()),
            None => MissKeySnafu.fail(),
        }
    }

    pub fn string<'a>() -> impl Parser<'a, Output = StringBytes> {
        move |s: &mut Tokens<'a>| match s.peek() {
            Some(first) => {
                let first = StringBytes::new(first.clone());
                let str = first.as_utf8();
                // TODO(j0hn50n133): string syntax supported by iredis is actually very complex
                if str.starts_with('"') && str.ends_with('"') && str.len() >= 2 {
                    s.yield_value(str[1..str.len() - 1].into())
                } else {
                    s.yield_value(str.into())
                }
            }
            None => MissKeySnafu.fail(),
        }
    }

    pub enum TTLOption {
        TTL(u64),
        KeepTTL,
    }
    pub fn ttl<'a>() -> impl Parser<'a, Output = TTLOption> {
        move |s: &mut Tokens<'a>| {
            let ttl_type = optional(alt((
                keyword("EX"),
                keyword("PX"),
                keyword("EXAT"),
                keyword("PXAT"),
                keyword("KEEPTTL"),
            )))
            .parse(s)
            .unwrap();
            if let Some(ttl_type) = ttl_type {
                let ttl = value_of_type::<u64>().parse(s).context(TTLMissTimeSnafu {
                    ttl_type: ttl_type.clone(),
                })?;
                Ok(match ttl_type.as_utf8() {
                    "EX" => TTLOption::TTL(ttl * 1000),
                    "PX" => TTLOption::TTL(ttl),
                    "PXAT" => TTLOption::TTL(ttl - timestamp_ms()),
                    "EXAT" => TTLOption::TTL(ttl * 1000 - timestamp_ms()),
                    _ => unreachable!(),
                })
            } else {
                MismatchedKeywordSnafu {
                    expected: "EX | PX | EXAT | PXAT | KEEPTTL",
                    actual: s
                        .peek()
                        .map(|s| StringBytes::new(s.clone()).into())
                        .unwrap_or("".to_string()),
                }
                .fail()
            }
        }
    }
}

mod combinator {
    use super::{Parser, Tokens};

    /// chain two parser and return the output of the second parser
    pub fn right<'a, P1, P2>(parser1: P1, parser2: P2) -> impl Parser<'a, Output = P2::Output>
    where
        P1: Parser<'a>,
        P2: Parser<'a>,
    {
        move |s: &mut Tokens<'a>| {
            let _ = parser1.parse(s)?;
            let second = parser2.parse(s)?;
            Ok(second)
        }
    }

    /// chain two parser and return a tuple of their output
    pub fn chain<'a, P1, P2>(
        parser1: P1,
        parser2: P2,
    ) -> impl Parser<'a, Output = (P1::Output, P2::Output)>
    where
        P1: Parser<'a>,
        P2: Parser<'a>,
    {
        move |s: &mut Tokens<'a>| {
            let first = parser1.parse(s)?;
            let second = parser2.parse(s)?;
            Ok((first, second))
        }
    }

    /// optional parser, return `None` if the parser failed, impossible to return `Err`
    pub fn optional<'a, P>(parser: P) -> impl Parser<'a, Output = Option<P::Output>>
    where
        P: Parser<'a>,
    {
        move |s: &mut Tokens<'a>| {
            if let Ok(output) = parser.parse(s) {
                Ok(Some(output))
            } else {
                Ok(None)
            }
        }
    }
}

pub use alt::*;
pub use combinator::*;
pub use operand::*;
use snafu::Snafu;

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_name() {
        let test_input: Vec<Bytes> = vec!["SET".into(), "KEY".into(), "\"VALUE\"".into()];
        struct SetArgs {
            key: common_base::bytes::Bytes,
            value: common_base::bytes::Bytes,
        }

        fn parse_set_args<'a>(input: &mut Tokens<'_>) -> ParseResult<'a, SetArgs> {
            (keyword("SET")).parse(input).unwrap();
            let (key, value) = chain(key(), string()).parse(input).unwrap();
            Ok(SetArgs {
                key,
                value: value.into(),
            })
        }
        let mut tokens = Tokens::new(&test_input);
        let set_args = parse_set_args(&mut tokens).unwrap();
        assert_eq!(tokens.cursor, 3);
        assert_eq!(&set_args.key[..], "KEY".as_bytes());
        assert_eq!(&set_args.value[..], "VALUE".as_bytes());
    }
}
