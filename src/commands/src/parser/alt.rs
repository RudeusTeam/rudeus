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

use paste::paste;

use super::{ParseResult, Parser, Tokens};

pub trait Alt<'a> {
    type Output;
    fn choice(&self, tokens: &mut Tokens<'a>) -> ParseResult<Self::Output>;
}

pub fn alt<'a, List: Alt<'a>>(l: List) -> impl Parser<'a, Output = List::Output> {
    move |tks: &mut Tokens<'a>| l.choice(tks)
}

macro_rules! impl_alt {
    ($p1:ident $(, $p:ident)*) => {
        paste!{
            #[allow(unused_parens)]
            impl<'a, [< $p1:upper >] $( ,[<$p: upper>] )* > Alt<'a> for (
    [< $p1:upper >] $( ,[< $p: upper >] )*
            )
            where
                [< $p1:upper> ] : Parser<'a>,
                $(
                    [<$p: upper>] : Parser<'a, Output=[<$p1:upper>]::Output>,
                )*
            {
                type Output = [< $p1:upper >]::Output;

                fn choice(&self, tokens: &mut Tokens<'a>) -> ParseResult<Self::Output> {
                    let ([< $p1:lower >] $(, [< $p:lower >])*) = self;
                    [< $p1:lower >].parse(tokens)
                    $(
                    .or_else(|_| [< $p:lower >].parse(tokens))
                    )*
                }
            }
        }
    };
}

impl_alt!(P1);
impl_alt!(P1, P2);
impl_alt!(P1, P2, P3);
impl_alt!(P1, P2, P3, P4);
impl_alt!(P1, P2, P3, P4, P5);
impl_alt!(P1, P2, P3, P4, P5, P6);
impl_alt!(P1, P2, P3, P4, P5, P6, P7);
impl_alt!(P1, P2, P3, P4, P5, P6, P7, P8);

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;
    use crate::parser::keyword;

    #[test]
    fn test_valid_alt() {
        let foo: Vec<Bytes> = vec!["foo".into()];
        let bar: Vec<Bytes> = vec!["bar".into()];
        let baz: Vec<Bytes> = vec!["baz".into()];
        let invalid: Vec<Bytes> = vec!["invalid".into()];

        let mut foo_tokens = Tokens::new(&foo);
        let mut bar_tokens = Tokens::new(&bar);
        let mut baz_tokens = Tokens::new(&baz);
        let mut invalid_tokens = Tokens::new(&invalid);

        let p = alt((keyword("foo"), keyword("bar"), keyword("baz")));

        assert!(p.parse(&mut foo_tokens).is_ok());
        assert!(p.parse(&mut bar_tokens).is_ok());
        assert!(p.parse(&mut baz_tokens).is_ok());
        assert!(p.parse(&mut invalid_tokens).is_err());
    }
}
