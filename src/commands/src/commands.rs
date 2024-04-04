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
use redis_protocol::resp3::types::BytesFrame;

mod string;

#[derive(Debug, strum::Display)]
#[strum(serialize_all = "snake_case")]
pub enum CommandId {
    SET,
}

pub struct CommandAttr {
    /// command id i.e. command name
    cmd_id: CommandId,
}

pub trait Command {
    type Error;
    type Args;

    fn get_attr(&self) -> &CommandAttr;

    /// Parse an array of Bytes, since client can only send RESP3 Array frames
    fn parse(&self, input: Vec<Bytes>) -> Result<Self::Args, Self::Error>;
    fn execute(&self) -> Result<BytesFrame, Self::Error>;
}

#[cfg(test)]
pub mod test_utility {
    use bytes::Bytes;
    use redis_protocol::codec;
    use redis_protocol::resp3::types::BytesFrame;

    pub fn resp3_encode_command(cmd: &str) -> Vec<Bytes> {
        let f = codec::resp3_encode_command(cmd);
        let data: Vec<_> = match f {
            BytesFrame::Array { data, .. } => data
                .iter()
                .map(|b| match b {
                    BytesFrame::SimpleString { data, .. } => data.clone(),
                    BytesFrame::BlobString { data, .. } => data.clone(),
                    _ => unreachable!(),
                })
                .collect(),
            _ => unreachable!(),
        };
        data
    }
}
