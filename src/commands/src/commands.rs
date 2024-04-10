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

use std::collections::HashMap;

use bytes::Bytes;
use once_cell::sync::Lazy;
use redis_protocol::resp3::types::BytesFrame;
use roxy::storage::Storage;

use crate::error::Result;

mod string;

pub static COMMANDS_TABLE: Lazy<HashMap<CommandId, Command>> = Lazy::new(|| {
    let mut table = HashMap::new();
    table.insert(CommandId::SET, Command::new(CommandId::SET));
    table
});

/// ```                                                                     
/// COMMAND: string => Command ID => Command ──create──► CommandInstance
/// ```
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, strum::Display, strum::EnumString)]
#[strum(serialize_all = "lowercase")]
#[strum(ascii_case_insensitive)]
pub enum CommandId {
    SET,
}

pub struct Command {
    /// command id i.e. command name
    id: CommandId,
}

impl Command {
    pub fn new(cmd_id: CommandId) -> Self {
        Self { id: cmd_id }
    }

    pub fn new_instance(&self) -> impl CommandInstance {
        match self.id {
            CommandId::SET => string::Set::new(),
        }
    }
}

pub trait CommandInstance {
    fn get_attr(&self) -> &Command;

    fn id(&self) -> CommandId {
        self.get_attr().id
    }

    /// Parse an array of Bytes, since client can only send RESP3 Array frames
    fn parse(&mut self, input: Vec<Bytes>) -> Result<()>;
    fn execute(self, storage: &Storage, namespace: Bytes) -> BytesFrame;
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
