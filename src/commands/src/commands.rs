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

// pub type GlobalCommandTable = HashMap<CommandId, Command>;
#[derive(Default)]
pub struct GlobalCommandTable {
    table: HashMap<CommandIdRef<'static>, Command>,
}

impl GlobalCommandTable {
    pub fn register<T: CommandTypeInfo>(&mut self) {
        let cmd = Command::new::<T>();
        self.table.insert(cmd.id, cmd);
    }

    pub fn get(&self, cmd_id: &CommandId) -> Option<&Command> {
        self.table.get(cmd_id.to_ascii_uppercase().as_str())
    }

    pub fn new() -> GlobalCommandTable {
        GlobalCommandTable {
            table: HashMap::new(),
        }
    }
}

macro_rules! register_mod {
    ($($mod:ident),*) => {
        Lazy::new(|| {
            let mut table = Default::default();
                $(
                    $mod::register(&mut table);
                );*
            table
        })
    };
}

#[macro_export]
macro_rules! register {
    ($($cmd:ident),*) => {
        pub(in $crate::commands) fn register(table: &mut GlobalCommandTable) {
            static START: Once = Once::new();
            START.call_once(move || {
                $(
                    table.register::<$cmd>();
                )*
            });
        }
    };
}

/// create a command type stub for a command
///
/// [`cmd_id`] will be converted to uppercase
#[macro_export]
macro_rules! command_type_stub {
    (id: $cmd_id:literal) => {
        paste::paste! {
            fn command() -> &'static Command {
                static STUB: Lazy<Command> =
            Lazy::new(|| Command::new_stub(
                            stringify!([< $cmd_id:upper >])
                        ));
                &STUB
            }
        }
    };
}

pub static GLOBAL_COMMANDS_TABLE: Lazy<GlobalCommandTable> = register_mod! {string};

/// Mapping relationship between command id and command instance
/// [`CommandId`] => [`Command`] --create--> [`CommandInstance`]
pub type CommandId = str;
pub type CommandIdRef<'a> = &'a CommandId;

type CreateInstanceFn = fn() -> Box<dyn CommandInstance>;

#[derive(Clone)]
pub struct Command {
    /// command id i.e. command name
    id: CommandIdRef<'static>,
    create_instance_fn: CreateInstanceFn,
}
impl Command {
    pub fn create_instance(&self) -> Box<dyn CommandInstance> {
        (self.create_instance_fn)()
    }
}

impl Command {
    pub(crate) fn new<F: CommandTypeInfo>() -> Self {
        let mut cmd = F::command().clone();
        cmd.create_instance_fn = F::boxed;
        cmd
    }

    pub(crate) fn new_stub(cmd_id: CommandIdRef<'static>) -> Self {
        /// [`dummy_create_inst`] is a dummy function to satisfy the type of `CommandInstance`
        /// and prevent cyclic dependency between [`create_inst`] and [`new`] of CommandInst
        fn dummy_create_inst() -> Box<dyn CommandInstance> {
            unreachable!()
        }
        Self {
            id: cmd_id,
            create_instance_fn: dummy_create_inst,
        }
    }
}

pub trait CommandInstance: Send {
    /// Parse command arguments representing in an array of Bytes, since client can only send RESP3 Array frames
    fn parse(&mut self, input: &[Bytes]) -> Result<()>;
    fn execute(&mut self, storage: &Storage, namespace: Bytes) -> Result<BytesFrame>;
}

pub trait CommandTypeInfo: CommandInstance + Sized + 'static {
    /// Tell the system how to create instance
    fn new() -> Self;

    /// Static typing infomation of command, which is used to register the command
    fn command() -> &'static Command;

    /// Boxed version of `new`
    fn boxed() -> Box<dyn CommandInstance> {
        Box::new(Self::new())
    }

    fn id() -> CommandIdRef<'static> {
        Self::command().id
    }
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
