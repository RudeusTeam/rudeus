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

use snafu::Snafu;

use crate::commands::CommandIdRef;
use crate::parser::ParseError;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum Error {
    #[snafu(display("Invalid '{}' command", cmd_id))]
    InvalidCmdSyntax {
        source: ParseError,
        cmd_id: CommandIdRef<'static>,
    },
    #[snafu(display("Fail to execute command '{}' because of storage error", cmd_id))]
    FailInStorage {
        source: roxy::error::Error,
        cmd_id: CommandIdRef<'static>,
    },
}

pub(crate) type Result<T> = std::result::Result<T, Error>;
