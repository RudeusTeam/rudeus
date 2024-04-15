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

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum Error {
    #[snafu(display("Unknown command '{}'", cmd))]
    UnknownCommand { cmd: String },
    #[snafu(transparent)]
    ExecuteError { source: commands::error::Error },
    #[snafu(display("Failed to bind to address '{}': {}", bind, source))]
    BindError {
        bind: String,
        source: std::io::Error,
    },
    #[snafu(display("Another instance is already Instaunning"))]
    InstanceAlreadyExists { source: std::io::Error },
    #[snafu(display("Failed to acquire process file lock: '{}'", path))]
    AcquireFileLock {
        source: std::io::Error,
        path: String,
    },
}

pub type Result<T> = std::result::Result<T, Error>;
