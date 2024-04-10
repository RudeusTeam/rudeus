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

use std::error::Error;

use common_runtime::runtime::Runtime;
use common_telemetry::log::{self, LoggingOption};
use roxy::storage::{Storage, StorageConfig};
use serde::{Deserialize, Serialize};
use server::server::{Server, ServerConfig};

#[derive(Debug, Deserialize, Serialize)]
pub struct RudeusConfig {
    logging: LoggingOption,
    server: ServerConfig,
    storage: StorageConfig,
}

fn main() -> Result<(), Box<dyn Error>> {
    let config: RudeusConfig = toml::from_str(
        r#"
        [logging]
        stdout = true
        level = "debug"
        [server]
        bind = "127.0.0.1:6666"
        [storage]
        path = "/tmp/roxy"
        secondary_path = "/tmp/roxy2"
        [storage.rocksdb]
        block_size = 4096
    "#,
    )?;
    let _log_workers = log::init(&config.logging);

    let storage = Storage::try_new(config.storage)?;
    let server = Server::new(storage, config.server);

    let rt = Runtime::builder()
        .worker_threads(4)
        .runtime_name("Network")
        .thread_name("network")
        .build()
        .expect("Failed to build runtime");
    rt.block_on(server.start());
    Ok(())
}
