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
use std::sync::Arc;

use common_runtime::global_runtime::{block_on_network, init_global_runtimes};
use common_telemetry::log::{self, LoggingOption};
use roxy::storage::{Storage, StorageConfig};
use serde::{Deserialize, Serialize};
use server::rudeus_lock::try_lock_rudeus;
use server::server::{Server, ServerConfig};

#[derive(Debug, Deserialize, Serialize)]
pub struct RudeusConfig {
    logging: LoggingOption,
    server: ServerConfig,
    storage: StorageConfig,
}

fn main() -> Result<(), Box<dyn Error>> {
    init_global_runtimes(None, None, None);
    let config_path = "example/example.toml";
    let config_str = std::fs::read_to_string(config_path)?;

    let config: RudeusConfig = toml::from_str(&config_str)?;
    let _log_workers = log::init(&config.logging);
    let _rudeus_lock = try_lock_rudeus(config.storage.dbpath())?;

    let mut storage = Storage::try_new(config.storage)?;
    storage.open(roxy::storage::OpenMode::Default)?;
    let server = Server::new(Arc::new(storage), config.server);

    block_on_network(server.start())?;
    Ok(())
}
