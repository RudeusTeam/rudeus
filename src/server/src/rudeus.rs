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

use common_runtime::runtime::Runtime;
use common_telemetry::log::{self, info, LoggingOptionBuilder};
use server::connection::Connection;
use tokio::net::TcpListener;

async fn start() {
    let logging_option = LoggingOptionBuilder::default()
        .append_stdout(true)
        .level(Some("debug".to_owned()))
        .build()
        .unwrap();
    let _log_workers = log::init(&logging_option);

    info!("Rudeus listening on: {}", "127.0.0.1:6379");
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    loop {
        let stream = listener.accept().await.map(|(socket, _)| socket).unwrap();
        tokio::spawn(async move {
            let mut conn = Connection::new(stream);
            conn.start().await
        });
    }
}

fn main() {
    let rt = Runtime::builder()
        .worker_threads(4)
        .runtime_name("Network")
        .thread_name("network")
        .build()
        .expect("Failed to build runtime");
    rt.block_on(start());
}
