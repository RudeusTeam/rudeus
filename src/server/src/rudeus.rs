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
use common_runtime::runtime::Runtime;
use common_telemetry::log::{self, info, LoggingOptionBuilder};
use futures::{SinkExt, StreamExt as _};
use redis_protocol::codec::Resp3;
use redis_protocol::resp3::types::BytesFrame;
use redis_protocol::tokio_util::codec::Framed;
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::Decoder;

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
        let framed = listener
            .accept()
            .await
            .map(|(socket, _)| Resp3::default().framed(socket))
            .unwrap();
        tokio::spawn(async { process(framed).await });
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

async fn process(framed: Framed<TcpStream, Resp3>) {
    let (mut writer, mut reader) = framed.split();
    while let Some(frame) = reader.next().await {
        let frame = frame.unwrap();
        info!("Received: {:?}", frame);
        let response = BytesFrame::SimpleString {
            data: Bytes::from_static(b"OK"),
            attributes: None,
        };
        writer.send(response).await.unwrap();
    }

    // Rest of the code...
}
