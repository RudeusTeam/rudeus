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

use bytes::{Bytes, BytesMut};
use common_telemetry::log::{self, debug, info, LoggingOptionBuilder};
use redis_protocol::resp2::decode::decode;
use redis_protocol::resp2::encode::encode_bytes;
use redis_protocol::resp2::types::Frame;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

#[tokio::main]
async fn main() {
    let logging_option = LoggingOptionBuilder::default()
        .append_stdout(true)
        .build()
        .unwrap();
    let _log_workers = log::init(&logging_option);

    info!("Rudeus listening on: {}", "127.0.0.1:6379");
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    loop {
        let (socket, _) = listener.accept().await.unwrap();
        tokio::spawn(async { process(socket).await });
    }
}

async fn process(mut socket: tokio::net::TcpStream) {
    let mut buf = BytesMut::with_capacity(1024);
    while (socket.read_buf(&mut buf).await).is_ok() {
        let b = buf.freeze();
        let (frame, _consumed) = match decode(&b) {
            Ok(Some((f, c))) => (f, c),
            Ok(None) => panic!("Incomplete frame"),
            Err(_) => todo!(),
        };
        debug!("Frame: {:?}", frame);
        buf = BytesMut::with_capacity(1024);
        let mut response = BytesMut::with_capacity(1024);
        let _ = encode_bytes(&mut response, &Frame::SimpleString(Bytes::from(&b"OK"[..])));
        let _ = socket.write(&response.freeze()[..]).await;
    }
    debug!("Received: {:?}", buf);
    debug!("Processing socket");
}
