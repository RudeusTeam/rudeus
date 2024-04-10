use common_telemetry::log::info;
use roxy::storage::Storage;
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;

use crate::connection::Connection;

#[derive(Debug, Serialize, Deserialize)]
pub struct ServerConfig {
    bind: String,
}

pub struct Server {
    storage: Storage,
    bind: String,
}

impl Server {
    pub fn new(storage: Storage, config: ServerConfig) -> Self {
        Self {
            storage,
            bind: config.bind,
        }
    }

    pub fn storage(&self) -> &Storage {
        &self.storage
    }

    pub async fn start(&self) {
        info!("Rudeus listening on: {}", self.bind);
        let listener = TcpListener::bind(&self.bind).await.unwrap();
        loop {
            let stream = listener.accept().await.map(|(socket, _)| socket).unwrap();
            tokio::spawn(async move {
                let mut conn = Connection::new(stream);
                conn.start().await
            });
        }
    }
}
