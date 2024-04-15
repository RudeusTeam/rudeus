use common_telemetry::log::info;
use roxy::storage::{Storage, StorageRef};
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use tokio::net::TcpListener;

use crate::connection::Connection;
use crate::error::{BindSnafu, Result};

#[derive(Debug, Serialize, Deserialize)]
pub struct ServerConfig {
    bind: String,
}

pub struct Server {
    storage: StorageRef,
    bind: String,
}

impl Server {
    pub fn new(storage: StorageRef, config: ServerConfig) -> Self {
        Self {
            storage,
            bind: config.bind,
        }
    }

    pub fn storage(&self) -> &Storage {
        &self.storage
    }

    pub async fn start(&self) -> Result<()> {
        // static self here is to make sure that the server is alive for the lifetime of the program
        info!("Rudeus listening on: {}", self.bind);
        let listener = TcpListener::bind(&self.bind).await.context(BindSnafu {
            bind: self.bind.clone(),
        })?;
        loop {
            let stream = listener.accept().await.map(|(socket, _)| socket).unwrap();
            let storage = self.storage.clone();
            tokio::spawn(async move {
                let mut conn = Connection::new(stream, storage);
                conn.start().await
            });
        }
    }
}
