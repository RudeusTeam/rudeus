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

use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;

use common_telemetry::log::info;
use snafu::ResultExt;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use crate::error::{BuildRuntimeSnafu, Result};

/// A wrapper of tokio runtime
#[derive(Debug, Clone)]
pub struct Runtime {
    /// Name of the runtime
    name: String,
    /// Handle of the runtime
    handle: tokio::runtime::Handle,

    // Used to receive a drop signal when dropper is dropped, inspired by greptime
    _dropper: Arc<Dropper>,
}

#[derive(Debug)]
pub struct Dropper {
    close: Option<oneshot::Sender<()>>,
}

impl Drop for Dropper {
    fn drop(&mut self) {
        let _ = self.close.take().map(|v| v.send(()));
    }
}

impl Runtime {
    pub fn builder() -> Builder {
        Default::default()
    }

    /// spawn a future to the runtime
    pub fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.handle.spawn(future)
    }

    /// Spawn a blocking function onto the runtime using the handle
    pub fn spawn_blocking<F, R>(&self, func: F) -> JoinHandle<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        self.handle.spawn_blocking(func)
    }

    pub fn block_on<F: Future>(&self, future: F) -> F::Output {
        self.handle.block_on(future)
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

pub struct Builder {
    /// Name of the runtime
    rt_name: String,
    /// Name of the thread in the runtime
    thread_name: String,
    /// tokio runtime builder
    builder: tokio::runtime::Builder,
}

static RUNTIME_ID: AtomicUsize = AtomicUsize::new(0);

impl Default for Builder {
    fn default() -> Self {
        Self {
            rt_name: format!("runtime-{}", RUNTIME_ID.fetch_add(1, Ordering::Relaxed)),
            thread_name: "rudeus-default-worker".to_string(),
            builder: tokio::runtime::Builder::new_multi_thread(),
        }
    }
}

impl Builder {
    /// sets the number of worker threads for the runtime to [`cnt`]
    /// This function actually delegate to [`tokio::runtime::Builder::worker_threads`]
    pub fn worker_threads(&mut self, cnt: usize) -> &mut Self {
        self.builder.worker_threads(cnt);
        self
    }

    pub fn thread_name(&mut self, name: impl Into<String>) -> &mut Self {
        self.thread_name = format!("rudeus-{}", name.into());
        self
    }

    pub fn runtime_name(&mut self, name: impl Into<String>) -> &mut Self {
        self.rt_name = name.into();
        self
    }

    pub fn build(&mut self) -> Result<Runtime> {
        let tk_rt = self
            .builder
            .enable_all()
            .thread_name(self.thread_name.clone())
            .build()
            .context(BuildRuntimeSnafu)?;
        let name = self.rt_name.clone();
        let handle = tk_rt.handle().clone();
        let (close_tx, close_rx) = oneshot::channel();
        // Block the runtime and let this thread own the runtime
        // This is to ensure the runtime is not dropped until the dropper is dropped
        let _ = thread::Builder::new()
            .name(format!("{}-blocker", self.thread_name))
            .spawn(move || tk_rt.block_on(close_rx));
        Ok(Runtime {
            name,
            handle,
            _dropper: Arc::new(Dropper {
                close: Some(close_tx),
            }),
        })
    }
}

pub fn create_runtime(runtime_name: &str, thread_name: &str, worker_threads: usize) -> Runtime {
    info!("Creating runtime with runtime_name: {runtime_name}, thread_name: {thread_name},worker_threads: {worker_threads}.");

    Builder::default()
        .runtime_name(runtime_name)
        .thread_name(thread_name)
        .worker_threads(worker_threads)
        .build()
        .expect("Failed to build runtime")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_runtime() -> Arc<Runtime> {
        let rt = Builder::default()
            .worker_threads(2)
            .thread_name("test ")
            .build();
        Arc::new(rt.unwrap())
    }

    #[test]
    fn test_block_on_async() {
        let rt = create_runtime();
        let out = rt.block_on(async {
            let (tx, rx) = oneshot::channel();

            let _ = thread::spawn(move || {
                thread::sleep(std::time::Duration::from_secs(1));
                tx.send("BAZINGA").unwrap();
            });
            rx.await.unwrap()
        });
        assert_eq!(out, "BAZINGA")
    }

    #[test]
    fn test_spawn_from_blocking() {
        let rt = create_runtime();
        let rt1 = rt.clone();
        let out = rt.block_on(async {
            let rt2 = rt1.clone();
            let inner = rt1
                .spawn_blocking(move || rt2.spawn(async move { "BAZINGA" }))
                .await
                .unwrap();
            inner.await.unwrap()
        });
        assert_eq!(out, "BAZINGA")
    }

    #[test]
    fn test_spawn_join() {
        let rt = create_runtime();
        let handle = rt.spawn(async { "BAZINGA" });
        assert_eq!(rt.block_on(handle).unwrap(), "BAZINGA")
    }
}
