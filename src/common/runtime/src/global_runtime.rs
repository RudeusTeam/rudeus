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

//! Global runtime management

use std::future::Future;
use std::sync::{Mutex, Once};

use once_cell::sync::Lazy;
use paste::paste;
use tokio::task::JoinHandle;

use crate::runtime::{create_runtime, Runtime};

#[derive(Default)]
struct ConfigRuntimes {
    read_runtime: Option<Runtime>,
    write_runtime: Option<Runtime>,
    bg_runtime: Option<Runtime>,
    network_runtime: Option<Runtime>,
    already_init: bool,
}

struct GlobalRuntimes {
    read_runtime: Runtime,
    write_runtime: Runtime,
    bg_runtime: Runtime,
    network_runtime: Runtime,
}

impl GlobalRuntimes {
    fn new(
        read_runtime: Option<Runtime>,
        write_runtime: Option<Runtime>,
        bg_runtime: Option<Runtime>,
        network_runtime: Option<Runtime>,
    ) -> Self {
        Self {
            read_runtime: read_runtime.unwrap_or_else(|| {
                create_runtime("global-read", "read-worker", DEFAULT_READ_WORKER_THREADS)
            }),
            write_runtime: write_runtime.unwrap_or_else(|| {
                create_runtime("global-write", "write-worker", DEFAULT_WRITE_WORKER_THREADS)
            }),
            bg_runtime: bg_runtime.unwrap_or_else(|| {
                create_runtime("global-bg", "bg-worker", DEFAULT_BG_WORKER_THREADS)
            }),
            network_runtime: network_runtime.unwrap_or_else(|| {
                create_runtime(
                    "global-network",
                    "network-worker",
                    DEFAULT_NETWORK_WORKER_THREADS,
                )
            }),
        }
    }
}

const DEFAULT_READ_WORKER_THREADS: usize = 4;
const DEFAULT_WRITE_WORKER_THREADS: usize = 4;
const DEFAULT_BG_WORKER_THREADS: usize = 4;
const DEFAULT_NETWORK_WORKER_THREADS: usize = 4;

static GLOBAL_RUNTIMES: Lazy<GlobalRuntimes> = Lazy::new(|| {
    let mut c = CONFIG_RUNTIMES.lock().unwrap();
    let r = c.read_runtime.take();
    let w = c.write_runtime.take();
    let bg = c.bg_runtime.take();
    let network = c.network_runtime.take();
    c.already_init = true;
    GlobalRuntimes::new(r, w, bg, network)
});

static CONFIG_RUNTIMES: Lazy<Mutex<ConfigRuntimes>> =
    Lazy::new(|| Mutex::new(ConfigRuntimes::default()));

/// Initialize the global runtimes
///
/// # Panics
/// Panics when the global runtimes are already initialized.
/// You should call this function before using any runtime functions.
pub fn init_global_runtimes(
    read: Option<Runtime>,
    write: Option<Runtime>,
    background: Option<Runtime>,
) {
    static START: Once = Once::new();
    START.call_once(move || {
        let mut c = CONFIG_RUNTIMES.lock().unwrap();
        assert!(!c.already_init, "Global runtimes already initialized");
        c.read_runtime = read;
        c.write_runtime = write;
        c.bg_runtime = background;
    });
}

macro_rules! define_global_runtime_spawnning {
    ($name:ident) => {
        paste! {
            pub fn [<$name _runtime>]() -> Runtime{
                GLOBAL_RUNTIMES.[<$name _runtime>].clone()
            }

            pub fn [<spawn_ $name>]<F>(future: F) -> JoinHandle<F::Output>
            where
                F: Future + Send + 'static,
                F::Output: Send + 'static,
            {
                GLOBAL_RUNTIMES.[<$name _runtime>].spawn(future)
            }

            pub fn [<spawn_blocking_ $name>]<F, R>(func: F) -> JoinHandle<R>
            where
                F: FnOnce() -> R + Send + 'static,
                R: Send + 'static,
            {
                GLOBAL_RUNTIMES.[<$name _runtime>].spawn_blocking(func)
            }

            pub fn [<runtime_name_ $name>]() -> String {
                GLOBAL_RUNTIMES.[<$name _runtime>].name().to_string()
            }

            pub fn [<block_on_ $name>]<F: Future>(future: F) -> F::Output {
                GLOBAL_RUNTIMES.[<$name _runtime>].block_on(future)
            }
        }
    };
}

define_global_runtime_spawnning!(read);
define_global_runtime_spawnning!(write);
define_global_runtime_spawnning!(bg);
define_global_runtime_spawnning!(network);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_spawn_block_on() {
        let handle = spawn_read(async { 1 + 1 });
        assert_eq!(2, block_on_read(handle).unwrap());

        let handle = spawn_write(async { 2 + 2 });
        assert_eq!(4, block_on_write(handle).unwrap());

        let handle = spawn_bg(async { 3 + 3 });
        assert_eq!(6, block_on_bg(handle).unwrap());

        let handle = spawn_network(async { 4 + 4 });
        assert_eq!(8, block_on_network(handle).unwrap());
    }

    macro_rules! define_spawn_blocking_test {
        ($type: ident) => {
            paste! {
                #[test]
                fn [<test_spawn_ $type _from_blocking>]() {
                    let runtime = [<$type _runtime>]();
                    let out = runtime.block_on(async move {
                        let inner = [<spawn_blocking_  $type>](move || {
                                [<spawn_ $type>](async move { "hello" })
                            }).await.unwrap();

                        inner.await.unwrap()
                    });

                    assert_eq!(out, "hello")
                }
            }
        };
    }

    define_spawn_blocking_test!(read);
    define_spawn_blocking_test!(write);
    define_spawn_blocking_test!(bg);
    define_spawn_blocking_test!(network);
}
