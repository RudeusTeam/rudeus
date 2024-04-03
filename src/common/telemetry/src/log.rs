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

use std::sync::{Arc, Mutex, Once};

use derive_builder::Builder;
use once_cell::sync::Lazy;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_error::ErrorLayer;
pub use tracing_log::log::*;
use tracing_log::LogTracer;
use tracing_subscriber::fmt::Layer;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::Registry;

#[derive(Debug, Builder)]
pub struct LoggingOption {
    append_stdout: bool,
}

pub fn init(opts: &LoggingOption) -> Vec<WorkerGuard> {
    let mut guards = vec![];
    LogTracer::init().expect("log tracer must be valid");
    let stdout_logging_layer = if opts.append_stdout {
        let (stdout_writer, stdout_guard) = tracing_appender::non_blocking(std::io::stdout());
        guards.push(stdout_guard);

        Some(Layer::new().with_writer(stdout_writer).with_ansi(true))
    } else {
        None
    };
    let subscriber = Registry::default()
        .with(stdout_logging_layer)
        .with(ErrorLayer::default());
    tracing::subscriber::set_global_default(subscriber)
        .expect("error setting global tracing subscriber");
    guards
}

static GLOBAL_UT_LOG_GUARDS: Lazy<Arc<Mutex<Option<Vec<WorkerGuard>>>>> =
    Lazy::new(|| Arc::new(Mutex::new(None)));

/// Initialize logging for unit test
pub fn init_ut_logging() {
    static START: Once = Once::new();
    START.call_once(|| {
        let mut g = GLOBAL_UT_LOG_GUARDS.lock().unwrap();
        let opts = LoggingOptionBuilder::default()
            .append_stdout(true)
            .build()
            .unwrap();
        *g = Some(init(&opts))
    });
}
