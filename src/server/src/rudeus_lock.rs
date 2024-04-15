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

//! Rudeus lock is a simple file lock implementation for ensuring only one instance of a program is running.

use std::fs::{File, OpenOptions};

use fs4::FileExt as _;
use snafu::ResultExt as _;

use crate::error::{AcquireFileLockSnafu, InstanceAlreadyExistsSnafu, Result};

pub struct RudeusLock {
    file: File,
}

impl RudeusLock {
    fn new(file: File) -> Result<Self> {
        file.try_lock_exclusive()
            .context(InstanceAlreadyExistsSnafu)?;
        Ok(Self { file })
    }
}

impl Drop for RudeusLock {
    fn drop(&mut self) {
        self.file.unlock().unwrap();
    }
}

pub fn try_lock_rudeus(storage_path: &str) -> Result<RudeusLock> {
    let path = std::path::Path::new(storage_path).join("rudeus.lock");
    let file = OpenOptions::new()
        .write(true)
        .read(true)
        .create(true)
        .truncate(true)
        .open(&path)
        .context(AcquireFileLockSnafu {
            path: path.display().to_string(),
        })?;
    RudeusLock::new(file)
}
