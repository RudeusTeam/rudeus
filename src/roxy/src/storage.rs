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

use std::sync::Arc;

use common_telemetry::log::info;
use derive_builder::Builder;
use parking_lot::RwLock;
use rocksdb::DB;
use snafu::ResultExt;
use strum::{Display, VariantArray};

use crate::error::{
    CreateColumnFamilySnafu, InitializeStorageSnafu, ListColumnFamilySnafu, OpenRocksDBSnafu,
    Result,
};

#[derive(Builder, Debug, Clone)]
pub struct RocksDBConfig {
    block_size: usize,
}
#[derive(Builder, Debug)]
pub struct StorageConfig {
    dbpath: String,
    secondary_path: String,
    _open_mode: OpenMode,
    _backup_path: Option<String>,
    rocks_db: RocksDBConfig,
}

#[derive(Debug, Clone, Copy, Default, Display, VariantArray, PartialEq, Eq)]
#[strum(serialize_all = "lowercase")]
pub enum ColumnFamily {
    #[default]
    Default,
    Metadata,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpenMode {
    Default,
    ReadOnly,
    SecondaryInstance,
}

pub type StorageRef = Arc<Storage>;

pub struct Storage {
    inner: RwLock<Inner>,
}

impl Storage {
    pub fn try_new(config: StorageConfig) -> Result<Self> {
        Ok(Self {
            inner: RwLock::new(Inner::try_new(config)?),
        })
    }
    pub fn open(&mut self, mode: OpenMode) -> Result<()> {
        self.inner.write().open(mode)
    }
}

struct Inner {
    // TODO: use MaybeUninit to avoid the need for Option in the future
    db: Option<rocksdb::DB>,
    _env: rocksdb::Env,
    config: StorageConfig,
    db_closing: bool,
}

// mainly come from KVRocks
impl Inner {
    fn try_new(config: StorageConfig) -> Result<Self> {
        let env = rocksdb::Env::new().context(InitializeStorageSnafu)?;
        Ok(Self {
            db: None,
            db_closing: true,
            _env: env,
            config,
        })
    }

    fn init_rocksdb_options() -> rocksdb::Options {
        let mut options = rocksdb::Options::default();
        options.create_if_missing(true);
        options
    }

    fn init_table_options(&self) -> rocksdb::BlockBasedOptions {
        let mut table_options = rocksdb::BlockBasedOptions::default();
        table_options.set_format_version(5);
        table_options.set_index_type(rocksdb::BlockBasedIndexType::TwoLevelIndexSearch);
        table_options.set_bloom_filter(10.0, false);
        table_options.set_metadata_block_size(4096);
        table_options.set_data_block_index_type(rocksdb::DataBlockIndexType::BinaryAndHash);
        table_options.set_data_block_hash_ratio(0.75);
        table_options.set_block_size(self.config.rocks_db.block_size);
        table_options
    }

    fn create_column_families(&self, options: &rocksdb::Options) -> Result<()> {
        let cf_options = options.clone();
        let mut db = match DB::open(options, &self.config.dbpath) {
            // TODO(j0hn50n133): This is a hack to ignore the error when the column families are not opened
            // remove this hack in the future
            // Rocksdb could not be opened if we don't list all column families
            // therefore if the column families are already created, we won't be able to open the db here
            Err(e) if e.as_ref().contains("Column families not opened") => return Ok(()),
            r => r,
        }
        .context(OpenRocksDBSnafu {
            dbpath: self.config.dbpath.clone(),
        })?;

        ColumnFamily::VARIANTS
            .iter()
            .filter(|&&cf| cf != ColumnFamily::Default)
            .try_for_each(|&cf| {
                let cf_name = cf.to_string();
                db.create_cf(&cf_name, &cf_options)
                    .context(CreateColumnFamilySnafu {
                        column_family: cf_name,
                    })
            })?;
        Ok(())
    }

    fn open(&mut self, mode: OpenMode) -> Result<()> {
        self.db_closing = false;
        let options = Self::init_rocksdb_options();

        if mode == OpenMode::Default {
            self.create_column_families(&options)?;
        }

        let _metadata_table_opts = self.init_table_options();

        let metadata_opts = options.clone();

        let _subkey_table_opts = self.init_table_options();

        let subkey_opts = options.clone();

        let column_families = [
            rocksdb::ColumnFamilyDescriptor::new(ColumnFamily::Default.to_string(), subkey_opts),
            rocksdb::ColumnFamilyDescriptor::new(ColumnFamily::Metadata.to_string(), metadata_opts),
        ];

        let _old_column_families =
            rocksdb::DB::list_cf(&options, &self.config.dbpath).context(ListColumnFamilySnafu)?;

        let start = std::time::Instant::now();
        self.db = Some(
            match mode {
                OpenMode::Default => DB::open_cf_descriptors(
                    &options,
                    &self.config.dbpath,
                    column_families.into_iter(),
                ),
                OpenMode::ReadOnly => DB::open_cf_descriptors_read_only(
                    &options,
                    &self.config.dbpath,
                    column_families.into_iter(),
                    true, // TODO(j0hn50n133): find out reasonable way to deal with this, Maybe having a log means some one is writing to the db
                ),
                OpenMode::SecondaryInstance => DB::open_cf_descriptors_as_secondary(
                    &options,
                    &self.config.dbpath,
                    &self.config.secondary_path,
                    column_families.into_iter(),
                ),
            }
            .context(OpenRocksDBSnafu {
                dbpath: self.config.dbpath.clone(),
            })?,
        );
        let end = std::time::Instant::now();
        let duration = end - start;

        info!(
            "Success to load the data from disk: {}ms",
            duration.as_millis()
        );

        Ok(())
    }

    fn close(&mut self) {
        if let Some(db) = self.db.take() {
            let _ = db.flush_wal(false);
            db.cancel_all_background_work(true);
        };
        self.db_closing = true;
    }
}

impl Drop for Inner {
    fn drop(&mut self) {
        self.close()
    }
}

#[cfg(test)]
mod tests {

    use common_telemetry::log::LoggingOptionBuilder;

    use super::*;

    #[test]
    fn test() {
        common_telemetry::log::init(
            &LoggingOptionBuilder::default()
                .append_stdout(true)
                .build()
                .unwrap(),
        );
        let mut storage = Inner::try_new(StorageConfig {
            dbpath: "test".to_string(),
            secondary_path: "test/secondary".to_string(),
            _open_mode: OpenMode::Default,
            _backup_path: None,
            rocks_db: RocksDBConfig { block_size: 4096 },
        })
        .unwrap();
        storage.open(OpenMode::Default).unwrap();
    }
}
