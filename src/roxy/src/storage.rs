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

use std::mem::MaybeUninit;
use std::sync::Arc;

use common_base::bytes::Bytes;
use common_base::lock_pool;
use common_base::lock_pool::LockPool;
use common_telemetry::log::info;
use derive_builder::Builder;
use parking_lot::RwLock;
use rocksdb::{ReadOptions, WriteBatch, WriteOptions, DB};
use snafu::ResultExt;
use strum::VariantArray;

use crate::error::{
    CreateColumnFamilySnafu, InitializeStorageSnafu, ListColumnFamilySnafu, OpenRocksDBSnafu,
    Result, WriteToRocksDBSnafu,
};

#[derive(Builder, Debug, Clone)]
pub struct RocksDBConfig {
    #[builder(default = "4096")]
    block_size: usize,
}

#[derive(Builder, Debug)]
pub struct StorageConfig {
    dbpath: String,
    #[builder(default)]
    secondary_path: String,
    #[builder(default)]
    _open_mode: OpenMode,
    #[builder(default)]
    _backup_path: Option<String>,
    rocks_db: RocksDBConfig,
}

#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, VariantArray, strum::IntoStaticStr, strum::Display,
)]
#[strum(serialize_all = "lowercase")]
pub enum ColumnFamilyId {
    #[default]
    Default,
    Metadata,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum OpenMode {
    #[default]
    Default,
    ReadOnly,
    SecondaryInstance,
}

pub type StorageRef = Arc<Storage>;

pub struct Storage {
    // TODO: use MaybeUninit to avoid the need for Option in the future
    db: MaybeUninit<rocksdb::DB>,
    _env: rocksdb::Env,
    config: StorageConfig,
    db_closing: bool,

    /// This RwLock is used to allow multiple readers or one writer who may close the database
    db_lock: RwLock<()>,
    lock_pool: LockPool,
}

// mainly come from KVRocks
impl Storage {
    pub fn try_new(config: StorageConfig) -> Result<Self> {
        let env = rocksdb::Env::new().context(InitializeStorageSnafu)?;
        Ok(Self {
            db: MaybeUninit::uninit(),
            db_lock: RwLock::new(()),
            db_closing: true,
            _env: env,
            lock_pool: LockPool::new(16),
            config,
        })
    }

    fn init_rocksdb_options(&self) -> rocksdb::Options {
        let mut options = rocksdb::Options::default();
        options.create_if_missing(true);
        options
    }

    fn init_table_options(&self) -> rocksdb::BlockBasedOptions {
        let mut table_options = rocksdb::BlockBasedOptions::default();
        table_options.set_format_version(5);
        table_options.set_index_type(rocksdb::BlockBasedIndexType::TwoLevelIndexSearch);
        table_options.set_bloom_filter(
            10.0, false, /* this doesn't matter since `block_based`'s ignored*/
        );
        table_options.set_metadata_block_size(4096);
        table_options.set_data_block_index_type(rocksdb::DataBlockIndexType::BinaryAndHash);
        table_options.set_data_block_hash_ratio(0.75);
        table_options.set_block_size(self.config.rocks_db.block_size);
        table_options
    }

    fn create_column_families(&self, options: &rocksdb::Options) -> Result<()> {
        let cf_options = options.clone();
        let db = match DB::open(options, &self.config.dbpath) {
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

        ColumnFamilyId::VARIANTS
            .iter()
            .filter(|&&cf| cf != ColumnFamilyId::Default)
            .try_for_each(|&cf| {
                let cf_name: &str = cf.into();
                db.create_cf(cf_name, &cf_options)
                    .context(CreateColumnFamilySnafu {
                        column_family: cf_name,
                    })
            })?;
        Ok(())
    }

    pub fn open(&mut self, mode: OpenMode) -> Result<()> {
        let _guard = self.db_lock.write();

        self.db_closing = false;
        let options = self.init_rocksdb_options();

        if mode == OpenMode::Default {
            self.create_column_families(&options)?;
        }

        let metadata_table_opts = self.init_table_options();

        let mut metadata_opts = options.clone();
        metadata_opts.set_block_based_table_factory(&metadata_table_opts);

        let _subkey_table_opts = self.init_table_options();

        let subkey_opts = options.clone();

        let column_families = [
            rocksdb::ColumnFamilyDescriptor::new(ColumnFamilyId::Default.to_string(), subkey_opts),
            rocksdb::ColumnFamilyDescriptor::new(
                ColumnFamilyId::Metadata.to_string(),
                metadata_opts,
            ),
        ];

        let _old_column_families =
            rocksdb::DB::list_cf(&options, &self.config.dbpath).context(ListColumnFamilySnafu)?;

        let start = std::time::Instant::now();
        self.db.write(
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

    pub fn get_write_batch(&self) -> rocksdb::WriteBatch {
        rocksdb::WriteBatch::default()
    }

    pub fn close(&mut self) {
        let _guard = self.db_lock.write();
        let db = self.db();
        let _ = db.flush_wal(false);
        db.cancel_all_background_work(true);
        self.db_closing = true;
        unsafe {
            self.db.assume_init_drop();
        }
    }

    pub fn column_family_handler(&self, cf: ColumnFamilyId) -> rocksdb::ColumnFamilyRef {
        self.db().cf_handle(cf.into()).unwrap()
    }

    pub fn get(&self, opts: &ReadOptions, cf: ColumnFamilyId, key: &[u8]) -> Result<Option<Bytes>> {
        self.db()
            .get_cf_opt(&self.column_family_handler(cf), key, opts)
            .context(WriteToRocksDBSnafu)
            .map(|op| op.map(|v| v.into()))
    }

    pub fn write(&self, write_options: &WriteOptions, updates: WriteBatch) -> Result<()> {
        self.db()
            .write_opt(updates, write_options)
            .context(WriteToRocksDBSnafu)
    }

    pub fn lock_key(&self, key: Bytes) -> lock_pool::MutexGuard {
        self.lock_pool.lock(key)
    }

    pub(crate) fn db(&self) -> &DB {
        unsafe { self.db.assume_init_ref() }
    }
}

impl Drop for Storage {
    fn drop(&mut self) {
        self.close()
    }
}

#[cfg(test)]
pub fn setup_test_storage_for_ut() -> Storage {
    let dbpath_temp = tempfile::tempdir().unwrap();
    let dbpath = dbpath_temp.path().to_str().unwrap().to_string();
    let secondary_path = dbpath_temp
        .path()
        .join("secondary")
        .as_path()
        .to_str()
        .unwrap()
        .to_string();
    let mut storage = Storage::try_new(StorageConfig {
        dbpath,
        secondary_path,
        _open_mode: OpenMode::Default,
        _backup_path: None,
        rocks_db: RocksDBConfigBuilder::default().build().unwrap(),
    })
    .unwrap();
    storage.open(OpenMode::Default).unwrap();
    storage
}
