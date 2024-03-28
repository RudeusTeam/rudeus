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

use snafu::{Location, Snafu};

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Fail to initialize storage"))]
    InitializeStorage {
        source: rocksdb::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Fail to create column family: {}", column_family))]
    CreateColumnFamily {
        source: rocksdb::Error,
        column_family: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Fail to open rocksdb in path: {}", dbpath))]
    OpenRocksDB {
        source: rocksdb::Error,
        dbpath: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Fail to list column family"))]
    ListColumnFamily { source: rocksdb::Error },
}

pub type Result<T> = std::result::Result<T, Error>;
