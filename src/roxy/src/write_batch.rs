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

/// Scan the the puts and deletes of a [write batch](`rocksdb::WriteBatch`).
///
/// The application must provide an implementation of this trait when
/// scanning the operations within a `WriteBatch`
pub trait WriteBatchScanner {
    /// Called with a key and value that were `put` into the batch.
    fn put(&mut self, key: Box<[u8]>, value: Box<[u8]>);
    /// Called with a key that was `delete`d from the batch.
    fn delete(&mut self, key: Box<[u8]>);
}

pub struct RocksDBWriteBatchIterator<'a> {
    scanner: &'a mut dyn WriteBatchScanner,
}

impl<'a> rocksdb::WriteBatchIterator for RocksDBWriteBatchIterator<'a> {
    fn put(&mut self, key: Box<[u8]>, value: Box<[u8]>) {
        self.scanner.put(key, value)
    }

    fn delete(&mut self, key: Box<[u8]>) {
        self.scanner.delete(key)
    }
}
