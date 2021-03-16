#![feature(allocator_api)]

use crate::datastore::DataStore;
use std::path::Path;
use anyhow::Result;

mod schema;
mod file_ops;
mod datastore;

pub struct Notus<> {
    persistence: DataStore
}
impl Notus {
    pub fn open<P : AsRef<Path>>(dir : P) -> Result<Self> {
        let store = DataStore::open(dir)?;
        Ok(Self {
            persistence: store
        })
    }
    pub fn put(&mut self, key : Vec<u8>, value : Vec<u8>) -> Result<()> {
        self.persistence.put(key, value)
    }
    pub fn get(&mut self, key : Vec<u8>) -> Result<Option<Vec<u8>>> {
        self.persistence.get(key)
    }
    pub fn delete(&mut self, key : Vec<u8>) -> Result<()> {
        self.persistence.delete(key)
    }
    pub fn merge(&mut self) -> Result<()> {
        self.persistence.merge()
    }
}