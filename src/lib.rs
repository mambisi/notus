use crate::datastore::DataStore;
use std::path::Path;
use anyhow::Result;

mod schema;
mod file_ops;
mod datastore;

pub struct Notus {
    store : DataStore
}
impl Notus {
    pub fn open<P : AsRef<Path>>(dir : P) -> Result<Self> {
        let store = DataStore::open(dir)?;
        Ok(Self {
            store
        })
    }
    pub fn put(&mut self, key : Vec<u8>, value : Vec<u8>) -> Result<()> {
        self.store.put(key,value)
    }
    pub fn get(&mut self, key : Vec<u8>) -> Result<Option<Vec<u8>>> {
        self.store.get(key)
    }
    pub fn delete(&mut self, key : Vec<u8>) -> Result<()> {
        self.store.delete(key)
    }
    pub fn merge(&mut self) -> Result<()> {
        self.store.merge()
    }
}