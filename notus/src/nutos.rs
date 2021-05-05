use crate::datastore::DataStore;
use std::path::Path;
use anyhow::Result;
use std::sync::Arc;

pub struct Notus {
    persistence: Arc<DataStore>
}
impl Notus {
    pub fn open<P : AsRef<Path>>(dir : P) -> Result<Self> {
        let store = DataStore::open(dir)?;
        Ok(Self {
            persistence: Arc::new(store)
        })
    }
    pub fn put(&self, key : Vec<u8>, value : Vec<u8>) -> Result<()> {
        self.persistence.put(key, value)
    }
    pub fn get(&self, key : Vec<u8>) -> Result<Option<Vec<u8>>> {
        self.persistence.get(key)
    }
    pub fn delete(&self, key : Vec<u8>) -> Result<()> {
        self.persistence.delete(key)
    }
    pub fn merge(&self) -> Result<()> {
        self.persistence.merge()
    }
}

#[test]
fn monotonic_inserts() {
    common::setup_logger();

    let db = Config::new().temporary(true).flush_every_ms(None).open().unwrap();

    for len in [1_usize, 16, 32, 1024].iter() {
        for i in 0_usize..*len {
            let mut k = vec![];
            for c in 0_usize..i {
                k.push((c % 256) as u8);
            }
            db.insert(&k, &[]).unwrap();
        }

        let count = db.iter().count();
        assert_eq!(count, *len as usize);

        let count2 = db.iter().rev().count();
        assert_eq!(count2, *len as usize);

        db.clear().unwrap();
    }

    for len in [1_usize, 16, 32, 1024].iter() {
        for i in (0_usize..*len).rev() {
            let mut k = vec![];
            for c in (0_usize..i).rev() {
                k.push((c % 256) as u8);
            }
            db.insert(&k, &[]).unwrap();
        }

        let count3 = db.iter().count();
        assert_eq!(count3, *len as usize);

        let count4 = db.iter().rev().count();
        assert_eq!(count4, *len as usize);

        db.clear().unwrap();
    }
}