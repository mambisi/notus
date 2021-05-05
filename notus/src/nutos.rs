use crate::datastore::{DataStore, KeyDirEntry};
use std::path::Path;
use anyhow::{Result, Error};
use std::sync::Arc;
use std::alloc::Global;
use std::fs::read;

pub struct Notus {
    store: Arc<DataStore>
}
impl Notus {
    pub fn open<P : AsRef<Path>>(dir : P) -> Result<Self> {
        let store = DataStore::open(dir)?;
        Ok(Self {
            store: Arc::new(store)
        })
    }
    pub fn put(&self, key : Vec<u8>, value : Vec<u8>) -> Result<()> {
        self.store.put(key, value)
    }
    pub fn get(&self, key : Vec<u8>) -> Result<Option<Vec<u8>>> {
        if key.is_empty(){
            return Ok(None)
        }
        self.store.get(key)
    }
    pub fn delete(&self, key : Vec<u8>) -> Result<()> {
        if key.is_empty(){
            return Ok(())
        }
        self.store.delete(key)
    }
    pub fn merge(&self) -> Result<()> {
        self.store.merge()
    }

    pub fn clear(&self) -> Result<()> {
        self.store.clear()
    }

    pub fn iter(&self) -> DBIterator {
        DBIterator {
            store: self.store.clone(),
            inner: self.store.keys(),
            cursor: 0
        }
    }
}

pub struct DBIterator {
    store: Arc<DataStore>,
    inner : Vec<Vec<u8>>,
    cursor : usize
}

impl DBIterator {
    fn new(store : Arc<DataStore>) -> Self {
        let keys = store.keys();
        Self {
            store,
            inner: keys,
            cursor: 0
        }
    }
}

impl Iterator for DBIterator {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        let key = match self.inner.get(self.cursor) {
            None => {
                return None
            }
            Some(key) => {
                key.clone()
            }
        };

        match self.store.get(key.clone()) {
            Ok(Some(value)) => {
                self.cursor += 1;
                Some(Ok((key,value)))
            }
            _ => {
                None
            }
        }
    }


}

impl DoubleEndedIterator for DBIterator {
    fn next_back(&mut self) -> Option<Self::Item> {
        let position = match self.inner.len().checked_sub(1) {
            None => {
                return None
            }
            Some(position) => {
                match  position.checked_sub(self.cursor) {
                    None => {
                        return None
                    }
                    Some(position) => {
                        position
                    }
                }
            }
        };

        let key = match self.inner.get(position) {
            None => {
                return None
            }
            Some(key) => {
                key.clone()
            }
        };

        match self.store.get(key.clone()) {
            Ok(Some(value)) => {
                self.cursor += 1;
                Some(Ok((key,value)))
            }
            _ => {
                None
            }
        }
    }
}

fn clean_up() {
    fs_extra::dir::remove("./testdir/_test_monotonic_inserts");
}

#[test]
fn monotonic_inserts() {
    clean_up();
    let db = Notus::open("./testdir/_test_monotonic_inserts").unwrap();

    for len in [1_usize, 16, 32, 1024].iter() {
        for i in 0_usize..*len {
            let mut k = vec![];
            for c in 0_usize..i {
                k.push((c % 256) as u8);
            }
            db.put(k, vec![]).unwrap();
        }


        let count = db.iter().count();
        assert_eq!(count, *len as usize);

        let count2 = db.iter().rev().count();
        assert_eq!(count2, *len as usize);

        db.clear().unwrap();
        //clean_up();
    }


    for len in [1_usize, 16, 32, 1024].iter() {
        for i in (0_usize..*len).rev() {
            let mut k = vec![];
            for c in (0_usize..i).rev() {
                k.push((c % 256) as u8);
            }
            db.put(k, vec![]).unwrap();
        }

        let count3 = db.iter().count();
        assert_eq!(count3, *len as usize);

        let count4 = db.iter().rev().count();
        assert_eq!(count4, *len as usize);

        db.clear().unwrap();
    }
}