use crate::datastore::{DataStore};
use std::path::{Path, PathBuf};
use anyhow::{Result};
use std::sync::{Arc};
use std::ops::{ RangeFrom};
use std::fmt::{Display, Formatter};
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;

pub struct Notus {
    dir: PathBuf,
    temp: bool,
    store: Arc<DataStore>,
    dropped: Arc<AtomicBool>,
}

impl Display for Notus {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut out = String::new();
        for res in self.iter() {
            match res {
                Ok((k, v)) => {
                    out.push_str(&format!("{:?} : {:?} \n", k, v))
                }
                Err(_) => {}
            }
        }
        writeln!(f, "{}", out)
    }
}

impl Notus {
    pub fn open<P: AsRef<Path>>(dir: P) -> Result<Self> {
        let store = Arc::new(DataStore::open(dir.as_ref())?);
        let instance = Self {
            dir: PathBuf::from(dir.as_ref()),
            temp: false,
            store,
            dropped: Arc::new(AtomicBool::new(false)),
        };
        instance.start_background_workers();
        Ok(instance)
    }

    fn start_background_workers(&self) {
        let is_dropped = self.dropped.clone();
        let store = self.store.clone();
        thread::spawn(move || {
            loop {
                let is_dropped = is_dropped.load(Ordering::Acquire);
                if is_dropped {
                    break;
                }
                store.flush();
                thread::sleep(Duration::from_millis(1));
            }
            drop(store)
        });
    }

    pub fn temp<P: AsRef<Path>>(dir: P) -> Result<Self> {
        let store = Arc::new(DataStore::open(dir.as_ref())?);
        let instance = Self {
            dir: PathBuf::from(dir.as_ref()),
            temp: true,
            store,
            dropped: Arc::new(AtomicBool::new(false)),
        };
        instance.start_background_workers();
        Ok(instance)
    }
    pub fn put(&self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        self.store.put(key, value)
    }
    pub fn get(&self, key: &Vec<u8>) -> Result<Option<Vec<u8>>> {
        if key.is_empty() {
            return Ok(None);
        }
        self.store.get(key)
    }
    pub fn delete(&self, key: &Vec<u8>) -> Result<()> {
        if key.is_empty() {
            return Ok(());
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
        DBIterator::new(self.store.clone())
    }

    pub fn range(&self, range: RangeFrom<Vec<u8>>) -> DBIterator {
        DBIterator::range(self.store.clone(), range)
    }

    pub fn prefix(&self, prefix: &Vec<u8>) -> DBIterator {
        DBIterator::prefix(self.store.clone(), prefix)
    }
}

impl Drop for Notus {
    fn drop(&mut self) {
        self.dropped.store(true, Ordering::Release);
        if self.temp {
            //fs_extra::dir::remove(self.dir.as_path());
        }
    }
}

pub struct DBIterator {
    store: Arc<DataStore>,
    inner: Vec<Vec<u8>>,
    cursor: usize,
}

impl DBIterator {
    fn new(store: Arc<DataStore>) -> Self {
        let keys = store.keys();
        Self {
            store,
            inner: keys,
            cursor: 0,
        }
    }

    fn range(store: Arc<DataStore>, range: RangeFrom<Vec<u8>>) -> Self {
        let keys = store.range(range);
        Self {
            store,
            inner: keys,
            cursor: 0,
        }
    }

    fn prefix(store: Arc<DataStore>, prefix: &Vec<u8>) -> Self {
        let keys = store.prefix(prefix);
        Self {
            store,
            inner: keys,
            cursor: 0,
        }
    }
}

impl Iterator for DBIterator {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        let key = match self.inner.get(self.cursor) {
            None => {
                return None;
            }
            Some(key) => {
                key
            }
        };

        match self.store.get(key) {
            Ok(Some(value)) => {
                self.cursor += 1;
                Some(Ok((key.clone(), value)))
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
                return None;
            }
            Some(position) => {
                match position.checked_sub(self.cursor) {
                    None => {
                        return None;
                    }
                    Some(position) => {
                        position
                    }
                }
            }
        };

        let key = match self.inner.get(position) {
            None => {
                return None;
            }
            Some(key) => {
                key
            }
        };

        match self.store.get(key) {
            Ok(Some(value)) => {
                self.cursor += 1;
                Some(Ok((key.clone(), value)))
            }
            _ => {
                None
            }
        }
    }
}

