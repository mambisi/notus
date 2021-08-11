use crate::datastore::{DataStore, MergeOperator, RawKey, DEFAULT_INDEX};
use crate::errors::NotusError;
use crate::Result;
use std::alloc::Global;
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::ops::{RangeFrom, Range, RangeBounds};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::thread;
use std::time::Duration;
use std::ops::Bound;
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
                Ok((k, v)) => out.push_str(&format!("{:?} : {:?} \n", k, v)),
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
                thread::sleep(Duration::from_millis(10));
                let is_dropped = is_dropped.load(Ordering::Acquire);
                if is_dropped {
                    break;
                }
                store.flush();
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
        self.store
            .put(RawKey(DEFAULT_INDEX.to_string(), key), value)
    }
    pub fn get(&self, key: &Vec<u8>) -> Result<Option<Vec<u8>>> {
        if key.is_empty() {
            return Ok(None);
        }
        self.store
            .get(&RawKey(DEFAULT_INDEX.to_string(), key.clone()))
    }

    pub fn contains(&self, key: &Vec<u8>) -> Result<bool> {
        if key.is_empty() {
            return Ok(false);
        }
        self.store
            .contains(&RawKey(DEFAULT_INDEX.to_string(), key.clone()))
    }

    pub fn delete(&self, key: &Vec<u8>) -> Result<()> {
        if key.is_empty() {
            return Ok(());
        }
        self.store
            .delete(&RawKey(DEFAULT_INDEX.to_string(), key.clone()))
    }

    pub fn compact(&self) -> Result<()> {
        self.store.merge()
    }

    pub fn clear(&self) -> Result<()> {
        self.store.clear()
    }

    pub fn put_cf(&self, column: &str, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        self.store.put(RawKey(column.to_string(), key), value)
    }
    pub fn merge_cf(
        &self,
        merge_operator: impl MergeOperator + 'static,
        column: &str,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> Result<()> {
        let old_value = self.store.get(&RawKey(column.to_string(), key.clone()))?;

        let merged_value = merge_operator(&key, old_value, &value);
        match merged_value {
            None => {
                self.delete_cf(column, &key);
            }
            Some(value) => {
                self.put_cf(column, key, value);
            }
        }
        Ok(())
    }
    pub fn merge(
        &self,
        merge_operator: impl MergeOperator + 'static,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> Result<()> {
        let column = DEFAULT_INDEX.to_string();
        let old_value = self.store.get(&RawKey(column.clone(), key.clone()))?;

        let merged_value = merge_operator(&key, old_value, &value);

        match merged_value {
            None => {
                self.delete(&key);
            }
            Some(value) => {
                self.put(key, value);
            }
        }
        Ok(())
    }
    pub fn get_cf(&self, column: &str, key: &Vec<u8>) -> Result<Option<Vec<u8>>> {
        if key.is_empty() {
            return Ok(None);
        }
        self.store.get(&RawKey(column.to_string(), key.clone()))
    }

    pub fn contains_cf(&self,  column: &str, key: &Vec<u8>) -> Result<bool> {
        if key.is_empty() {
            return Ok(false);
        }
        self.store
            .contains(&RawKey(column.to_string(), key.clone()))
    }

    pub fn delete_cf(&self, column: &str, key: &Vec<u8>) -> Result<()> {
        if key.is_empty() {
            return Ok(());
        }
        self.store.delete(&RawKey(column.to_string(), key.clone()))
    }

    pub fn range_cf<R>(&self, column: &str, range : R) -> DBIterator where R : RangeBounds<Vec<u8>>{
        DBIterator::range(column, self.store.clone(), range)
    }

    pub fn prefix_cf(&self, column: &str, prefix: &Vec<u8>) -> DBIterator {
        DBIterator::prefix(column, self.store.clone(), prefix)
    }

    pub fn iter_cf(&self, column: &str) -> DBIterator {
        DBIterator::new(column, self.store.clone())
    }

    pub fn iter(&self) -> DBIterator {
        DBIterator::new(DEFAULT_INDEX, self.store.clone())
    }

    pub fn range<R>(&self, range :R) -> DBIterator where R : RangeBounds<Vec<u8>> {
        DBIterator::range(DEFAULT_INDEX, self.store.clone(), range)
    }

    pub fn prefix(&self, prefix: &Vec<u8>) -> DBIterator {
        DBIterator::prefix(DEFAULT_INDEX, self.store.clone(), prefix)
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
    column: String,
    store: Arc<DataStore>,
    inner: Vec<Vec<u8>>,
    cursor: usize,
}

impl DBIterator {
    fn new(column: &str, store: Arc<DataStore>) -> Self {
        let keys = store.keys(column);
        Self {
            column: column.to_string(),
            store,
            inner: keys,
            cursor: 0,
        }
    }

    fn range<R>(column: &str, store: Arc<DataStore>, range : R) -> Self where  R : RangeBounds<Vec<u8>> {
        let keys = store.range(column, range);
        Self {
            column: column.to_string(),
            store,
            inner: keys,
            cursor: 0,
        }
    }

    fn prefix(column: &str, store: Arc<DataStore>, prefix: &Vec<u8>) -> Self {
        let keys = store.prefix(column, prefix);
        Self {
            column: column.to_string(),
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
            Some(key) => key,
        };
        match self.store.get(&RawKey(self.column.clone(), key.clone())) {
            Ok(Some(value)) => {
                self.cursor += 1;
                Some(Ok((key.clone(), value)))
            }
            _ => None,
        }
    }
}

impl DoubleEndedIterator for DBIterator {
    fn next_back(&mut self) -> Option<Self::Item> {
        let position = match self.inner.len().checked_sub(1) {
            None => {
                return None;
            }
            Some(position) => match position.checked_sub(self.cursor) {
                None => {
                    return None;
                }
                Some(position) => position,
            },
        };

        let key = match self.inner.get(position) {
            None => {
                return None;
            }
            Some(key) => key,
        };

        match self.store.get(&RawKey(self.column.clone(), key.clone())) {
            Ok(Some(value)) => {
                self.cursor += 1;
                Some(Ok((key.clone(), value)))
            }
            _ => None,
        }
    }
}
