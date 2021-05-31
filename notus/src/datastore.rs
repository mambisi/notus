use crate::errors::NotusError;
use crate::file_ops::{
    create_new_file_pair, fetch_file_pairs, get_lock_file, ActiveFilePair, FilePair,
};
use crate::schema::DataEntry;
use bincode::ErrorKind;
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use std::alloc::Global;
use std::collections::{BTreeMap, HashMap};
use std::fs::File;
use std::ops::{RangeFrom, RangeBounds, Range, RangeInclusive, RangeToInclusive, RangeFull, Bound};
use std::path::{Path, PathBuf};
use std::sync::{RwLock, Mutex};
use std::ops;

use crate::Result;

pub trait MergeOperator: Fn(&[u8], Option<Vec<u8>>, &[u8]) -> Option<Vec<u8>> {}

impl<F> MergeOperator for F where F: Fn(&[u8], Option<Vec<u8>>, &[u8]) -> Option<Vec<u8>> {}

pub struct Column {
    merge_operator: Box<dyn MergeOperator>,
}

pub const DEFAULT_INDEX: &str = "_";

#[derive(Clone, Serialize, Deserialize)]
pub struct RawKey(pub String, pub Vec<u8>);

impl RawKey {
    fn default(key: Vec<u8>) -> Self {
        Self {
            0: DEFAULT_INDEX.to_string(),
            1: key,
        }
    }
}

#[derive(Default, Debug, Clone)]
pub struct KeyDirEntry {
    file_id: u32,
    data_entry_position: u32,
}

impl KeyDirEntry {
    pub fn new(file_id: u32, pos: u32) -> Self {
        KeyDirEntry {
            file_id,
            data_entry_position: pos,
        }
    }
}

type MultiMap<I, K, V> = BTreeMap<I, BTreeMap<K, V>>;

pub struct KeysDir {
    keys: RwLock<BTreeMap<Box<[u8]>, KeyDirEntry>>,
}

impl KeysDir {
    pub fn insert(&self, key: Vec<u8>, value: KeyDirEntry) -> Result<()> {
        let mut keys_dir_writer = self
            .keys
            .write()
            .map_err(|e| NotusError::RWLockPoisonError(format!("{}", e)))?;

        keys_dir_writer.insert(key.into_boxed_slice(), value);
        Ok(())
    }

    pub fn remove(&self, key: &[u8]) -> Result<()> {
        let mut keys_dir_writer = self
            .keys
            .write()
            .map_err(|e| NotusError::RWLockPoisonError(format!("{}", e)))?;
        keys_dir_writer.remove(key);
        Ok(())
    }

    pub fn clear(&self) -> Result<()> {
        let mut keys_dir_writer = self
            .keys
            .write()
            .map_err(|e| NotusError::RWLockPoisonError(format!("{}", e)))?;
        keys_dir_writer.clear();
        Ok(())
    }

    pub fn keys(&self) -> Vec<Vec<u8>> {
        let keys_dir_reader = match self.keys.read() {
            Ok(rdr) => rdr,
            Err(_) => {
                return vec![];
            }
        };
        keys_dir_reader.keys().map(|key| {
            key.to_vec()
        }).collect()
    }


    pub fn range<R>(&self, range: R) -> Vec<Vec<u8>> where R: RangeBounds<[u8]> {
        let keys_dir_reader = match self.keys.read() {
            Ok(rdr) => rdr,
            Err(_) => {
                return vec![];
            }
        };
        keys_dir_reader.range(range).map(|(k, _)| k.to_vec()).collect()
    }

    pub fn prefix(&self, prefix: &Vec<u8>) -> Vec<Vec<u8>> {
        let keys_dir_reader = match self.keys.read() {
            Ok(rdr) => rdr,
            Err(_) => {
                return vec![];
            }
        };
        keys_dir_reader
            .range(prefix.clone().into_boxed_slice()..)
            .take_while(|(k, _)| k.starts_with(&prefix))
            .map(|(k, _)| k.to_vec())
            .collect()
    }

    pub fn get(&self, key: &[u8]) -> Option<KeyDirEntry> {
        let keys_dir_reader = match self.keys.read() {
            Ok(rdr) => rdr,
            Err(_) => {
                return None;
            }
        };
        match keys_dir_reader.get(key) {
            None => {
                None
            }
            Some(entry) => {
               Some(entry.clone())
            }
        }
    }

    pub fn contains(&self, index: &str, key: &[u8]) -> Result<bool> {
        let keys_dir_reader = match self.keys.read() {
            Ok(rdr) => rdr,
            Err(error) => {
                return Err(NotusError::RWLockPoisonError(format!("{}", error)));
            }
        };
        Ok(keys_dir_reader.contains_key(key))
    }
}

impl KeysDir {
    pub fn new(file_pairs: &BTreeMap<u32, FilePair>) -> Result<Self> {
        let keys = RwLock::new(BTreeMap::new());
        let keys_dir = Self { keys };
        for (_, fp) in file_pairs {
            fp.fetch_hint_entries(&keys_dir)?;
        }
        Ok(keys_dir)
    }
}

pub struct DataStore {
    lock_file: File,
    dir: PathBuf,
    active_file: ActiveFilePair,
    keys_dir: KeysDir,
    files_dir: RwLock<BTreeMap<u32, FilePair>>,
    lock_memory: Mutex<()>,
}

impl DataStore {
    pub fn open<P: AsRef<Path>>(dir: P) -> Result<Self> {
        let lock_file = get_lock_file(dir.as_ref())?;
        let active_file_pair = create_new_file_pair(dir.as_ref())?;
        let files_dir = fetch_file_pairs(dir.as_ref())?;
        let keys_dir = KeysDir::new(&files_dir)?;
        let mut instance = Self {
            lock_file,
            dir: dir.as_ref().to_path_buf(),
            active_file: ActiveFilePair::from(active_file_pair)?,
            keys_dir,
            files_dir: RwLock::new(files_dir),
            lock_memory: Mutex::new(()),
        };
        instance.lock()?;
        Ok(instance)
    }

    fn lock(&mut self) -> Result<()> {
        self.lock_file
            .lock_exclusive()
            .map_err(|_| NotusError::LockFailed(String::from(self.dir.to_string_lossy())))?;
        Ok(())
    }

    pub fn put(&self, key : Vec<u8>, value: Vec<u8>) -> Result<()> {
        self.lock_memory.lock();
        let data_entry = DataEntry::new(key.clone(), value);
        let key_dir_entry = self.active_file.write(&data_entry)?;
        self.keys_dir.insert(key, key_dir_entry);
        Ok(())
    }

    pub fn get(&self, key: &Vec<u8>) -> Result<Option<Vec<u8>>> {
        self.lock_memory.lock();

        let key_dir_entry = match self.keys_dir.get(key) {
            None => {
                return Ok(None);
            }
            Some(value) => value,
        };

        let files_dir_rlock = self
            .files_dir
            .read()
            .map_err(|e| NotusError::RWLockPoisonError(format!("{}", e)))?;

        let fp = match files_dir_rlock.get(&key_dir_entry.file_id) {
            None => {
                return Ok(None);
            }
            Some(fp) => fp,
        };
        let data_entry = fp.read(key_dir_entry.data_entry_position as u64)?;
        Ok(Some(data_entry.value()))
    }

    pub fn delete(&self, key: &Vec<u8>) -> Result<()> {
        self.lock_memory.lock();
        self.active_file.remove(key.clone())?;
        self.keys_dir.remove(key);
        Ok(())
    }

    pub fn contains(&self, raw_key: &RawKey) -> Result<bool> {
        self.lock_memory.lock();
        let result = self.keys_dir.contains(&raw_key.0, &raw_key.1)?;
        Ok(result)
    }

    pub fn clear(&self) -> Result<()> {
        self.lock_memory.lock();
        for key in self.keys().iter() {
            self.active_file.remove(key.clone())?;
        }
        self.keys_dir.clear()?;
        Ok(())
    }

    pub fn keys(&self) -> Vec<Vec<u8>> {
        self.keys_dir.keys()
    }

    pub fn range<R>(&self,range: R) -> Vec<Vec<u8>> where R: RangeBounds<[u8]> {
        self.keys_dir.range(range)
    }

    pub fn prefix(&self,prefix: &Vec<u8>) -> Vec<Vec<u8>> {
        self.keys_dir.prefix(prefix)
    }

    pub fn merge(&self) -> Result<()> {
        let merged_file_pair = ActiveFilePair::from(create_new_file_pair(self.dir.as_path())?)?;
        let mut mark_for_removal = Vec::new();

        let files_dir_rlock = self
            .files_dir
            .read()
            .map_err(|e| NotusError::RWLockPoisonError(format!("{}", e)))?;

        for (_, fp) in files_dir_rlock.iter() {
            if fp.file_id() == self.active_file.file_id() {
                continue;
            }
            let hints = fp.get_hints()?;
            for hint in hints {
                let key = hint.key();
                if let Some(keys_dir_entry) = self.keys_dir.get(&key) {
                    if keys_dir_entry.file_id == fp.file_id() {
                        let data_entry = fp.read(hint.data_entry_position())?;
                        let key_entry = merged_file_pair.write(&data_entry)?;
                        self.keys_dir.insert(key, key_entry);
                    }
                }
            }
            mark_for_removal.push(fp.data_file_path());
            mark_for_removal.push(fp.hint_file_path());
        }

        fs_extra::remove_items(&mark_for_removal);
        Ok(())
    }

    pub fn flush(&self) -> Result<()> {
        Ok(())
    }
}

impl Drop for DataStore {
    fn drop(&mut self) {
        self.flush();
        self.lock_file.unlock().unwrap();
    }
}

#[cfg(test)]
mod tests {
    use crate::datastore::{DataStore, RawKey, DEFAULT_INDEX};
    use serial_test::serial;

    #[test]
    #[serial]
    fn test_data_store() {
        let mut ds = DataStore::open("./testdir/_test_data_store").unwrap();
        ds.put(vec![1, 2, 3], vec![4, 5, 6])
            .unwrap();
        println!("{:#?}", ds.get(&vec![1, 2, 3]).unwrap());
        clean_up()
    }

    #[test]
    #[serial]
    fn test_data_reopens() {
        clean_up();
        {
            let mut ds = DataStore::open("./testdir/_test_data_store").unwrap();
            ds.put(vec![1, 2, 3], vec![4, 5, 6])
                .unwrap();
            ds.put(vec![3, 1, 2], vec![12, 32, 1])
                .unwrap();
            ds.put(vec![3, 1, 4], vec![121, 200, 187])
                .unwrap();
            ds.put(vec![1, 2, 3], vec![3, 3, 3])
                .unwrap();
            println!("{:#?}", ds.keys());
        }

        {
            let mut ds = DataStore::open("./testdir/_test_data_store").unwrap();
            ds.put(vec![1, 2, 3], vec![9, 9, 6])
                .unwrap();
            ds.delete(&vec![3, 1, 2]).unwrap();
            println!("{:?}", ds.get(&vec![1, 2, 3]));
            println!("{:#?}", ds.keys());
        }

        {
            let mut ds = DataStore::open("./testdir/_test_data_store").unwrap();
            ds.delete(&vec![1, 2, 3]).unwrap();
            println!("{:#?}", ds.keys());
        }

        clean_up()
    }

    #[test]
    #[serial]
    fn test_data_merge_store() {
        clean_up();
        {
            let mut ds = DataStore::open("./testdir/_test_data_merge_store").unwrap();
            ds.put(vec![1, 2, 3], vec![4, 5, 6])
                .unwrap();
            ds.put(vec![3, 1, 2], vec![12, 32, 1])
                .unwrap();
            ds.put(vec![3, 1, 4], vec![121, 200, 187])
                .unwrap();
            ds.put(vec![1, 2, 3], vec![3, 3, 3])
                .unwrap();
        }

        {
            let mut ds = DataStore::open("./testdir/_test_data_merge_store").unwrap();
            ds.put(vec![1, 2, 3], vec![4, 4, 4])
                .unwrap();
            ds.put(vec![3, 1, 2], vec![12, 32, 1])
                .unwrap();
            ds.put(vec![3, 1, 4], vec![12, 54, 0])
                .unwrap();
            ds.put(vec![8, 27, 34], vec![3, 3, 3])
                .unwrap();
        }

        let mut keys_before_merge = vec![];
        let mut keys_after_merge = vec![];

        {
            let mut ds = DataStore::open("./testdir/_test_data_merge_store").unwrap();
            ds.put(vec![1, 2, 3], vec![9, 9, 6])
                .unwrap();
            ds.delete(&vec![3, 1, 2]).unwrap();
            keys_before_merge = ds.keys();
        }

        {
            let mut ds = DataStore::open("./testdir/_test_data_merge_store").unwrap();
            ds.merge();
            keys_after_merge = ds.keys();
        }

        assert_eq!(keys_before_merge, keys_after_merge);
        clean_up();
    }

    #[test]
    #[serial]
    fn test_reopen_without_closing_error() {
        let mut ds = DataStore::open("./testdir/_test_data_merge_store").unwrap();
        ds.put(vec![1, 2, 3], vec![4, 5, 6])
            .unwrap();
        ds.put(vec![3, 1, 2], vec![12, 32, 1])
            .unwrap();
        ds.put(vec![3, 1, 4], vec![121, 200, 187])
            .unwrap();
        ds.put(vec![1, 2, 3], vec![3, 3, 3])
            .unwrap();
        println!("{:#?}", ds.keys());

        let mut open_result = DataStore::open("./testdir/_test_data_merge_store");
        assert!(open_result.is_err());
    }

    fn clean_up() {
        fs_extra::dir::remove("./testdir");
    }
}
