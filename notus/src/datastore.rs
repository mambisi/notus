use std::collections::{BTreeMap, BTreeSet};
use crate::file_ops::{FilePair, fetch_file_pairs, create_new_file_pair, get_lock_file, ActiveFilePair};
use std::path::{Path, PathBuf};
use crate::schema::DataEntry;
use anyhow::bail;
use std::fs::File;
use fs2::FileExt;
use anyhow::Result;
use crate::errors::NotusError;
use std::sync::{RwLock, Arc, PoisonError, RwLockReadGuard};
use std::alloc::Global;
use std::ops::{Range, RangeFrom};
use dashmap::DashMap;
use std::collections::hash_map::RandomState;
use dashmap::mapref::one::Ref;

#[derive(Default, Debug, Clone)]
pub struct KeyDirEntry {
    file_id: String,
    key_size: u64,
    value_size: u64,
    data_entry_position: u64,
}

impl KeyDirEntry {
    pub fn new(file_id: String, key_size: u64, value_size: u64, pos: u64) -> Self {
        KeyDirEntry {
            file_id,
            key_size,
            value_size,
            data_entry_position: pos,
        }
    }
}

#[derive(Debug, Clone)]
enum KeyDirValueEntry {
    Persisted(KeyDirEntry),
    Buffer,
}

pub struct KeysDir {
    keys: RwLock<BTreeMap<Vec<u8>, KeyDirValueEntry>>
}

impl KeysDir {
    pub fn insert(&self, key: Vec<u8>, value: KeyDirEntry) -> Result<()> {
        let mut keys_dir_writer = self.keys.write().map_err(|e| {
            NotusError::RWLockPoisonError(format!("{}", e))
        })?;
        keys_dir_writer.insert(key, KeyDirValueEntry::Persisted(value));
        Ok(())
    }

    pub fn buffer_insert(&self, key: Vec<u8>) -> Result<()> {
        let mut keys_dir_writer = self.keys.write().map_err(|e| {
            NotusError::RWLockPoisonError(format!("{}", e))
        })?;
        keys_dir_writer.insert(key, KeyDirValueEntry::Buffer);
        Ok(())
    }

    pub fn remove(&self, key: &[u8]) -> Result<()> {
        let mut keys_dir_writer = self.keys.write().map_err(|e| {
            NotusError::RWLockPoisonError(format!("{}", e))
        })?;
        keys_dir_writer.remove(key);
        Ok(())
    }

    pub fn clear(&self) -> Result<()> {
        let mut keys_dir_writer = self.keys.write().map_err(|e| {
            NotusError::RWLockPoisonError(format!("{}", e))
        })?;
        keys_dir_writer.clear();
        Ok(())
    }

    pub fn keys(&self) -> Vec<Vec<u8>> {
        let keys_dir_reader = match self.keys.read() {
            Ok(rdr) => {
                rdr
            }
            Err(_) => {
                return vec![];
            }
        };
        keys_dir_reader.iter().map(|(k, _)| {
            k.clone()
        }).collect()
    }

    pub fn range(&self, range: RangeFrom<Vec<u8>>) -> Vec<Vec<u8>> {
        let keys_dir_reader = match self.keys.read() {
            Ok(rdr) => {
                rdr
            }
            Err(_) => {
                return vec![];
            }
        };
        keys_dir_reader.range(range).map(|(k, _)| {
            k.clone()
        }).collect()
    }

    pub fn prefix(&self, prefix: &Vec<u8>) -> Vec<Vec<u8>> {
        let keys_dir_reader = match self.keys.read() {
            Ok(rdr) => {
                rdr
            }
            Err(_) => {
                return vec![];
            }
        };
        keys_dir_reader.range(prefix.clone()..).take_while(|(k, _)| k.starts_with(prefix)).map(|(k, _)| {
            k.clone()
        }).collect()
    }

    pub fn get(&self, key: &[u8]) -> Option<KeyDirValueEntry> {
        let keys_dir_reader = match self.keys.read() {
            Ok(rdr) => {
                rdr
            }
            Err(_) => {
                return None;
            }
        };
        match keys_dir_reader.get(key) {
            None => {
                None
            }
            Some(s) => {
                Some(s.clone())
            }
        }
    }
}

impl KeysDir {
    pub fn new(file_pairs: &BTreeMap<String, FilePair>) -> Result<Self> {
        let keys = RwLock::new(BTreeMap::new());
        let mut keys_dir = Self {
            keys
        };
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
    keys_dir: Arc<KeysDir>,
    files_dir: Arc<RwLock<BTreeMap<String, FilePair>>>,
    buffer: DashMap<Vec<u8>, Vec<u8>>,
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
            keys_dir: Arc::new(keys_dir),
            files_dir: Arc::new(RwLock::new(files_dir)),
            buffer: Default::default(),
        };
        instance.lock()?;
        Ok(instance)
    }

    fn lock(&mut self) -> Result<()> {
        self.lock_file.try_lock_exclusive().map_err(|_| {
            NotusError::LockFailed(String::from(self.dir.to_string_lossy()))
        })?;
        Ok(())
    }

    pub fn put(&self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        self.buffer.insert(key.clone(), value);
        self.keys_dir.buffer_insert(key);
        Ok(())
    }

    fn persist(&self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        self.buffer.remove(&key);
        let data_entry = DataEntry::new(key.clone(), value);
        let key_dir_entry = self.active_file.write(&data_entry)?;
        self.keys_dir.insert(key, key_dir_entry);
        Ok(())
    }

    pub fn get(&self, key: &Vec<u8>) -> Result<Option<Vec<u8>>> {
        let key_dir_entry = match self.keys_dir.get(key) {
            None => {
                return Ok(None);
            }
            Some(entry) => {
                match entry {
                    KeyDirValueEntry::Persisted(entry) => {
                        entry
                    }
                    KeyDirValueEntry::Buffer => {
                        return match self.buffer.get(key) {
                            None => {
                                Ok(None)
                            }
                            Some(value) => {
                                Ok(Some(value.value().clone()))
                            }
                        };
                    }
                }
            }
        };

        let files_dir = self.files_dir.clone();
        let files_dir_rlock = files_dir.read().map_err(|e| {
            NotusError::RWLockPoisonError(format!("{}", e))
        })?;
        ;

        let fp = match files_dir_rlock.get(&key_dir_entry.file_id) {
            None => {
                return Ok(None);
            }
            Some(fp) => {
                fp
            }
        };
        let data_entry = fp.read(key_dir_entry.data_entry_position)?;
        Ok(Some(data_entry.value()))
    }

    pub fn delete(&self, key: &Vec<u8>) -> Result<()> {
        self.active_file.remove(key.clone())?;
        self.keys_dir.remove(key);
        Ok(())
    }

    pub fn clear(&self) -> Result<()> {
        for key in self.keys().iter() {
            self.active_file.remove(key.clone())?;
        }
        self.keys_dir.clear()?;
        Ok(())
    }

    pub fn keys(&self) -> Vec<Vec<u8>> {
        self.keys_dir.keys()
    }

    pub fn range(&self, range: RangeFrom<Vec<u8>>) -> Vec<Vec<u8>> {
        self.keys_dir.range(range)
    }

    pub fn prefix(&self, prefix: &Vec<u8>) -> Vec<Vec<u8>> {
        self.keys_dir.prefix(prefix)
    }

    pub fn merge(&self) -> anyhow::Result<()> {
        let merged_file_pair = ActiveFilePair::from(create_new_file_pair(self.dir.as_path())?)?;
        let mut mark_for_removal = Vec::new();

        let files_dir = self.files_dir.clone();
        let files_dir_rlock = files_dir.read().map_err(|e| {
            NotusError::RWLockPoisonError(format!("{}", e))
        })?;
        ;

        for (_, fp) in files_dir_rlock.iter() {
            if fp.file_id() == self.active_file.file_id() {
                continue;
            }
            let hints = fp.get_hints()?;
            for hint in hints {
                if let Some(keys_dir_entry) = self.keys_dir.get(&hint.key()) {
                    if let KeyDirValueEntry::Persisted(keys_dir_entry) = keys_dir_entry {
                        if keys_dir_entry.file_id == fp.file_id() {
                            let data_entry = fp.read(hint.data_entry_position())?;
                            let key_entry = merged_file_pair.write(&data_entry)?;
                            self.keys_dir.insert(hint.key(), key_entry);
                        }
                    }
                }
            }
            mark_for_removal.push(fp.data_file_path());
            mark_for_removal.push(fp.hint_file_path());
        }

        fs_extra::remove_items(&mark_for_removal);
        Ok(())
    }

    pub fn flush(&self, ) -> Result<()>{
        //Todo remove
        //let buffer :
        for entry in self.buffer.iter(){
            self.persist(entry.key().clone(),entry.value().to_vec())?;
        }
        //self.buffer.clear();
        Ok(())
    }
}

impl Drop for DataStore {
    fn drop(&mut self) {
        self.flush().unwrap();
        self.lock_file.unlock().unwrap();
    }
}

#[cfg(test)]
mod tests {
    use crate::datastore::DataStore;
    use serial_test::serial;

    #[test]
    #[serial]
    fn test_data_store() {
        let mut ds = DataStore::open("./testdir/_test_data_store").unwrap();
        ds.put(vec![1, 2, 3], vec![4, 5, 6]).unwrap();
        println!("{:#?}", ds.get(&vec![1, 2, 3]).unwrap());
        clean_up()
    }

    #[test]
    #[serial]
    fn test_data_reopens() {
        clean_up();
        {
            let mut ds = DataStore::open("./testdir/_test_data_store").unwrap();
            ds.put(vec![1, 2, 3], vec![4, 5, 6]).unwrap();
            ds.put(vec![3, 1, 2], vec![12, 32, 1]).unwrap();
            ds.put(vec![3, 1, 4], vec![121, 200, 187]).unwrap();
            ds.put(vec![1, 2, 3], vec![3, 3, 3]).unwrap();
            println!("{:#?}", ds.keys());
        }

        {
            let mut ds = DataStore::open("./testdir/_test_data_store").unwrap();
            ds.put(vec![1, 2, 3], vec![9, 9, 6]).unwrap();
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
            ds.put(vec![1, 2, 3], vec![4, 5, 6]).unwrap();
            ds.put(vec![3, 1, 2], vec![12, 32, 1]).unwrap();
            ds.put(vec![3, 1, 4], vec![121, 200, 187]).unwrap();
            ds.put(vec![1, 2, 3], vec![3, 3, 3]).unwrap();
        }

        {
            let mut ds = DataStore::open("./testdir/_test_data_merge_store").unwrap();
            ds.put(vec![1, 2, 3], vec![4, 4, 4]).unwrap();
            ds.put(vec![3, 1, 2], vec![12, 32, 1]).unwrap();
            ds.put(vec![3, 1, 4], vec![12, 54, 0]).unwrap();
            ds.put(vec![8, 27, 34], vec![3, 3, 3]).unwrap();
        }

        let mut keys_before_merge = vec![];
        let mut keys_after_merge = vec![];

        {
            let mut ds = DataStore::open("./testdir/_test_data_merge_store").unwrap();
            ds.put(vec![1, 2, 3], vec![9, 9, 6]).unwrap();
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
        ds.put(vec![1, 2, 3], vec![4, 5, 6]).unwrap();
        ds.put(vec![3, 1, 2], vec![12, 32, 1]).unwrap();
        ds.put(vec![3, 1, 4], vec![121, 200, 187]).unwrap();
        ds.put(vec![1, 2, 3], vec![3, 3, 3]).unwrap();
        println!("{:#?}", ds.keys());

        let mut open_result = DataStore::open("./testdir/_test_data_merge_store");
        assert!(open_result.is_err());
    }

    fn clean_up() {
        fs_extra::dir::remove("./testdir");
    }
}