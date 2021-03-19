use std::collections::BTreeMap;
use crate::file_ops::{FilePair, fetch_file_pairs, create_new_file_pair, get_lock_file, ActiveFilePair};
use std::path::{Path, PathBuf};
use crate::schema::DataEntry;
use anyhow::bail;
use std::fs::File;
use fs2::FileExt;

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

pub struct KeysDir {
    keys: BTreeMap<Vec<u8>, KeyDirEntry>
}

impl KeysDir {
    pub fn insert(&mut self, key: Vec<u8>, value: KeyDirEntry) {
        self.keys.insert(key, value);
    }

    pub fn remove(&mut self, key: &[u8]) {
        self.keys.remove(key);
    }

    pub fn keys(&self) -> Vec<Vec<u8>> {
        self.keys.iter().map(|(k, v)| {
            k.clone()
        }).collect()
    }

    pub fn get(&self, key: &[u8]) -> Option<KeyDirEntry> {
        match self.keys.get(key) {
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
    pub fn new(file_pairs: &BTreeMap<String, FilePair>) -> anyhow::Result<Self> {
        let mut keys_dir = Self {
            keys: Default::default()
        };
        for (_, fp) in file_pairs {
            fp.fetch_hint_entries(&mut keys_dir)?;
        }
        Ok(keys_dir)
    }
}



pub struct DataStore {
    lock_file: File,
    dir: PathBuf,
    active_file: ActiveFilePair,
    keys_dir: KeysDir,
    files_dir: BTreeMap<String, FilePair>,

}

impl DataStore {
    pub fn open<P: AsRef<Path>>(dir: P) -> anyhow::Result<Self> {
        let lock_file = get_lock_file(dir.as_ref())?;
        let active_file_pair = create_new_file_pair(dir.as_ref())?;
        let files_dir = fetch_file_pairs(dir.as_ref())?;
        let keys_dir = KeysDir::new(&files_dir)?;
        let mut instance = Self {
            lock_file,
            dir: dir.as_ref().to_path_buf(),
            active_file : ActiveFilePair::from(active_file_pair)?,
            keys_dir,
            files_dir,
        };
        instance.lock()?;
        Ok(instance)
    }

    fn lock(&mut self) -> anyhow::Result<()> {
        self.lock_file.try_lock_exclusive()?;
        Ok(())
    }

    pub fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> anyhow::Result<()> {
        let data_entry = DataEntry::new(key.clone(), value);
        let key_dir_entry = self.active_file.write(&data_entry)?;
        self.keys_dir.insert(key, key_dir_entry);
        Ok(())
    }

    pub fn get(&self, key: Vec<u8>) -> anyhow::Result<Option<Vec<u8>>> {
        let key_dir_entry = match self.keys_dir.get(&key) {
            None => {
                return Ok(None);
            }
            Some(entry) => {
                entry
            }
        };
        let fp = match self.files_dir.get(&key_dir_entry.file_id) {
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

    pub fn delete(&mut self, key: Vec<u8>) -> anyhow::Result<()> {
        self.active_file.remove(key.clone())?;
        self.keys_dir.remove(&key);
        Ok(())
    }

    pub fn keys(&self) -> Vec<Vec<u8>> {
        self.keys_dir.keys()
    }

    pub fn merge(&mut self) -> anyhow::Result<()> {
        let merged_file_pair = ActiveFilePair::from(create_new_file_pair(self.dir.as_path())?)?;
        let mut mark_for_removal = Vec::new();
        for (_, fp) in self.files_dir.iter() {
            if fp.file_id() == self.active_file.file_id() {
                continue;
            }
            let hints = fp.get_hints()?;
            for hint in hints {
                if let Some(keys_dir_entry) = self.keys_dir.get(&hint.key()) {
                    if keys_dir_entry.file_id == fp.file_id() {
                        let data_entry = fp.read(hint.data_entry_position())?;
                        let key_entry = merged_file_pair.write(&data_entry)?;
                        self.keys_dir.insert(hint.key(), key_entry);
                    }
                }
            }
            mark_for_removal.push(fp.data_file_path());
            mark_for_removal.push(fp.hint_file_path());
        }

        fs_extra::remove_items(&mark_for_removal);
        Ok(())
    }
}

impl Drop for DataStore {
    fn drop(&mut self) {
        self.lock_file.unlock().unwrap();
    }
}

#[cfg(test)]
mod tests {
    use crate::datastore::DataStore;

    #[test]
    fn test_data_store() {
        let mut ds = DataStore::open("./testdir/_test_data_store").unwrap();
        ds.put(vec![1, 2, 3], vec![4, 5, 6]).unwrap();
        println!("{:#?}", ds.get(vec![1, 2, 3]).unwrap());
        clean_up()
    }

    #[test]
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
            ds.delete(vec![3, 1, 2]).unwrap();
            println!("{:?}", ds.get(vec![1, 2, 3]));
            println!("{:#?}", ds.keys());
        }

        {
            let mut ds = DataStore::open("./testdir/_test_data_store").unwrap();
            ds.delete(vec![1, 2, 3]).unwrap();
            println!("{:#?}", ds.keys());
        }

        clean_up()
    }

    #[test]
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
            ds.delete(vec![3, 1, 2]).unwrap();
            keys_before_merge = ds.keys();
        }

        {
            let mut ds = DataStore::open("./testdir/_test_data_merge_store").unwrap();
            ds.merge();
            keys_after_merge = ds.keys();
        }

        assert_eq!(keys_before_merge,keys_after_merge);
        clean_up();
    }

    #[test]
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