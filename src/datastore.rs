use std::collections::BTreeMap;
use crate::file_ops::{FilePair, fetch_file_pairs, create_new_file_pair};
use std::path::Path;
use crate::schema::DataEntry;

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

    pub fn get(&self, key: &[u8]) -> Option<KeyDirEntry>{
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
        let mut file_dir = Self {
            keys: Default::default()
        };
        for (_, fp) in file_pairs {
            fp.fetch_hint_entries(&mut file_dir)?;
        }
        Ok(file_dir)
    }
}

pub struct DataStore {
    active_file: FilePair,
    keys_dir: KeysDir,
    files_dir: BTreeMap<String, FilePair>,
}

impl DataStore {
    pub fn open<P: AsRef<Path>>(dir: P) -> anyhow::Result<Self> {
        let active_file = create_new_file_pair(dir.as_ref())?;
        let files_dir = fetch_file_pairs(dir.as_ref())?;
        let keys_dir = KeysDir::new(&files_dir)?;

        Ok(Self {
            active_file,
            keys_dir,
            files_dir,
        })
    }

    pub fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> anyhow::Result<()>{
        let data_entry = DataEntry::new(key.clone(),value);
        let key_dir_entry = self.active_file.write_data_entry(&data_entry)?;
        self.keys_dir.insert(key, key_dir_entry);
        Ok(())
    }

    pub fn get(&self, key: Vec<u8>) -> anyhow::Result<Option<Vec<u8>>> {
        let key_dir_entry = match self.keys_dir.get(&key) {
            None => {
                return Ok(None)
            }
            Some(entry) => {
                entry
            }
        };
        let fp = match self.files_dir.get(&key_dir_entry.file_id){
            None => {
                return Ok(None)
            }
            Some(fp) => {
                fp
            }
        };
        let data_entry = fp.read_data_entry(key_dir_entry.data_entry_position)?;
        Ok(Some(data_entry.value()))
    }

    pub fn delete(&mut self, key: Vec<u8>) {
        unimplemented!()
    }
}

#[cfg(test)]
mod tests {
    use crate::datastore::DataStore;

    #[test]
    fn test_data_store() {
        let mut ds = DataStore::open("./testdir/_test_data_store").unwrap();
        ds.put(vec![1,2,3], vec![4,5,6]).unwrap();
        println!("{:#?}", ds.get(vec![1,2,3]).unwrap());
        clean_up()
    }

    fn clean_up() {
        fs_extra::dir::remove("./testdir");
    }
}