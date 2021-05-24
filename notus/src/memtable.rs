use crate::schema::DataEntry;
use chrono::{DateTime, Utc};

#[derive(Debug,Clone)]
pub struct MemTableEntry {
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
    pub tombstone: bool,
    pub timestamp: i64,
}

#[derive(Debug)]
pub struct MemTable {
    entries: Vec<MemTableEntry>,
    size: usize,
}

impl MemTable {
    pub fn new() -> Self {
        Self {
            entries: vec![],
            size: 0,
        }
    }

    pub fn size(&self) -> usize {
        self.size
    }
}

impl MemTable {
    fn get_index(&self, key: &[u8]) -> Result<usize, usize> {
        self.entries.binary_search_by_key(&key, |entry| entry.key.as_slice())
    }
}

impl MemTable {
    pub fn insert(&mut self, key: &[u8], value: &[u8]) {
        let entry = MemTableEntry{
            key: key.to_vec(),
            value: Some(value.to_vec()),
            tombstone: false,
            timestamp: Utc::now().timestamp(),
        };

        match self.get_index(key) {
            Ok(index) => {
                if let Some(old_value) = self.entries[index].value.as_ref() {
                    if value.len() < old_value.len() {
                        self.size -= old_value.len() - value.len();
                    } else {
                        self.size += value.len() - old_value.len();
                    }
                }
                self.entries[index] = entry;
            }
            Err(index) => {
                self.size += key.len() + value.len() + 8 + 1; // Increase the size of the MemTable by the Key size, Value size, Timestamp size (8 bytes), Tombstone size (1 byte).
                self.entries.insert(index, entry)
            }
        }
    }

    pub fn remove(&mut self, key: &[u8]) {

        let entry = MemTableEntry {
            key: key.to_vec(),
            value: None,
            timestamp: Utc::now().timestamp(),
            tombstone: true,
        };

        match self.get_index(key) {
            Ok(index) => {
                if let Some(value) = self.entries[index].value.as_ref() {
                    self.size -= value.len();
                }
                self.entries[index] = entry;
            }
            Err(index) => {
                self.size += key.len() + 8 + 1; // Increase the size of the MemTable by the Key size, Value size, Timestamp size (8 bytes), Tombstone size (1 byte).
                self.entries.insert(index, entry)
            }
        }
    }

    pub fn get(&self, key: &[u8]) -> Option<&MemTableEntry> {
        if let Ok(index) = self.get_index(key) {
            return Some(&self.entries[index]);
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use crate::memtable::MemTable;
    use std::convert::TryInto;

    #[test]
    fn test_memtable() {
        let mut memtable = MemTable::new();

        for i in 0..100_u32 {
            memtable.insert(&i.to_be_bytes().to_vec(), &i.to_be_bytes().to_vec())
        }

        for i in 0..100_u32 {
            let entry = memtable.get(&i.to_be_bytes().to_vec()).unwrap();
            let mut raw_value : [u8; 4] = [0;4];
            raw_value.copy_from_slice(entry.value.as_ref().unwrap());
            let j = u32::from_be_bytes(raw_value);
            assert_eq!(i,j)
        }
        println!("{}", memtable.size());

        for i in 0..50_u32 {
            memtable.remove(&i.to_be_bytes().to_vec())
        }

        println!("{}", memtable.size())
    }
}