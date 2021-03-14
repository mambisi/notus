use std::path::{Path, PathBuf};
use chrono::Utc;
use std::fs::{OpenOptions, File};
use std::io::{Result, BufReader, Seek, SeekFrom, BufWriter, Write};
use fs_extra::dir::DirOptions;
use std::collections::BTreeMap;
use std::ffi::OsStr;
use anyhow::{bail, Error};
use crate::schema::{HintEntry, DataEntry, Decoder, Encoder};
use crate::datastore::{KeyDirEntry, KeysDir};

const DATA_FILE_EXTENSION: &str = "data";
const HINT_FILE_EXTENSION: &str = "hint";

#[derive(Debug)]
pub struct FilePair {
    file_id: String,
    data_file_path: PathBuf,
    hint_file_path: PathBuf,
}

impl FilePair {
    fn new(file_id: &str) -> Self {
        Self {
            file_id: file_id.to_string(),
            data_file_path: Default::default(),
            hint_file_path: Default::default(),
        }
    }
}

impl FilePair {
    pub fn read_data_entry(&self, entry_position: u64) -> anyhow::Result<DataEntry> {
        let data_file = File::open(&self.data_file_path.as_path())?;
        let mut reader = BufReader::new(data_file);
        reader.seek(SeekFrom::Start(entry_position))?;
        let data_entry = DataEntry::decode(&mut reader)?;
        Ok(data_entry)
    }

    pub fn write_data_entry(&self, entry: &DataEntry) -> anyhow::Result<KeyDirEntry> {
        //Appends entry to data file
        let data_file = OpenOptions::new().write(true).open(&self.data_file_path.as_path())?;
        let mut dfw = BufWriter::new(data_file);
        let data_entry_position = dfw.seek(SeekFrom::End(0))?;
        dfw.write_all(&entry.encode())?;
        dfw.flush();
        //Append hint to hint file
        let hint_entry = HintEntry::from(entry, data_entry_position);
        let hint_file = OpenOptions::new().write(true).open(&self.hint_file_path.as_path())?;
        let mut hfw = BufWriter::new(hint_file);
        hfw.seek(SeekFrom::End(0))?;
        hfw.write_all(&hint_entry.encode())?;
        hfw.flush();


        Ok(KeyDirEntry::new(self.file_id.to_string(),
                            hint_entry.key_size(),
                            hint_entry.value_size(),
                            data_entry_position))
    }

    pub fn fetch_hint_entries(&self, keys_dir : &mut KeysDir) -> anyhow::Result<()>{
        let hint_file = File::open(&self.hint_file_path.as_path())?;
        let mut rdr = BufReader::new(hint_file);
        loop {
            let hint_entry = match HintEntry::decode(&mut rdr) {
                Ok(hint) => {
                    hint
                }
                Err(_) => {
                    //Todo Check for EOF
                    break
                }
            };
            if hint_entry.is_deleted() {
             keys_dir.remove(&hint_entry.key())
            }
            else {
                let key_dir_entry = KeyDirEntry::new(self.file_id.to_string(),
                                                     hint_entry.key_size(),
                                                     hint_entry.value_size(),
                                                     hint_entry.data_entry_position());
                keys_dir.insert(hint_entry.key(),key_dir_entry);
            }
        }
        Ok(())
    }
}

pub fn create_new_file_pair<P: AsRef<Path>>(dir: P) -> anyhow::Result<FilePair> {
    fs_extra::dir::create_all(dir.as_ref(), false)?;
    let file_name = Utc::now().timestamp_nanos().to_string();
    let mut data_file_path = PathBuf::new();
    data_file_path.push(dir.as_ref());
    data_file_path.push(format!("{}.{}", file_name, DATA_FILE_EXTENSION));
    data_file_path.set_extension(DATA_FILE_EXTENSION);

    let mut hint_file_path = PathBuf::new();
    hint_file_path.push(dir.as_ref());
    hint_file_path.push(format!("{}.{}", file_name, HINT_FILE_EXTENSION));
    hint_file_path.set_extension(HINT_FILE_EXTENSION);

    OpenOptions::new().create_new(true).write(true).open(data_file_path.as_path())?;
    OpenOptions::new().create_new(true).write(true).open(hint_file_path.as_path())?;

    Ok(FilePair {
        data_file_path,
        hint_file_path,
        file_id: file_name,
    })
}

pub fn fetch_file_pairs<P: AsRef<Path>>(dir: P) -> anyhow::Result<BTreeMap<String, FilePair>> {
    let mut file_pairs = BTreeMap::new();
    let mut option = DirOptions::new();
    option.depth = 1;

    let dir_content = fs_extra::dir::get_dir_content2(dir, &option)?;
    for file in dir_content.files.iter() {
        let file_path = Path::new(file);
        let file_extension = String::from(file_path.extension().unwrap().to_string_lossy());
        match file_extension.as_str() {
            DATA_FILE_EXTENSION => {}
            HINT_FILE_EXTENSION => {}
            _ => {
                continue;
            }
        };

        let file_name = String::from(file_path.file_name().unwrap().to_string_lossy());
        let file_name = &file_name[..file_name.len() - 5];
        let file_pair = file_pairs.entry(file_name.to_owned()).or_insert(FilePair::new(file_name));
        match file_extension.as_str() {
            DATA_FILE_EXTENSION => {
                file_pair.data_file_path = file_path.to_path_buf()
            }
            HINT_FILE_EXTENSION => {
                file_pair.hint_file_path = file_path.to_path_buf()
            }
            _ => {}
        };
    }
    Ok(file_pairs)
}


#[cfg(test)]
mod tests {
    use crate::file_ops::{create_new_file_pair, fetch_file_pairs};

    #[test]
    fn test_create_file_pairs() {
        create_new_file_pair("./testdir").unwrap();
        create_new_file_pair("./testdir").unwrap();
        create_new_file_pair("./testdir").unwrap();

        let b = fetch_file_pairs("./testdir").unwrap();
        println!("{:#?}", b);
        clean_up()
    }

    fn clean_up() {
        fs_extra::dir::remove("./testdir");
    }
}

