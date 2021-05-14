use std::io;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum NotusError {
    #[error("io error")]
    IOError(#[from] io::Error),
    #[error("fs extra error")]
    FSExtraError(#[from] fs_extra::error::Error),
    #[error("bincode error")]
    BincodeError(#[from] bincode::Error),
    #[error("Data Corrupt")]
    CorruptValue,
    #[error("Merge failed")]
    MergeError,
    #[error("failed to lock nutos director {0}")]
    LockFailed(String),
    #[error("RW lock poison {0}")]
    RWLockPoisonError(String),
    #[error("unknown data store error")]
    Unknown,
}
