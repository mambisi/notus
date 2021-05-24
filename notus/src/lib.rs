#![feature(allocator_api)]

use crate::errors::NotusError;

pub mod datastore;
pub mod errors;
pub mod file_ops;
pub mod nutos;
pub mod schema;

pub type Result<T> = std::result::Result<T, NotusError>;

#[cfg(test)]
mod tests;
mod memtable;
