#![deny(missing_docs)]
#![feature(seek_convenience)]
//! A key-value store.

pub use error::Result;
pub use kv::KvStore;

mod error;
mod kv;
