#![deny(missing_docs)]
//! A key-value store.

pub use error::Result;
pub use kv::KvStore;

mod error;
mod kv;
