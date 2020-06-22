#![deny(missing_docs)]
#![feature(seek_convenience)]
//! A key-value store.

pub use error::Result;
pub use kv::{KvStore, KvsEngine};
pub use request::KvsRequest;
pub use response::KvsResponse;

mod error;
mod kv;
mod request;
mod response;
