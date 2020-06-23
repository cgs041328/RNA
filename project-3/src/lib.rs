#![deny(missing_docs)]
#![feature(seek_convenience)]
//! A key-value store.

pub use engines::{KvStore, KvsEngine, SledEngine};
pub use error::Result;
pub use request::KvsRequest;
pub use response::KvsResponse;

mod engines;
mod error;
mod request;
mod response;
