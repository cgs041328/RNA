use failure::Error;

///Result type for kvs
pub type Result<T> = std::result::Result<T, Error>;
