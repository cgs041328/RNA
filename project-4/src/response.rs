use serde::{Deserialize, Serialize};

///KvsResponse
#[derive(Serialize, Deserialize, Debug)]
pub enum KvsResponse {
    ///Ok response
    Ok(Option<String>),
    ///Err response
    Err(String),
}
