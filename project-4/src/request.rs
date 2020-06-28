use serde::{Deserialize, Serialize};

///KvsRequest
#[derive(Serialize, Deserialize, Debug)]
pub enum KvsRequest {
    ///Get command
    Get {
        ///key
        key: String,
    },
    ///Set command
    Set {
        ///key
        key: String,
        ///value
        value: String,
    },
    /// Remove command
    Remove {
        ///key
        key: String,
    },
}
