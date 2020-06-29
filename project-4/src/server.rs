use crate::{KvsEngine, KvsRequest, KvsResponse, Result};
use serde::Deserialize;
use std::io::Write;
use std::net::{SocketAddr, TcpListener};

///KvsServer
pub struct KvsServer<E: KvsEngine> {
    engine: E,
}

impl<E: KvsEngine> KvsServer<E> {
    /// Create a `KvsServer` with a given storage engine.
    pub fn new(engine: E) -> Self {
        KvsServer { engine }
    }
    /// accept connections and process them
    pub fn run(&self, addr: SocketAddr) -> Result<()> {
        let listener = TcpListener::bind(addr)?;
        for stream in listener.incoming() {
            let mut stream = stream.unwrap();
            let mut de = serde_json::Deserializer::from_reader(&mut stream);
            let request: KvsRequest = KvsRequest::deserialize(&mut de)?;
            println!("{:?}", request);

            let response: KvsResponse;
            match request {
                KvsRequest::Get { key } => match self.engine.get(key.to_owned())? {
                    Some(value) => {
                        response = KvsResponse::Ok(Some(value));
                    }
                    None => {
                        response = KvsResponse::Ok(Some("Key not found".to_owned()));
                    }
                },
                KvsRequest::Set { key, value } => {
                    if let Err(_) = self.engine.set(key.to_owned(), value.to_owned()) {
                        response = KvsResponse::Err("Set error".to_owned());
                    } else {
                        response = KvsResponse::Ok(None);
                    }
                }
                KvsRequest::Remove { key } => {
                    if let Err(_) = self.engine.remove(key.to_owned()) {
                        response = KvsResponse::Err("Key not found".to_owned());
                    } else {
                        response = KvsResponse::Ok(None);
                    }
                }
            }
            serde_json::to_writer(&mut stream, &response)?;
            stream.flush()?;
        }
        Ok(())
    }
}
