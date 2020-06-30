use crate::thread_pool::ThreadPool;
use crate::{KvsEngine, KvsRequest, KvsResponse, Result};
use serde::Deserialize;
use std::io::Write;
use std::net::{SocketAddr, TcpListener, TcpStream};

///KvsServer
pub struct KvsServer<E: KvsEngine, P: ThreadPool> {
    engine: E,
    pool: P,
}
use log::error;

impl<E: KvsEngine, P: ThreadPool> KvsServer<E, P> {
    /// Create a `KvsServer` with a given storage engine.
    pub fn new(engine: E, pool: P) -> Self {
        KvsServer { engine, pool }
    }
    /// accept connections and process them
    pub fn run(&self, addr: SocketAddr) -> Result<()> {
        let listener = TcpListener::bind(addr)?;
        for stream in listener.incoming() {
            let engine = self.engine.clone();
            let stream = stream.unwrap();
            self.pool.spawn(move || {
                if let Err(e) = serve(engine, stream) {
                    error!("Error on serving client: {}", e);
                }
            })
        }
        Ok(())
    }
}

fn serve<E: KvsEngine>(engine: E, mut stream: TcpStream) -> Result<()> {
    let mut de = serde_json::Deserializer::from_reader(&mut stream);
    let request: KvsRequest = KvsRequest::deserialize(&mut de)?;
    println!("{:?}", request);

    let response: KvsResponse;
    match request {
        KvsRequest::Get { key } => match engine.get(key.to_owned())? {
            Some(value) => {
                response = KvsResponse::Ok(Some(value));
            }
            None => {
                response = KvsResponse::Ok(Some("Key not found".to_owned()));
            }
        },
        KvsRequest::Set { key, value } => {
            if let Err(_) = engine.set(key.to_owned(), value.to_owned()) {
                response = KvsResponse::Err("Set error".to_owned());
            } else {
                response = KvsResponse::Ok(None);
            }
        }
        KvsRequest::Remove { key } => {
            if let Err(_) = engine.remove(key.to_owned()) {
                response = KvsResponse::Err("Key not found".to_owned());
            } else {
                response = KvsResponse::Ok(None);
            }
        }
    }
    serde_json::to_writer(&mut stream, &response)?;
    stream.flush()?;
    Ok(())
}
