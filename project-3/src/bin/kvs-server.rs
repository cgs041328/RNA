use kvs::*;
use log::info;
use serde::Deserialize;
use simplelog::{Config, LevelFilter, TerminalMode};
use std::env;
use std::io::prelude::*;
use std::net::{SocketAddr, TcpListener};
use structopt::StructOpt;

const DEFAULT_ENGINE: &str = "kvs";
const DEFAULT_ADDRESS: &str = "127.0.0.1:4000";

#[derive(StructOpt, Debug)]
#[structopt(name = "kvs-server", version = env!("CARGO_PKG_VERSION"), author = env!("CARGO_PKG_AUTHORS"), about = env!("CARGO_PKG_DESCRIPTION"))]
struct Opt {
    #[structopt(
        long,
        help = "Sets the listening address",
        value_name = "IP:PORT",
        default_value = DEFAULT_ADDRESS,
        parse(try_from_str)
    )]
    addr: SocketAddr,
    #[structopt(long, help = "Sets the storage engine", value_name = "ENGINE-NAME", default_value = DEFAULT_ENGINE, possible_values = &["kvs","sled"])]
    engine: String,
}

fn main() -> Result<()> {
    simplelog::TermLogger::init(LevelFilter::Info, Config::default(), TerminalMode::Stderr)
        .unwrap();
    let opt = Opt::from_args();

    let engine = opt.engine;
    info!("kvs-server version: {}", env!("CARGO_PKG_VERSION"));
    info!("Storage engine: {}", engine);
    info!("Listening on {}", opt.addr);

    let listener = TcpListener::bind(opt.addr)?;

    // accept connections and process them serially
    for stream in listener.incoming() {
        let mut stream = stream.unwrap();
        let mut de = serde_json::Deserializer::from_reader(&mut stream);
        let request: KvsRequest = KvsRequest::deserialize(&mut de)?;
        println!("{:?}", request);

        let response: KvsResponse;
        match request {
            KvsRequest::Get { key } => {
                let mut store = KvStore::open(env::current_dir()?)?;
                match store.get(key.to_owned())? {
                    Some(value) => {
                        response = KvsResponse::Ok(Some(value));
                    }
                    None => {
                        response = KvsResponse::Err("Key not found".to_owned());
                    }
                }
            }
            KvsRequest::Set { key, value } => {
                let mut store = KvStore::open(env::current_dir()?)?;
                if let Err(_) = store.set(key.to_owned(), value.to_owned()) {
                    response = KvsResponse::Err("Set error".to_owned());
                } else {
                    response = KvsResponse::Ok(None);
                }
            }
            KvsRequest::Remove { key } => {
                let mut store = KvStore::open(env::current_dir()?)?;
                if let Err(_) = store.remove(key.to_owned()) {
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
