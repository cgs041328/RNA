use kvs::*;
use log::info;
use simplelog::{Config, LevelFilter, TerminalMode};
use std::env;
use std::io::prelude::*;
use std::net::{SocketAddr, TcpListener, TcpStream};
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
        let response = "ok";
        stream.write(response.as_bytes())?;
        stream.flush()?;
    }
    Ok(())
}
