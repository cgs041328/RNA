use clap::{App, Arg, SubCommand};
use env_logger;
use kvs::*;
use log::info;
use std::env;
use std::net::SocketAddr;
use std::process::exit;
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
    #[structopt(long, help = "Sets the storage engine", value_name = "ENGINE-NAME")]
    engine: Option<String>,
}

fn main() -> Result<()> {
    env_logger::init();
    let mut opt = Opt::from_args();

    let engine = opt.engine.unwrap_or(DEFAULT_ENGINE.to_string());
    info!("kvs-server version: {}", env!("CARGO_PKG_VERSION"));
    info!("Storage engine: {}", engine);
    info!("Listening on {}", opt.addr);

    Ok(())
}
