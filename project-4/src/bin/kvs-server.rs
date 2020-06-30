use failure::format_err;
use kvs::thread_pool::ThreadPool;
use kvs::*;
use log::info;
use simplelog::{Config, LevelFilter, TerminalMode};
use std::net::SocketAddr;
use std::{
    env, fs,
    io::{Read, Write},
    path::Path,
};
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

    let curr_dir = env::current_dir()?;
    match engine.as_str() {
        "sled" => {
            current_engine_or(&curr_dir, "sled")?;
            let engine = SledEngine::open(&curr_dir)?;
            run_with_engine(engine, opt.addr)?;
        }
        "kvs" => {
            current_engine_or(&curr_dir, "kvs")?;
            let engine = KvStore::open(&curr_dir)?;
            run_with_engine(engine, opt.addr)?;
        }
        _ => unreachable!(),
    }

    Ok(())
}

fn run_with_engine<E: KvsEngine>(engine: E, addr: SocketAddr) -> Result<()> {
    let pool = thread_pool::NaiveThreadPool::new(1)?;
    let server = KvsServer::new(engine, pool);
    server.run(addr)
}

fn current_engine_or<'a>(path: &Path, engine: &'a str) -> Result<&'a str> {
    let engine_path = path.join("type");
    let mut engine_type_file = fs::OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(&engine_path)?;
    let mut engine_type = String::new();
    engine_type_file.read_to_string(&mut engine_type)?;
    if engine_type.is_empty() {
        engine_type_file.write(engine.as_bytes())?;
        engine_type_file.flush()?;
    } else if engine_type != String::from(engine) {
        return Err(format_err!("Wrong engine"));
    }
    Ok(engine)
}
