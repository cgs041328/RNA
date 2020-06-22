use clap::{App, Arg, SubCommand};
use kvs::*;
use serde::Deserialize;
use serde_json;
use std::env;
use std::io::prelude::*;
use std::net::{SocketAddr, TcpStream};
use std::process::exit;

#[macro_use]
extern crate failure;

const DEFAULT_ADDRESS: &str = "127.0.0.1:4000";

fn main() -> Result<()> {
    let matches = App::new("kvs-client")
        .version(env!("CARGO_PKG_VERSION"))
        .author(env!("CARGO_PKG_AUTHORS"))
        .about(env!("CARGO_PKG_DESCRIPTION"))
        .subcommand(
            SubCommand::with_name("set")
                .args(&[
                    Arg::with_name("key").help("A string key").required(true),
                    Arg::with_name("value").help("A string key").required(true),
                    Arg::with_name("addr")
                        .help("Server address")
                        .long("addr")
                        .takes_value(true)
                        .value_name("IP-PORT")
                        .default_value(DEFAULT_ADDRESS),
                ])
                .about("Set the value of a string key to a string"),
        )
        .subcommand(
            SubCommand::with_name("get")
                .args(&[
                    Arg::with_name("key").help("A string key").required(true),
                    Arg::with_name("addr")
                        .help("Server address")
                        .long("addr")
                        .value_name("IP-PORT")
                        .default_value(DEFAULT_ADDRESS),
                ])
                .about("Get the string value of a given string key"),
        )
        .subcommand(
            SubCommand::with_name("rm")
                .args(&[
                    Arg::with_name("key").help("A string key").required(true),
                    Arg::with_name("addr")
                        .help("Server address")
                        .long("addr")
                        .value_name("IP-PORT")
                        .default_value(DEFAULT_ADDRESS),
                ])
                .about("Remove a given key"),
        )
        .get_matches();

    if let (cmd, Some(_matches)) = matches.subcommand() {
        let addr = _matches.value_of("addr").expect("Addr is missing");
        let addr: SocketAddr = addr.parse().expect("Addr format is wrong");
        let mut stream = TcpStream::connect(addr)?;

        match cmd {
            "set" => {
                let key = _matches.value_of("key").expect("Key is missing");
                let value = _matches.value_of("value").expect("Value is missing");
                let request = KvsRequest::Set {
                    key: key.to_owned(),
                    value: value.to_owned(),
                };
                serde_json::to_writer(&mut stream, &request)?;
                // let mut store = KvStore::open(env::current_dir()?)?;
                // store.set(key.to_owned(), value.to_owned())?;
                parse_response(&mut stream)?;
            }
            "get" => {
                let key = _matches.value_of("key").expect("Key is missing");
                // let mut store = KvStore::open(env::current_dir()?)?;
                // match store.get(key.to_owned())? {
                //     Some(value) => {
                //         println!("{}", value);
                //     }
                //     None => {
                //         println!("Key not found");
                //     }
                // }
                let request = KvsRequest::Get {
                    key: key.to_owned(),
                };
                serde_json::to_writer(&mut stream, &request)?;
                stream.flush()?;
                match parse_response(&mut stream)? {
                    Some(value) => {
                        println!("{}", value);
                    }
                    None => {
                        println!("Key not found");
                    }
                }
            }
            "rm" => {
                let key = _matches.value_of("key").expect("Key is missing");
                // let mut store = KvStore::open(env::current_dir()?)?;
                // if let Err(_) = store.remove(key.to_owned()) {
                //     println!("Key not found");
                //     exit(1);
                // }
                let request = KvsRequest::Remove {
                    key: key.to_owned(),
                };
                serde_json::to_writer(&mut stream, &request)?;
                parse_response(&mut stream)?;
            }
            _ => unreachable!(),
        }
    }
    Ok(())
}

fn parse_response(stream: &mut TcpStream) -> Result<Option<String>> {
    let mut de = serde_json::Deserializer::from_reader(stream);
    let response = KvsResponse::deserialize(&mut de)?;
    match response {
        KvsResponse::Ok(value) => Ok(value),
        KvsResponse::Err(e) => Err(format_err!("{}", e)),
    }
}
