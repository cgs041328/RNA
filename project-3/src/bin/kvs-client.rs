use clap::{App, Arg, SubCommand};
use kvs::*;
use std::env;
use std::process::exit;

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
                        .default_value("127.0.0.1:4000"),
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
                        .default_value("127.0.0.1:4000"),
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
                        .default_value("127.0.0.1:4000"),
                ])
                .about("Remove a given key"),
        )
        .get_matches();

    match matches.subcommand() {
        ("set", Some(_matches)) => {
            let key = _matches.value_of("key").expect("Key is missing");
            let value = _matches.value_of("value").expect("Value is missing");
            let mut store = KvStore::open(env::current_dir()?)?;
            store.set(key.to_owned(), value.to_owned())?;
        }
        ("get", Some(_matches)) => {
            let key = _matches.value_of("key").expect("Key is missing");
            let mut store = KvStore::open(env::current_dir()?)?;
            match store.get(key.to_owned())? {
                Some(value) => {
                    println!("{}", value);
                }
                None => {
                    println!("Key not found");
                }
            }
        }
        ("rm", Some(_matches)) => {
            let key = _matches.value_of("key").expect("Key is missing");
            let mut store = KvStore::open(env::current_dir()?)?;
            if let Err(_) = store.remove(key.to_owned()) {
                println!("Key not found");
                exit(1);
            }
        }
        _ => unreachable!(),
    }
    Ok(())
}
