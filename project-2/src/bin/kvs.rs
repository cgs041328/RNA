use clap::{App, Arg, SubCommand};
use kvs::*;
use std::process::exit;

fn main() -> Result<()> {
    let matches = App::new(env!("CARGO_PKG_NAME"))
        .version(env!("CARGO_PKG_VERSION"))
        .author(env!("CARGO_PKG_AUTHORS"))
        .about(env!("CARGO_PKG_DESCRIPTION"))
        .subcommand(
            SubCommand::with_name("set")
                .args(&[
                    Arg::with_name("key").help("A string key").required(true),
                    Arg::with_name("value").help("A string key").required(true),
                ])
                .about("Set the value of a string key to a string"),
        )
        .subcommand(
            SubCommand::with_name("get")
                .arg(Arg::with_name("key").help("A string key").required(true))
                .about("Get the string value of a given string key"),
        )
        .subcommand(
            SubCommand::with_name("rm")
                .arg(Arg::with_name("key").help("A string key").required(true))
                .about("Remove a given key"),
        )
        .get_matches();

    match matches.subcommand() {
        ("set", Some(_matches)) => {
            eprintln!("unimplemented");
            exit(1);
        }
        ("get", Some(_matches)) => {
            eprintln!("unimplemented");
            exit(1);
        }
        ("rm", Some(_matches)) => {
            eprintln!("unimplemented");
            exit(1);
        }
        _ => unreachable!(),
    }
}
