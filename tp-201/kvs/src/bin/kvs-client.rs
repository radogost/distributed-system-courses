use kvs::Client;
use kvs::Result;

use std::process::exit;

use clap::{App, Arg, ArgMatches, SubCommand};

fn get_address<'a>(matches: &'a ArgMatches) -> &'a str {
    matches.value_of("address").unwrap_or("127.0.0.1:4000")
}

fn main() -> Result<()> {
    let matches = App::new("kvs-client")
        .version(env!("CARGO_PKG_VERSION"))
        .author(env!("CARGO_PKG_AUTHORS"))
        .arg(Arg::with_name("version").short("V"))
        .subcommand(
            SubCommand::with_name("get")
                .about("Get the value of a given key")
                .arg(Arg::with_name("key").required(true))
                .arg(Arg::with_name("address").long("addr").takes_value(true)),
        )
        .subcommand(
            SubCommand::with_name("rm")
                .about("Remove a given key")
                .arg(Arg::with_name("key").required(true))
                .arg(Arg::with_name("address").long("addr").takes_value(true)),
        )
        .subcommand(
            SubCommand::with_name("set")
                .about("Set the value of a given key")
                .arg(Arg::with_name("key").required(true))
                .arg(Arg::with_name("value").required(true))
                .arg(Arg::with_name("address").long("addr").takes_value(true)),
        )
        .get_matches();

    if matches.is_present("version") {
        println!(env!("CARGO_PKG_VERSION"));
        return Ok(());
    }

    match matches.subcommand() {
        ("get", Some(matches)) => {
            let address = get_address(matches);
            let mut client = Client::new(address)?;
            let key = matches.value_of("key").expect("key argument missing");
            let value = client.get(key.to_owned())?;
            if value.is_empty() {
                println!("Key not found");
            } else {
                println!("{}", value);
            }
        }
        ("set", Some(matches)) => {
            let address = get_address(matches);
            let mut client = Client::new(address)?;
            let key = matches.value_of("key").expect("key argument missing");
            let value = matches.value_of("value").expect("value argument missing");
            client.set(key.to_owned(), value.to_owned())?;
        }
        ("rm", Some(matches)) => {
            let address = get_address(matches);
            let mut client = Client::new(address)?;
            let key = matches.value_of("key").expect("key argument is missing");
            client.remove(key.to_owned())?;
        }
        (subcommand, _) => {
            eprintln!("Unrecognized command: {}", subcommand);
            exit(1);
        }
    };

    Ok(())
}
