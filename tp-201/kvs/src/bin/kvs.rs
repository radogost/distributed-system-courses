use std::env::current_dir;
use std::process::exit;

use kvs::{KvStore, Result};

use clap::clap_app;

fn main() -> Result<()> {
    let matches = clap_app![kvs =>
        (version: env!("CARGO_PKG_VERSION"))
        (author: env!("CARGO_PKG_AUTHORS"))
        (@subcommand get =>
            (about: "Get the value of a given key")
            (@arg key: "key")
        )
        (@subcommand set =>
            (about: "Set the value of a given key")
            (@arg key: "key")
            (@arg value: "value")
        )
        (@subcommand rm =>
            (about: "Remove a given key")
            (@arg key: "key")
        )
    ]
    .get_matches();

    let mut kvs = KvStore::open(current_dir()?)?;
    match matches.subcommand() {
        ("get", Some(matches)) => {
            let key = matches.value_of("key").expect("key argument missing");
            let value = kvs.get(key.to_string())?;
            match value {
                Some(value) => println!("{}", value),
                None => println!("Key not found"),
            }
        }
        ("set", Some(matches)) => {
            let key = matches.value_of("key").expect("key argument missing");
            let value = matches.value_of("value").expect("value argument missing");
            kvs.set(key.to_string(), value.to_string())?
        }
        ("rm", Some(matches)) => {
            let key = matches.value_of("key").expect("key argument is missing");
            match kvs.remove(key.to_string()) {
                Ok(()) => {}
                Err(_) => {
                    println!("Key not found");
                    exit(1);
                }
            }
        }
        (subcommand, _) => {
            eprintln!("Unrecognized command: {}", subcommand);
            exit(1);
        }
    }
    Ok(())
}
