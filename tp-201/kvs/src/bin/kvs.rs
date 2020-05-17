#[macro_use]
extern crate clap;

use std::process::exit;

fn main() {
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

    match matches.subcommand() {
        ("get", Some(_arg_match)) => {
            eprintln!("unimplemented");
            exit(1);
        }
        ("set", Some(_arg_match)) => {
            eprintln!("unimplemented");
            exit(1);
        }
        ("rm", Some(_arg_match)) => {
            eprintln!("unimplemented");
            exit(1);
        }
        (subcommand, _) => {
            eprintln!("Unrecognized command: {}", subcommand);
            exit(1);
        }
    }
}
