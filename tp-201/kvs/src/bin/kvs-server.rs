use kvs::{KvStore, KvsEngine, KvsError, Result, Server, SledKvsEngine};

use std::env::current_dir;

use clap::{App, Arg, ArgMatches};

use env_logger::Env;
use log::info;

fn get_engine(matches: &ArgMatches) -> Result<String> {
    let engine_file = current_dir()?.join("engine");
    if !engine_file.exists() {
        return Ok(matches.value_of("engine").unwrap_or("kvs").to_owned());
    }

    let engine = std::fs::read_to_string(engine_file)?;

    if let Some(stored_engine) = matches.value_of("engine") {
        return if engine != stored_engine {
            Err(KvsError::Generic("Engine types don't match!".to_owned()))
        } else {
            Ok(engine)
        };
    }

    Ok(engine)
}

fn run<E: KvsEngine>(address: &str, engine: E) -> Result<()> {
    let mut server = Server::new(engine);
    server.run(address)
}

fn main() -> Result<()> {
    let logger_env = Env::default().filter_or("KVS", "info");
    env_logger::init_from_env(logger_env);

    let matches = App::new("kvs-server")
        .version(env!("CARGO_PKG_VERSION"))
        .author(env!("CARGO_PKG_AUTHORS"))
        .arg(Arg::with_name("version").short("V"))
        .arg(Arg::with_name("address").long("addr").takes_value(true))
        .arg(
            Arg::with_name("engine")
                .long("engine")
                .takes_value(true)
                .possible_values(&["sled", "kvs"]),
        )
        .get_matches();

    if matches.is_present("version") {
        println!(env!("CARGO_PKG_VERSION"));
        return Ok(());
    }

    let address = matches.value_of("address").unwrap_or("127.0.0.1:4000");
    let engine = get_engine(&matches)?;

    info!("Version: {}", env!("CARGO_PKG_VERSION"));
    info!("Address: {}", address);
    info!("Engine: {}", engine);

    match &engine[..] {
        "sled" => {
            let engine = SledKvsEngine::open(current_dir()?)?;
            run(address, engine)
        }
        "kvs" => {
            let engine = KvStore::open(current_dir()?)?;
            run(address, engine)
        }
        _ => unreachable!(),
    }
}
