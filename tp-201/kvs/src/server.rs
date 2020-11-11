use crate::error::{KvsError, Result};
use crate::protocol::{CommandRequest, CommandResponse};
use crate::KvsEngine;

use std::io::Write;
use std::net::{TcpListener, TcpStream, ToSocketAddrs};

/// Server
pub struct Server<E: KvsEngine> {
    engine: E,
}

impl<E: KvsEngine> Server<E> {
    /// Creates a new server on this IP:PORT
    pub fn new(engine: E) -> Self {
        Self { engine }
    }

    /// Serves incoming requests. Should never return.
    pub fn run<A: ToSocketAddrs>(&mut self, address: A) -> Result<()> {
        let listener = TcpListener::bind(address)?;
        for stream in listener.incoming() {
            self.handle_request(stream?)?;
        }
        Ok(())
    }

    fn handle_request(&mut self, mut stream: TcpStream) -> Result<()> {
        let client_request = bincode::deserialize_from(&stream)?;

        let response = match client_request {
            CommandRequest::Get { key } => self.handle_get(key),
            CommandRequest::Set { key, value } => self.handle_set(key, value),
            CommandRequest::Remove { key } => self.handle_remove(key),
        };

        let response = bincode::serialize(&response)?;
        stream.write(&response)?;

        Ok(())
    }

    fn handle_get(&mut self, key: String) -> CommandResponse {
        let value = self.engine.get(key);

        match value {
            Ok(Some(value)) => CommandResponse::Value(value),
            Ok(None) => CommandResponse::Value("".to_owned()),
            Err(e) => CommandResponse::Failure(e.to_string()),
        }
    }

    fn handle_set(&mut self, key: String, value: String) -> CommandResponse {
        match self.engine.set(key, value) {
            Ok(()) => CommandResponse::Success(),
            Err(e) => CommandResponse::Failure(e.to_string()),
        }
    }

    fn handle_remove(&mut self, key: String) -> CommandResponse {
        match self.engine.remove(key) {
            Ok(()) => CommandResponse::Success(),
            Err(KvsError::NoSuchKey(_)) => CommandResponse::Failure("Key not found!".to_owned()),
            Err(e) => CommandResponse::Failure(e.to_string()),
        }
    }
}
