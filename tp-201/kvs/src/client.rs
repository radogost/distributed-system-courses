use crate::error::{KvsError, Result};
use crate::protocol::{CommandRequest, CommandResponse};

use std::io::Write;
use std::net::{TcpStream, ToSocketAddrs};

use bincode;

/// Client used to connect a server
pub struct Client {
    stream: TcpStream,
}

impl Client {
    /// Creates a new client
    pub fn new<A: ToSocketAddrs>(addr: A) -> Result<Self> {
        let stream = TcpStream::connect(addr)?;
        Ok(Self { stream })
    }

    /// Get the value for the given key
    pub fn get(&mut self, key: String) -> Result<String> {
        let command = CommandRequest::Get { key };
        let response = self.send_request(command)?;

        match response {
            CommandResponse::Value(value) => Ok(value),
            CommandResponse::Failure(cause) => Err(KvsError::Generic(cause)),
            _ => unreachable!(),
        }
    }

    /// Set value for the given key
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let command = CommandRequest::Set { key, value };
        let response = self.send_request(command)?;

        match response {
            CommandResponse::Success() => Ok(()),
            CommandResponse::Failure(cause) => Err(KvsError::Generic(cause)),
            _ => unreachable!(),
        }
    }

    /// Remove the given key from the store
    pub fn remove(&mut self, key: String) -> Result<()> {
        let command = CommandRequest::Remove { key };
        let response = self.send_request(command)?;

        match response {
            CommandResponse::Success() => Ok(()),
            CommandResponse::Failure(cause) => Err(KvsError::Generic(cause)),
            _ => unreachable!(),
        }
    }

    fn send_request(&mut self, request: CommandRequest) -> Result<CommandResponse> {
        let request = bincode::serialize(&request).unwrap();
        self.stream.write(&request)?;

        let response = bincode::deserialize_from(&self.stream)?;
        Ok(response)
    }
}
