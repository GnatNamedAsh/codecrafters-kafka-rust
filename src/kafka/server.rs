use std::collections::HashMap;
use std::io::{self, Read, Write};
use std::net::{Shutdown, SocketAddr};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tokio::time::timeout;
use crate::kafka::api;
use crate::kafka::topic::Topic;

pub struct Connection {
    stream: TcpStream,
    correlation_id: i32,
    pub api_key: i16,
    pub api_version: u16,
    pub body: [u8; 1000],
    pub body_length: usize,
    pub topics: Vec<Topic>,
    pub partition_limit: i32,
    client_id: String,
    read_timeout: Duration,
    write_timeout: Duration,
}

impl Connection {
    pub fn new(stream: TcpStream) -> Self {
        Connection {
            stream,
            correlation_id: 0,
            api_key: 0,
            api_version: 0,
            body: [0; 1000],
            body_length: 0,
            topics: Vec::new(),
            partition_limit: 0,
            client_id: String::from("default"),
            read_timeout: Duration::from_secs(30),
            write_timeout: Duration::from_secs(30),
        }
    }

    // Read the request header
    // The request header is 12 bytes long
    // the first 4 bytes are the length of the request (int32)
    // the next 2 bytes are the request API key (int16)
    // the next 2 bytes are the request API version (int16)
    // the final 4 bytes are the correlation ID (int32)
    pub async fn read(&mut self) -> Result<(), io::Error> {
        let mut request_header: [u8; 1024] = [0; 1024];
        match timeout(self.read_timeout, self.stream.read(&mut request_header)).await {
            Ok(Ok(bytes_read)) if bytes_read == 0 => {
                return Err(io::Error::new(
                    io::ErrorKind::ConnectionAborted,
                    "Connection closed by peer",
                ));
            }
            Ok(Ok(_)) => {
                let message_size = i32::from_be_bytes(request_header[0..4].try_into().unwrap());
                self.api_key = i16::from_be_bytes(request_header[4..6].try_into().unwrap());
                self.api_version = u16::from_be_bytes(request_header[6..8].try_into().unwrap());
                self.correlation_id = i32::from_be_bytes(request_header[8..12].try_into().unwrap());
                let client_id_length = i16::from_be_bytes(request_header[12..14].try_into().unwrap());
                let mut next_index = 14 + client_id_length as usize;
                self.client_id = String::from_utf8(request_header[14..next_index].to_vec()).unwrap();
                // tag buffer after client id, length is 1 byte, ignore it
                next_index += 1;
                // check if we're at the end of the request message
                if (next_index - 4) >= (message_size as usize) {
                    self.body_length = 0;
                } else {
                    self.body_length = (message_size as usize) - next_index + 4;
                    // theoretically, the body is the remaining bytes in the request header
                    // i.e. if the message size is 32, and the client_id_length is 9, we're at index 23 + 1 for the tag buffer, so 24
                    // since the message size bytes are not calculated as part of the message size, we need to add back the 4 bytes for the message size 
                    // the body is the remaining bytes, so 32 - next_index + 4 = 32 - 24 + 4 = 12
                    
                    // this means the totoal protocol message is really 36 bytes, but the message size is 32
                    // the header takes up 8 bytes, the client_id + length bytes take up 11 bytes, and the tag buffer takes up 1 byte
                    // so the body is the remaining bytes, which is 32 - 8 - 11 - 1 = 12

                    // we add 4 to account for the message size bytes at the start, then copy to the body
                    self.body[0..self.body_length].copy_from_slice(&request_header[next_index..message_size as usize + 4]);
                }

                Ok(())
            }
            Ok(Err(e)) => Err(e),
            Err(_) => Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "Read operation timed out",
            )),
        }
    }

    pub fn determine_api_key_func(
        &self,
    ) -> Option<fn(&mut Connection) -> Result<([u8; 1016], i32), io::Error>> {
        match self.api_key {
            18 => Some(api::handle_api_key_18),
            75 => Some(api::handle_api_key_75),
            _ => None,
        }
    }

    pub async fn write(&mut self) -> Result<(), io::Error> {
        let mut header: [u8; 8] = [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];
        header[4..8].copy_from_slice(&self.correlation_id.to_be_bytes());
        let (response_body, body_size) = match self.determine_api_key_func() {
            Some(func) => func(self)?,
            None => return Err(io::Error::new(io::ErrorKind::Other, "No function found")),
        };
        header[0..4].copy_from_slice(&(body_size + 4).to_be_bytes());

        match timeout(self.write_timeout, async {
            self.stream.write(&header).await?;
            let (body, _) = response_body.split_at(body_size as usize);
            self.stream.write(body).await?;
            self.stream.flush().await
        })
        .await
        {
            Ok(Ok(())) => Ok(()),
            Ok(Err(e)) => Err(e),
            Err(_) => Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "Write operation timed out",
            )),
        }
    }
}

pub struct Server {
    connections: Arc<Mutex<HashMap<SocketAddr, Connection>>>,
}

impl Server {
    pub fn new() -> Self {
        Server {
            connections: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn run(&self, addr: &str, port: u16) {
        let addr = format!("{}:{}", addr, port);
        let listener = TcpListener::bind(addr).await.unwrap();
        // println!("Server listening on {}", listener.local_addr().unwrap());

        loop {
            match listener.accept().await {
                Ok((stream, peer_addr)) => {
                    // println!("Connection established: {}", peer_addr);

                    // Clone the Arc to the connections for this task
                    let connections = Arc::clone(&self.connections);

                    // Spawn a new task for each connection
                    tokio::spawn(async move {
                        let conn = Connection::new(stream);

                        // Create a scope for the lock to ensure it's released quickly
                        {
                            let mut connections_lock = connections.lock().await;
                            connections_lock.insert(peer_addr, conn);
                        }

                        // Handle the connection
                        Self::handle_connection(connections, peer_addr).await;
                    });
                }
                Err(e) => {
                    println!("Error accepting connection: {}", e);
                }
            }
        }
    }

    async fn handle_connection(
        connections: Arc<Mutex<HashMap<SocketAddr, Connection>>>,
        peer_addr: SocketAddr,
    ) {
        // Get a clone of the connection to work with outside the mutex
        let mut conn = {
            let mut connections_lock = connections.lock().await;
            if let Some(conn) = connections_lock.remove(&peer_addr) {
                println!("Starting to handle connection from {}", peer_addr);
                conn
            } else {
                println!("Connection not found: {}", peer_addr);
                return;
            }
        };

        loop {
            // Read operation
            match conn.read().await {
                Ok(_) => {
                    // Write operation
                    if let Err(e) = conn.write().await {
                        println!("Error writing to connection {}: {}", peer_addr, e);
                        break;
                    }
                }
                Err(e) => {
                    println!("Error reading from connection {}: {}", peer_addr, e);
                    break;
                }
            }
        }

        // Clean up the connection when done
        let mut connections_lock = connections.lock().await;
        if connections_lock.remove(&peer_addr).is_some() {
            println!("Connection closed: {}", peer_addr);
        }
    }
}
