use std::collections::HashMap;
use std::io::{self, Read, Write};
use std::net::{Shutdown, SocketAddr};
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Mutex;
use std::sync::Arc;

pub struct Connection {
    stream: TcpStream,
    correlation_id: i32,
    api_key: i16,
    api_version: i16
}

impl Connection {
    pub fn new(stream: TcpStream) -> Self {
        Connection {
            stream,
            correlation_id: 0,
            api_key: 0,
            api_version: 0
        }
    }

    // Read the request header
    // The request header is 12 bytes long
    // the first 4 bytes are the length of the request (int32)
    // the next 2 bytes are the request API key (int16)
    // the next 2 bytes are the request API version (int16)
    // the final 4 bytes are the correlation ID (int32)
    pub async fn read(&mut self) -> Result<[u8; 2048], io::Error> {
      // TODO: this is a hack to get the request header, we should use a buffer pool or
      // specify the exact size of the request header
      let mut request_header: [u8; 2048] = [0; 2048];
      self.stream.read(&mut request_header).await?;
      self.correlation_id = i32::from_be_bytes(request_header[8..12].try_into().unwrap());
      if self.correlation_id != 0 {
        println!("correlation_id: {}", self.correlation_id);
      }
      self.api_key = i16::from_be_bytes(request_header[4..6].try_into().unwrap());
      self.api_version = i16::from_be_bytes(request_header[6..8].try_into().unwrap());
      Ok(request_header)
    }

    pub async fn write(&mut self) -> Result<(), io::Error> {
      // we want to add in the error_code to the response header. It comes after the correlation_id
      // uses an int16
        let mut header: [u8; 10] = [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];
        header[4..8].copy_from_slice(&self.correlation_id.to_be_bytes());
        if self.api_key != 4 {
          header[8..10].copy_from_slice(&35_i16.to_be_bytes());
        }

        self.stream.write(&header).await?;
        self.stream.flush().await?;
        Ok(())
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
        println!("Server listening on {}", listener.local_addr().unwrap());

        loop {
            match listener.accept().await {
                Ok((stream, peer_addr)) => {
                    println!("Connection established: {}", peer_addr);
                    
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

    async fn handle_connection(connections: Arc<Mutex<HashMap<SocketAddr, Connection>>>, peer_addr: SocketAddr) {
        loop {
            // Scope for the lock
            {
                let mut connections_lock = connections.lock().await;
                if let Some(conn) = connections_lock.get_mut(&peer_addr) {
                    match conn.read().await {
                        Ok(message) => message,
                        Err(e) => {
                            println!("Error reading from connection {}: {}", peer_addr, e);
                            break;
                        }
                    }
                } else {
                    println!("Connection not found: {}", peer_addr);
                    break;
                }
            };
            
            // Another scope for writing
            {
                let mut connections_lock = connections.lock().await;
                if let Some(conn) = connections_lock.get_mut(&peer_addr) {
                    if let Err(e) = conn.write().await {
                        println!("Error writing to connection {}: {}", peer_addr, e);
                        break;
                    }
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