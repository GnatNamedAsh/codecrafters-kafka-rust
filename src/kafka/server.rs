use std::collections::HashMap;
use std::io::{self, Read, Write};
use std::net::{Shutdown, SocketAddr};
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Mutex;
use std::sync::Arc;

pub struct Connection {
    stream: TcpStream,
}

impl Connection {
    pub fn new(stream: TcpStream) -> Self {
        Connection {
            stream,
        }
    }

    pub async fn read(&mut self) -> Result<String, io::Error> {
        let mut buffer = [0; 1024];
        let n = self.stream.read(&mut buffer).await?;
        Ok(String::from_utf8_lossy(&buffer[..n]).to_string())
    }

    pub async fn write(&mut self) -> Result<(), io::Error> {
        let header: [u8; 8] = [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07];
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