#![allow(unused_imports)]
use crate::kafka::server::Server;

mod kafka;

#[tokio::main]
async fn main() {
    // Uncomment this block to pass the first stage
    let server = Server::new();
    server.run("127.0.0.1", 9092).await;
}
