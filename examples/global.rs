use dstore::global::Store;
use std::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let addr = "127.0.0.1:50051".parse().unwrap();

    println!("Dstore server listening on {}", addr);
    Store::start_server(addr).await
}
