use dstore::global::Global;
use std::error::Error;

/// Start Global server on defined IP:PORT address
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let addr = "127.0.0.1:50051".parse().unwrap();
    println!("Dstore server listening on {}", addr);
    Global::start_server(addr).await
}
