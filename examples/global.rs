use dstore::global::Store;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "127.0.0.1:50051".parse().unwrap();

    println!("Dstore server listening on {}", addr);
    Store::start_server(addr).await
}
