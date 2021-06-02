use bytes::Bytes;
use dstore::Queue;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut queue = Queue::connect("[::1]:50051").await?;
    queue
        .push_back(Bytes::from("Hello"), Bytes::from("World"))
        .await?;
    let popped = queue.pop_front(Bytes::from("Hello")).await?.to_vec();
    eprintln!("Hello, {}", String::from_utf8(popped)?);

    Ok(())
}
