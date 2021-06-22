use bytes::Bytes;
use dstore::Queue;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut queue = Queue::connect("[::1]:50051").await?;
    loop {
        let i = queue.pop_front(Bytes::from("tasks")).await?.to_vec();
        println!("{:?}", i);
    }

    Ok(())
}
