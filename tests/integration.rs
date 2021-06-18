use dstore::Local;  
use bytes::Bytes;

#[tokio::test]
async fn push_to_global_test() {
    // intialize global and local
    let global_addr= "[::1]:50051";
    let local_addr= "[::1]:50052";
    
    let local = Local::new(global_addr, local_addr).await.unwrap();
    let mut local = local.lock().await;

    // push key:value to global
    let key = Bytes::from("hello");
    let value = Bytes::from("world");
    if let Err(e) = local.insert(key.clone(), value.clone()).await {
        eprintln!("{}", e);
    } else {
        // fetch key:value from global
        match local.get(&key).await {
            Ok(val) => {
                assert_eq!(val, &value)
            }
            Err(e) => eprintln!("{}", e),
        }
    }
}
