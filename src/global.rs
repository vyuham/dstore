use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tonic::{transport::Server, Request, Response, Status};

use dstore::dstore_proto::dstore_server::{Dstore, DstoreServer};
use dstore::dstore_proto::{GetArg, GetResult, SetArg, SetResult};
struct Store {
    db: Arc<Mutex<HashMap<String, String>>>,
}

impl Store {
    fn new(db: Arc<Mutex<HashMap<String, String>>>) -> Self {
        Self { db }
    }
}

#[tonic::async_trait]
impl Dstore for Store {
    async fn set(&self, set_arg: Request<SetArg>) -> Result<Response<SetResult>, Status> {
        let mut db = self.db.lock().unwrap();
        let args = set_arg.into_inner();
        match db.contains_key(&args.key) {
            true => Ok(Response::new(SetResult { success: false })),
            false => {
                db.insert(args.key, args.value);
                Ok(Response::new(SetResult { success: true }))
            }
        }
    }

    async fn get(&self, get_arg: Request<GetArg>) -> Result<Response<GetResult>, Status> {
        let db = self.db.lock().unwrap();
        let args = get_arg.into_inner();
        match db.get(&args.key) {
            Some(val) => Ok(Response::new(GetResult {
                value: val.clone(),
                success: true,
            })),
            None => Ok(Response::new(GetResult {
                value: "".to_string(),
                success: false,
            })),
        }
    }

    async fn del(&self, del_arg: Request<GetArg>) -> Result<Response<SetResult>, Status> {
        let mut db = self.db.lock().unwrap();
        let args = del_arg.into_inner();
        match db.contains_key(&args.key) {
            false => Ok(Response::new(SetResult { success: false })),
            true => {
                db.remove(&args.key);
                Ok(Response::new(SetResult { success: true }))
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "127.0.0.1:50051".parse().unwrap();
    let global_store = Arc::new(Mutex::new(HashMap::<String, String>::new()));

    println!("Dstore server listening on {}", addr);

    Server::builder()
        .add_service(DstoreServer::new(Store::new(global_store)))
        .serve(addr)
        .await?;

    Ok(())
}
