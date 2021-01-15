use bytes::Bytes;
use std::sync::{Arc, Mutex};
use std::{collections::HashMap, net::SocketAddr};
use tonic::{transport::Server, Request, Response, Status};

use crate::dstore_proto::dstore_server::{Dstore, DstoreServer};
use crate::dstore_proto::{GetArg, GetResult, SetArg, SetResult};

pub struct Store {
    db: Arc<Mutex<HashMap<String, Bytes>>>,
}

impl Store {
    fn new() -> Self {
        Self {
            db: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn start_server(addr: SocketAddr) -> Result<(), Box<dyn std::error::Error>> {
        Server::builder()
            .add_service(DstoreServer::new(Self::new()))
            .serve(addr)
            .await?;

        Ok(())
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
                db.insert(args.key, Bytes::from(args.value));
                Ok(Response::new(SetResult { success: true }))
            }
        }
    }

    async fn get(&self, get_arg: Request<GetArg>) -> Result<Response<GetResult>, Status> {
        let db = self.db.lock().unwrap();
        let args = get_arg.into_inner();
        match db.get(&args.key) {
            Some(val) => Ok(Response::new(GetResult {
                value: val.to_vec(),
                success: true,
            })),
            None => Ok(Response::new(GetResult {
                value: vec![],
                success: false,
            })),
        }
    }

    async fn del(&self, get_arg: Request<GetArg>) -> Result<Response<SetResult>, Status> {
        let mut db = self.db.lock().unwrap();
        let args = get_arg.into_inner();
        match db.remove(&args.key) {
            Some(_) => Ok(Response::new(SetResult { success: true })),
            None => Ok(Response::new(SetResult { success: false })),
        }
    }
}
