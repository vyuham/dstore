use bytes::Bytes;
use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{Arc, Mutex},
};
use tonic::{transport::Server, Request, Response, Status};

use crate::dstore_proto::{
    dnode_client::DnodeClient,
    dstore_server::{Dstore, DstoreServer},
    Addr, GetArg, GetResult, Id, SetArg, SetResult,
};

pub struct Store {
    db: Arc<Mutex<HashMap<String, Bytes>>>,
    nodes: Arc<Mutex<Vec<String>>>,
}

impl Store {
    fn new() -> Self {
        Self {
            db: Arc::new(Mutex::new(HashMap::new())),
            nodes: Arc::new(Mutex::new(vec![])),
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
    async fn join(&self, join_arg: Request<Addr>) -> Result<Response<Id>, Status> {
        let mut nodes = self.nodes.lock().unwrap();
        nodes.push(format!("http://{}", join_arg.into_inner().addr));
        Ok(Response::new(Id {
            id: nodes.len() as i32,
        }))
    }

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

    async fn del(&self, del_arg: Request<GetArg>) -> Result<Response<SetResult>, Status> {
        let key = del_arg.into_inner().key;
        // Notify all nodes to delete copy
        let nodes = self.nodes.lock().unwrap().clone();
        for addr in nodes {
            let req = Request::new(GetArg { key: key.clone() });
            DnodeClient::connect(addr.clone())
                .await
                .unwrap()
                .del(req)
                .await?;
            println!("Deleting from node: {}", addr);
        }

        match self.db.lock().unwrap().remove(&key) {
            Some(_) => Ok(Response::new(SetResult { success: true })),
            None => Ok(Response::new(SetResult { success: false })),
        }
    }
}
