use bytes::Bytes;
use futures_util::StreamExt;
use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{Arc, Mutex},
};
use tokio::sync::mpsc;
use tonic::{transport::Server, Request, Response, Status};

use crate::{
    dstore_proto::{
        dnode_client::DnodeClient,
        dstore_server::{Dstore, DstoreServer},
        Addr, Byte, GetArg, GetResult, Id, SetArg, SetResult,
    },
    MAX_BYTE_SIZE,
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

    async fn set_file(
        &self,
        set_arg: Request<tonic::Streaming<Byte>>,
    ) -> Result<Response<SetResult>, Status> {
        let mut stream = set_arg.into_inner();
        let mut i: usize = 0;
        let (mut key, mut buf) = (String::new(), vec![]);
        {
            while let Some(byte) = stream.next().await {
                let byte = byte?;
                if i == 0 {
                    key = String::from_utf8(byte.body).unwrap();
                } else {
                    buf.append(&mut byte.body.clone());
                }
                i += 1;
            }
        }

        self.db.lock().unwrap().insert(key, Bytes::from(buf));
        Ok(Response::new(SetResult { success: true }))
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

    type GetFileStream = mpsc::Receiver<Result<Byte, Status>>;

    async fn get_file(
        &self,
        get_arg: Request<GetArg>,
    ) -> Result<Response<Self::GetFileStream>, Status> {
        let (mut tx, rx) = mpsc::channel(4);
        let db = self.db.clone();

        tokio::spawn(async move {
            let val = db
                .lock()
                .unwrap()
                .get(&get_arg.into_inner().key)
                .unwrap()
                .to_vec();
            for i in 0..val.len() / MAX_BYTE_SIZE {
                tx.send(Ok(Byte {
                    body: val[i * MAX_BYTE_SIZE..(i + 1) * MAX_BYTE_SIZE].to_vec(),
                }))
                .await
                .unwrap();
            }
        });

        Ok(Response::new(rx))
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
