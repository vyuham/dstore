use bytes::Bytes;
use futures_util::StreamExt;
use std::{
    collections::HashMap,
    net::SocketAddr,
    str,
    sync::{Arc, Mutex},
};
use tokio::sync::mpsc;
use tonic::{transport::Server, Request, Response, Status};

use crate::{
    dstore_proto::{
        dnode_client::DnodeClient,
        dstore_server::{Dstore, DstoreServer},
        Byte, Null, PushArg, Size,
    },
    MAX_BYTE_SIZE,
};

pub struct Store {
    db: Arc<Mutex<HashMap<Bytes, Bytes>>>,
    nodes: Arc<Mutex<Vec<Bytes>>>,
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
    async fn join(&self, args: Request<Byte>) -> Result<Response<Null>, Status> {
        let mut nodes = self.nodes.lock().unwrap();
        nodes.push(Bytes::from(args.into_inner().body));
        Ok(Response::new(Null {}))
    }

    async fn contains(&self, args: Request<Byte>) -> Result<Response<Size>, Status> {
        let db = self.db.lock().unwrap();
        if let Some(value) = db.get(&args.into_inner().body[..]) {
            Ok(Response::new(Size {
                size: value.len() as i32,
            }))
        } else {
            Err(Status::not_found("Value doesn't exist"))
        }
    }

    async fn push(&self, args: Request<PushArg>) -> Result<Response<Null>, Status> {
        let mut db = self.db.lock().unwrap();
        let args = args.into_inner();
        match db.contains_key(&args.key[..]) {
            true => Err(Status::already_exists(format!(
                "{} already in use.",
                str::from_utf8(&args.key).unwrap()
            ))),
            false => {
                db.insert(Bytes::from(args.key), Bytes::from(args.value));
                Ok(Response::new(Null {}))
            }
        }
    }

    async fn push_file(
        &self,
        args: Request<tonic::Streaming<Byte>>,
    ) -> Result<Response<Null>, Status> {
        let mut stream = args.into_inner();
        let mut i: usize = 0;
        let (mut key, mut buf) = (vec![], vec![]);
        while let Some(byte) = stream.next().await {
            let byte = byte?;
            if i == 0 {
                key.append(&mut byte.body.clone());
            } else {
                buf.append(&mut byte.body.clone());
            }
            i += 1;
        }

        self.db
            .lock()
            .unwrap()
            .insert(Bytes::from(key), Bytes::from(buf));
        Ok(Response::new(Null {}))
    }

    async fn pull(&self, args: Request<Byte>) -> Result<Response<Byte>, Status> {
        let db = self.db.lock().unwrap();
        let args = args.into_inner();
        match db.get(&args.body[..]) {
            Some(val) => Ok(Response::new(Byte { body: val.to_vec() })),
            None => Err(Status::not_found(format!(
                "{} mapping doesn't exist.",
                str::from_utf8(&args.body).unwrap()
            ))),
        }
    }

    type PullFileStream = mpsc::Receiver<Result<Byte, Status>>;

    async fn pull_file(
        &self,
        args: Request<Byte>,
    ) -> Result<Response<Self::PullFileStream>, Status> {
        let (mut tx, rx) = mpsc::channel(4);
        let db = self.db.clone();

        tokio::spawn(async move {
            let val = db
                .lock()
                .unwrap()
                .get(&args.into_inner().body[..])
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

    async fn remove(&self, args: Request<Byte>) -> Result<Response<Null>, Status> {
        let key = args.into_inner().body;

        // Notify all nodes to delete copy
        let nodes = self.nodes.lock().unwrap().clone();
        for addr in nodes {
            let addr = format!("http://{}", str::from_utf8(&addr).unwrap());
            DnodeClient::connect(addr.clone())
                .await
                .unwrap()
                .remove(Request::new(Byte { body: key.clone() }))
                .await?;
            eprintln!("Deleting from node: {}", addr);
        }

        match self.db.lock().unwrap().remove(&key[..]) {
            Some(_) => Ok(Response::new(Null {})),
            None => Err(Status::not_found(format!(
                "Couldn't remove {}",
                str::from_utf8(&key).unwrap()
            ))),
        }
    }
}
