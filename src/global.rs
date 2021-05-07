use bytes::Bytes;
use futures::StreamExt;
use std::{
    collections::{HashMap, VecDeque},
    net::SocketAddr,
    str,
    sync::{Arc, Mutex},
};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{transport::Server, Request, Response, Status};

use crate::{
    dstore_proto::{
        dstore_server::{Dstore, DstoreServer},
        Byte, KeyValue, Null, Size,
    },
    MAX_BYTE_SIZE,
};

/// Strore reference counted pointers to HashMaps maintaining state of Global
pub struct Global {
    /// In-memory database mapping KEY -> VALUE
    db: Arc<Mutex<HashMap<Bytes, Bytes>>>,
    /// Maps Local UIDs to a KEY invalidation queue
    cluster: Arc<Mutex<HashMap<Bytes, Arc<Mutex<VecDeque<Bytes>>>>>>,
}

impl Global {
    /// Generate initial, empty state of Global
    fn new() -> Self {
        Self {
            db: Arc::new(Mutex::new(HashMap::new())),
            cluster: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Initialiaze server and start Global service on `addr`
    pub async fn start_server(addr: SocketAddr) -> Result<(), Box<dyn std::error::Error>> {
        Server::builder()
            .add_service(DstoreServer::new(Self::new()))
            .serve(addr)
            .await?;

        Ok(())
    }
}

#[tonic::async_trait]
impl Dstore for Global {
    /// RPC to add new Local to cluster, with empty invalidation queue
    async fn join(&self, args: Request<Byte>) -> Result<Response<Null>, Status> {
        self.cluster.lock().unwrap().insert(
            Bytes::from(args.into_inner().body),
            Arc::new(Mutex::new(VecDeque::new())),
        );

        Ok(Response::new(Null {}))
    }

    /// Check if a certain KEY exists on Global, if yes return size of associated VALUE
    async fn contains(&self, args: Request<Byte>) -> Result<Response<Size>, Status> {
        match self.db.lock().unwrap().get(&args.into_inner().body[..]) {
            Some(value) => Ok(Response::new(Size {
                size: value.len() as i32,
            })),
            None => Err(Status::not_found("Value doesn't exist")),
        }
    }

    /// RPC that maps KEY to VALUE, if it doesn't already exist on Global
    async fn push(&self, args: Request<KeyValue>) -> Result<Response<Null>, Status> {
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

    /// RPC that maps KEY to streamed VALUE, provided it doesn't already exist on Global
    async fn push_file(
        &self,
        args: Request<tonic::Streaming<Byte>>,
    ) -> Result<Response<Null>, Status> {
        // Logic to recieve streamed VALUES
        let mut stream = args.into_inner();
        let mut i = 0;
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

    /// RPC that returns VALUE associated with KEY, provided it exist on Global
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

    /// Type to allow streaming og VALUE via RPC
    type PullFileStream = ReceiverStream<Result<Byte, Status>>;

    /// RPC that streams VALUE associated with KEY, if it exist on Global
    async fn pull_file(
        &self,
        args: Request<Byte>,
    ) -> Result<Response<Self::PullFileStream>, Status> {
        // Create a double ended channel for transporting VALUE packets processed within thread
        let (tx, rx) = mpsc::channel(4);
        let db = self.db.clone();

        // Spawn thread to manage partitioning of a large VALUE into packet frames
        tokio::spawn(async move {
            let val = db
                .lock()
                .unwrap()
                .get(&args.into_inner().body[..])
                .unwrap()
                .to_vec();
            // Size each frame upto MAX_BYTE_SIZE and encapsulate in response packet
            for i in 0..val.len() / MAX_BYTE_SIZE {
                tx.send(Ok(Byte {
                    body: val[i * MAX_BYTE_SIZE..(i + 1) * MAX_BYTE_SIZE].to_vec(),
                }))
                .await
                .unwrap();
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    /// RPC to remove KEY mappings on Global and add KEY to invalidate queues of Locals in cluster
    async fn remove(&self, args: Request<Byte>) -> Result<Response<Null>, Status> {
        let key = args.into_inner().body;

        // Push KEY into invalidate queue of all node
        for addr in self.cluster.lock().unwrap().values() {
            addr.lock().unwrap().push_back(Bytes::from(key.clone()));
        }

        // Remove KEY mapping from Global
        match self.db.lock().unwrap().remove(&key[..]) {
            Some(_) => Ok(Response::new(Null {})),
            None => Err(Status::not_found(format!(
                "Couldn't remove {}",
                str::from_utf8(&key).unwrap()
            ))),
        }
    }

    /// RPC to help Local invalidate cached VALUES
    async fn update(&self, args: Request<Byte>) -> Result<Response<Byte>, Status> {
        // Extract and return a KEY from invalidate queue associated with requesting Local
        let args = args.into_inner().body;
        match self
            .cluster
            .lock()
            .unwrap()
            .get(&args[..])
            .unwrap()
            .lock()
            .unwrap()
            .pop_front()
        {
            Some(keys) => Ok(Response::new(Byte {
                body: keys.to_vec(),
            })),
            None => Err(Status::not_found("")),
        }
    }
}

