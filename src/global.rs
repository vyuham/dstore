use bytes::Bytes;
use futures::StreamExt;
use std::{
    collections::{HashMap, VecDeque},
    str,
    sync::Arc,
};
use tokio::sync::{mpsc, Mutex};
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
    /// A mapping of queues and their names
    queues: Arc<Mutex<HashMap<Bytes, Mutex<VecDeque<Bytes>>>>>,
    /// Maps Local UIDs to a KEY invalidation queue
    cluster: Arc<Mutex<HashMap<Bytes, Mutex<VecDeque<Bytes>>>>>,
}

impl Global {
    /// Generate initial, empty state of Global
    fn new() -> Self {
        Self {
            db: Arc::new(Mutex::new(HashMap::new())),
            queues: Arc::new(Mutex::new(HashMap::new())),
            cluster: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Initialiaze server and start Global service on `addr`
    pub async fn start_server(addr: &str) -> Result<(), Box<dyn std::error::Error>> {
        Server::builder()
            .add_service(DstoreServer::new(Self::new()))
            .serve(addr.parse().unwrap())
            .await?;

        Ok(())
    }

    /// Wrapper for HashMap insert(), an abstraction for simplifying memory access
    pub async fn insert(&self, key: &Vec<u8>, value: &Vec<u8>) -> Result<(), String> {
        let mut db = self.db.lock().await;
        match db.contains_key(key) {
            true => Err(format!("{} already in use.", str::from_utf8(key).unwrap())),
            false => {
                db.insert(Bytes::from(key.clone()), Bytes::from(value.clone()));
                Ok(())
            }
        }
    }
}

#[tonic::async_trait]
impl Dstore for Global {
    /// RPC to add new Local to cluster, with empty invalidation queue
    async fn join(&self, args: Request<Byte>) -> Result<Response<Null>, Status> {
        self.cluster.lock().await.insert(
            Bytes::from(args.into_inner().body),
            Mutex::new(VecDeque::new()),
        );

        Ok(Response::new(Null {}))
    }

    /// Check if a certain KEY exists on Global, if yes return size of associated VALUE
    async fn contains(&self, args: Request<Byte>) -> Result<Response<Size>, Status> {
        match self.db.lock().await.get(&args.into_inner().body[..]) {
            Some(value) => Ok(Response::new(Size {
                size: value.len() as i32,
            })),
            None => Err(Status::not_found("Value doesn't exist")),
        }
    }

    /// RPC that maps KEY to VALUE, if it doesn't already exist on Global
    async fn push(&self, args: Request<KeyValue>) -> Result<Response<Null>, Status> {
        let KeyValue { key, value } = args.into_inner();
        match self.insert(&key, &value).await {
            Ok(_) => Ok(Response::new(Null {})),
            Err(e) => Err(Status::already_exists(e)),
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
        let (mut key, mut val_buf) = (vec![], vec![]);
        while let Some(byte) = stream.next().await {
            let Byte { body } = byte?;
            if i == 0 {
                key.append(&mut body.clone());
            } else {
                val_buf.append(&mut body.clone());
            }
            i += 1;
        }

        match self.insert(&key, &val_buf).await {
            Ok(_) => Ok(Response::new(Null {})),
            Err(e) => Err(Status::already_exists(e)),
        }
    }

    /// RPC that returns VALUE associated with KEY, provided it exist on Global
    async fn pull(&self, args: Request<Byte>) -> Result<Response<Byte>, Status> {
        let db = self.db.lock().await;
        let Byte { body } = args.into_inner();
        match db.get(&body[..]) {
            Some(val) => Ok(Response::new(Byte { body: val.to_vec() })),
            None => Err(Status::not_found(format!(
                "{} mapping doesn't exist.",
                str::from_utf8(&body).unwrap()
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
        let Byte { body } = args.into_inner();

        // Spawn thread to manage partitioning of a large VALUE into packet frames
        tokio::spawn(async move {
            let val = db.lock().await.get(&body[..]).unwrap().to_vec();
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
        for addr in self.cluster.lock().await.values() {
            addr.lock().await.push_back(Bytes::from(key.clone()));
        }

        // Remove KEY mapping from Global
        match self.db.lock().await.remove(&key[..]) {
            Some(_) => Ok(Response::new(Null {})),
            None => Err(Status::not_found(format!(
                "Couldn't remove {}",
                str::from_utf8(&key).unwrap()
            ))),
        }
    }

    /// RPC to push_back VALUEs onto a queue corresponding to a given KEY
    async fn en_queue(&self, args: Request<KeyValue>) -> Result<Response<Null>, Status> {
        let KeyValue { key, value } = args.into_inner();
        let mut queues = self.queues.lock().await;
        match queues.get(&key[..]) {
            Some(queue) => queue.lock().await.push_back(Bytes::from(value)),
            None => {
                let mut queue = VecDeque::new();
                queue.push_back(Bytes::from(value));
                queues.insert(Bytes::from(key), Mutex::new(queue));
            }
        }

        Ok(Response::new(Null {}))
    }

    /// RPC to pop_front VALUEs from a queue corresponding to a given KEY
    async fn de_queue(&self, args: Request<Byte>) -> Result<Response<Byte>, Status> {
        let Byte { body: key } = args.into_inner();
        match self.queues.lock().await.get(&key[..]) {
            Some(queue) => match queue.lock().await.pop_front() {
                Some(value) => Ok(Response::new(Byte {
                    body: value.to_vec(),
                })),
                None => Err(Status::not_found("")),
            },
            None => Err(Status::not_found("")),
        }
    }

    /// RPC to help Local invalidate cached VALUEs
    async fn update(&self, args: Request<Byte>) -> Result<Response<Byte>, Status> {
        // Extract and return a KEY from invalidate queue associated with requesting Local
        let Byte { body } = args.into_inner();
        match self
            .cluster
            .lock()
            .await
            .get(&body[..])
            .unwrap()
            .lock()
            .await
            .pop_front()
        {
            Some(keys) => Ok(Response::new(Byte {
                body: keys.to_vec(),
            })),
            None => Err(Status::not_found("")),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    async fn insert_single_test() {
        let global = Global::new();
        let (key, expected) = (b"Hello".to_vec(), b"World".to_vec());
        global.insert(&key, &expected).await;
        let value = global.get(&key).await;

        assert_eq!(value, expected);
    }
}
