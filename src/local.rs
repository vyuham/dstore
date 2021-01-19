use bytes::Bytes;
use futures_util::stream;
use std::{
    collections::HashMap,
    error::Error,
    str,
    sync::{Arc, Mutex},
};
use tonic::{
    transport::{Channel, Server},
    Request, Response, Status,
};

use crate::DstoreError;
use crate::{
    dstore_proto::{
        dnode_server::{Dnode, DnodeServer},
        dstore_client::DstoreClient,
        Byte, Null, PushArg,
    },
    MAX_BYTE_SIZE,
};

#[derive(Debug, Clone)]
pub struct Node {
    db: Arc<Mutex<HashMap<Bytes, Bytes>>>,
}

impl Node {
    pub fn new(db: Arc<Mutex<HashMap<Bytes, Bytes>>>) -> Self {
        Self { db }
    }
}

#[tonic::async_trait]
impl Dnode for Node {
    async fn remove(&self, args: Request<Byte>) -> Result<Response<Null>, Status> {
        let key = args.into_inner().body;
        match self.db.lock().unwrap().remove(&key[..]) {
            Some(_) => Ok(Response::new(Null {})),
            None => Err(Status::not_found(format!(
                "{} not found!",
                str::from_utf8(&key).unwrap()
            ))),
        }
    }
}

pub struct Store {
    db: Arc<Mutex<HashMap<Bytes, Bytes>>>,
    global: DstoreClient<Channel>,
    pub addr: String,
}

impl Store {
    pub async fn new(
        db: Arc<Mutex<HashMap<Bytes, Bytes>>>,
        global_addr: String,
        local_addr: String,
    ) -> Result<Self, Box<dyn Error>> {
        let mut global = DstoreClient::connect(format!("http://{}",global_addr)).await?;
        match global
            .join(Request::new(Byte {
                body: local_addr.as_bytes().to_vec(),
            }))
            .await
        {
            Ok(_) => Ok(Self {
                db,
                global,
                addr: local_addr,
            }),
            Err(_) => Err(Box::new(DstoreError("Couldn't join cluster".to_string()))),
        }
    }

    pub async fn start_client(
        global_addr: String,
        local_addr: String,
    ) -> Result<Self, Box<dyn Error>> {
        let db = Arc::new(Mutex::new(HashMap::new()));
        let node = Node::new(db.clone());
        let addr = local_addr.clone().parse().unwrap();
        tokio::spawn(async move {
            Server::builder()
                .add_service(DnodeServer::new(node))
                .serve(addr)
                .await
        });
        Ok(Self::new(db, global_addr, local_addr).await?)
    }

    pub async fn insert(&mut self, key: Bytes, value: Bytes) -> Result<(), Box<dyn Error>> {
        let mut db = self.db.lock().unwrap();
        if db.contains_key(&key) {
            return Err(Box::new(DstoreError("Key occupied!".to_string())));
        } else {
            let req = Byte { body: key.to_vec() };
            match self.global.contains(Request::new(req.clone())).await {
                Ok(_) => {
                    let res = self.global.pull(Request::new(req)).await?.into_inner();
                    db.insert(key, Bytes::from(res.body));
                    Err(Box::new(DstoreError(
                        "Local updated, Key occupied!".to_string(),
                    )))
                }
                Err(_) => {
                    let res: Result<Response<Null>, Status>;
                    if value.len() > MAX_BYTE_SIZE {
                        let mut frames = vec![Byte { body: key.to_vec() }];
                        for i in 0..value.len() / MAX_BYTE_SIZE {
                            frames.push(Byte {
                                body: value[i * MAX_BYTE_SIZE..(i + 1) * MAX_BYTE_SIZE].to_vec(),
                            })
                        }
                        let req = Request::new(stream::iter(frames));
                        res = self.global.push_file(req).await;
                    } else {
                        let req = Request::new(PushArg {
                            key: key.to_vec(),
                            value: value.to_vec(),
                        });
                        res = self.global.push(req).await;
                    }
                    if let Err(e) = res {
                        Err(Box::new(DstoreError(format!(
                            "Couldn't update Global: {}",
                            e
                        ))))
                    } else {
                        db.insert(key, value);
                        Ok(eprintln!("Database updated"))
                    }
                }
            }
        }
    }

    pub async fn get(&mut self, key: &Bytes) -> Result<Bytes, Box<dyn Error>> {
        let mut db = self.db.lock().unwrap();
        match db.get(key) {
            Some(value) => Ok(value.clone()),
            None => {
                let req = Request::new(Byte { body: key.to_vec() });
                match self.global.pull(req).await {
                    Ok(res) => {
                        let res = res.into_inner();
                        eprintln!("Updating Local");
                        db.insert(key.clone(), Bytes::from(res.body.clone()));
                        Ok(Bytes::from(res.body))
                    }
                    Err(_) => Err(Box::new(DstoreError(
                        "Key-Value mapping doesn't exist".to_string(),
                    ))),
                }
            }
        }
    }

    pub async fn remove(&mut self, key: &Bytes) -> Result<(), Box<dyn Error>> {
        let mut err = vec![];
        let db = self.db.lock().unwrap();
        let req = Request::new(Byte { body: key.to_vec() });
        match self.global.remove(req).await {
            Ok(_) => eprintln!("Global mapping removed!"),
            Err(_) => err.push("global"),
        }

        match db.contains_key(key) {
            true => eprintln!("Local mapping removed!"),
            false => err.push("local"),
        }

        match err.len() {
            0 => Ok(()),
            _ => Err(Box::new(DstoreError(format!(
                "Key missing from {}!",
                err.join(" and ")
            )))),
        }
    }
}
