use bytes::Bytes;
use std::{
    collections::HashMap,
    error::Error,
    net::SocketAddr,
    sync::{Arc, Mutex},
};
use tonic::{
    transport::{Channel, Server},
    Request, Response, Status,
};

use crate::dstore_proto::{
    dnode_server::{Dnode, DnodeServer},
    dstore_client::DstoreClient,
    {Addr, GetArg, SetArg, SetResult},
};
use crate::DstoreError;

#[derive(Debug, Clone)]
pub struct Node {
    db: Arc<Mutex<HashMap<String, Bytes>>>,
}

impl Node {
    pub fn new(db: Arc<Mutex<HashMap<String, Bytes>>>) -> Self {
        Self { db }
    }
}

#[tonic::async_trait]
impl Dnode for Node {
    async fn del(&self, del_arg: Request<GetArg>) -> Result<Response<SetResult>, Status> {
        match self.db.lock().unwrap().remove(&del_arg.into_inner().key) {
            Some(_) => Ok(Response::new(SetResult { success: true })),
            None => Ok(Response::new(SetResult { success: false })),
        }
    }
}

pub struct Store {
    db: Arc<Mutex<HashMap<String, Bytes>>>,
    global: DstoreClient<Channel>,
    pub addr: SocketAddr,
}

impl Store {
    pub async fn new(
        db: Arc<Mutex<HashMap<String, Bytes>>>,
        global_addr: String,
        local_addr: SocketAddr,
    ) -> Result<Self, Box<dyn Error>> {
        let mut global = DstoreClient::connect(global_addr).await?;
        global
            .join(Request::new(Addr {
                addr: format!("{}", local_addr),
            }))
            .await?
            .into_inner()
            .id;
        Ok(Self {
            db,
            global,
            addr: local_addr,
        })
    }

    pub async fn start_client(
        global_addr: String,
        local_addr: SocketAddr,
    ) -> Result<Self, Box<dyn Error>> {
        let db = Arc::new(Mutex::new(HashMap::new()));
        let node = Node::new(db.clone());
        tokio::spawn(async move {
            Server::builder()
                .add_service(DnodeServer::new(node))
                .serve(local_addr)
                .await
        });
        Ok(Self::new(db, global_addr, local_addr).await?)
    }

    pub async fn insert(&mut self, key: String, value: Bytes) -> Result<(), Box<dyn Error>> {
        let mut db = self.db.lock().unwrap();
        if db.contains_key(&key) {
            Err(Box::new(DstoreError("Key occupied!".to_string())))
        } else {
            let req = Request::new(SetArg {
                key: key.clone(),
                value: value.to_vec(),
            });
            let res = self.global.set(req).await?.into_inner();
            if res.success {
                db.insert(key, value);
                Ok(eprintln!("Database updated"))
            } else {
                let req = Request::new(GetArg { key: key.clone() });
                let res = self.global.get(req).await?.into_inner();
                db.insert(key, Bytes::from(res.value));
                Err(Box::new(DstoreError(
                    "Local updated, Key occupied!".to_string(),
                )))
            }
        }
    }

    pub async fn get(&mut self, key: &String) -> Result<Bytes, Box<dyn Error>> {
        let mut db = self.db.lock().unwrap();
        match db.get(key) {
            Some(value) => Ok(value.clone()),
            None => {
                let req = Request::new(GetArg { key: key.clone() });
                let res = self.global.get(req).await?.into_inner();
                if res.success {
                    eprintln!("Updating Local");
                    db.insert(key.clone(), Bytes::from(res.value.clone()));
                    Ok(Bytes::from(res.value))
                } else {
                    Err(Box::new(DstoreError(
                        "Key-Value mapping doesn't exist".to_string(),
                    )))
                }
            }
        }
    }

    pub async fn remove(&mut self, key: &String) -> Result<(), Box<dyn Error>> {
        let mut err = vec![];
        let db = self.db.lock().unwrap();
        let req = Request::new(GetArg { key: key.clone() });
        let res = self.global.del(req).await?.into_inner();
        match res.success {
            true => eprintln!("Global mapping removed!"),
            false => err.push("global"),
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
