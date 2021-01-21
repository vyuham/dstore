use bytes::Bytes;
use futures_util::{stream, StreamExt};
use std::{collections::HashMap, error::Error};
use tonic::{transport::Channel, Request};

use crate::{
    dstore_proto::{dstore_client::DstoreClient, Byte, PushArg},
    DstoreError, MAX_BYTE_SIZE,
};

pub struct Node {
    db: HashMap<Bytes, Bytes>,
    global: DstoreClient<Channel>,
    pub addr: String,
}

impl Node {
    pub async fn new(global_addr: String, local_addr: String) -> Result<Self, Box<dyn Error>> {
        let mut global = DstoreClient::connect(format!("http://{}", global_addr)).await?;
        match global
            .join(Request::new(Byte {
                body: local_addr.as_bytes().to_vec(),
            }))
            .await
        {
            Ok(_) => {
                let node = Self {
                    db: HashMap::new(),
                    global,
                    addr: local_addr,
                };

                Ok(node)
            }
            Err(_) => Err(Box::new(DstoreError("Couldn't join cluster".to_string()))),
        }
    }

    async fn update(&mut self) {
        while let Ok(key) = self.global.update(Request::new(Byte {body: self.addr.as_bytes().to_vec()})).await {
            self.db.remove(&key.into_inner().body[..]);
        }
    }

    pub async fn insert(&mut self, key: Bytes, value: Bytes) -> Result<(), Box<dyn Error>> {
        if value.len() > MAX_BYTE_SIZE {
            self.insert_single(key, value).await
        } else {
            self.insert_file(key, value).await
        }
    }

    pub async fn insert_single(&mut self, key: Bytes, value: Bytes) -> Result<(), Box<dyn Error>> {
        self.update().await;
        
        if self.db.contains_key(&key) {
            return Err(Box::new(DstoreError("Key occupied!".to_string())));
        } else {
            let req = Byte { body: key.to_vec() };
            match self.global.contains(Request::new(req.clone())).await {
                Ok(_) => {
                    let res = self.global.pull(Request::new(req)).await?.into_inner();
                    self.db.insert(key, Bytes::from(res.body));
                    Err(Box::new(DstoreError(
                        "Local updated, Key occupied!".to_string(),
                    )))
                }
                Err(_) => {
                    let mut frames = vec![Byte { body: key.to_vec() }];
                    for i in 0..value.len() / MAX_BYTE_SIZE {
                        frames.push(Byte {
                            body: value[i * MAX_BYTE_SIZE..(i + 1) * MAX_BYTE_SIZE].to_vec(),
                        })
                    }
                    let req = Request::new(stream::iter(frames));
                    let res = self.global.push_file(req).await;
                    if let Err(e) = res {
                        Err(Box::new(DstoreError(format!(
                            "Couldn't update Global: {}",
                            e
                        ))))
                    } else {
                        self.db.insert(key, value);
                        Ok(eprintln!("Database updated"))
                    }
                }
            }
        }
    }

    pub async fn insert_file(&mut self, key: Bytes, value: Bytes) -> Result<(), Box<dyn Error>> {
        self.update().await;

        if self.db.contains_key(&key) {
            return Err(Box::new(DstoreError("Key occupied!".to_string())));
        } else {
            let req = Byte { body: key.to_vec() };
            match self.global.contains(Request::new(req.clone())).await {
                Ok(_) => {
                    let res = self.global.pull(Request::new(req)).await?.into_inner();
                    self.db.insert(key, Bytes::from(res.body));
                    Err(Box::new(DstoreError(
                        "Local updated, Key occupied!".to_string(),
                    )))
                }
                Err(_) => {
                    let req = Request::new(PushArg {
                        key: key.to_vec(),
                        value: value.to_vec(),
                    });
                    let res = self.global.push(req).await;
                    if let Err(e) = res {
                        Err(Box::new(DstoreError(format!(
                            "Couldn't update Global: {}",
                            e
                        ))))
                    } else {
                        self.db.insert(key, value);
                        Ok(eprintln!("Database updated"))
                    }
                }
            }
        }
    }

    pub async fn get(&mut self, key: &Bytes) -> Result<Bytes, Box<dyn Error>> {
        self.update().await;

        match self.db.get(key) {
            Some(value) => Ok(value.clone()),
            None => {
                let size = match self
                    .global
                    .contains(Request::new(Byte { body: key.to_vec() }))
                    .await
                {
                    Ok(res) => res.into_inner().size,
                    Err(e) => return Err(Box::new(DstoreError(format!("Global: {}", e)))),
                } as usize;
                if size > MAX_BYTE_SIZE {
                    self.get_file(key).await
                } else {
                    self.get_single(key).await
                }
            }
        }
    }

    pub async fn get_single(&mut self, key: &Bytes) -> Result<Bytes, Box<dyn Error>> {
        self.update().await;

        match self.db.get(key) {
            Some(value) => Ok(value.clone()),
            None => {
                let req = Request::new(Byte { body: key.to_vec() });
                match self.global.pull(req).await {
                    Ok(res) => {
                        let res = res.into_inner();
                        eprintln!("Updating Local");
                        self.db.insert(key.clone(), Bytes::from(res.body.clone()));
                        Ok(Bytes::from(res.body))
                    }
                    Err(_) => Err(Box::new(DstoreError(
                        "Key-Value mapping doesn't exist".to_string(),
                    ))),
                }
            }
        }
    }

    pub async fn get_file(&mut self, key: &Bytes) -> Result<Bytes, Box<dyn Error>> {
        self.update().await;

        match self.db.get(key) {
            Some(value) => Ok(value.clone()),
            None => {
                let req = Request::new(Byte { body: key.to_vec() });
                let mut stream = self.global.pull_file(req).await.unwrap().into_inner();
                eprintln!("Updating Local");
                let mut value = vec![];
                while let Some(frame) = stream.next().await {
                    let mut frame = frame?;
                    value.append(&mut frame.body);
                }
                self.db.insert(key.clone(), Bytes::from(value.clone()));
                Ok(Bytes::from(value))
            }
        }
    }

    pub async fn remove(&mut self, key: &Bytes) -> Result<(), Box<dyn Error>> {
        let mut err = vec![];
        let req = Request::new(Byte { body: key.to_vec() });
        match self.global.remove(req).await {
            Ok(_) => eprintln!("Global mapping removed!"),
            Err(_) => err.push("global"),
        }

        match self.db.contains_key(key) {
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
