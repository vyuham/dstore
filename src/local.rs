use bytes::Bytes;
use std::collections::HashMap;
use tonic::{transport::Channel, Request};

use crate::dstore_proto::dstore_client::DstoreClient;
use crate::dstore_proto::{GetArg, SetArg};
use crate::DstoreError;

pub struct Store {
    db: HashMap<String, Bytes>,
    global: DstoreClient<Channel>,
}

impl Store {
    pub async fn new(addr: String) -> Result<Self, Box<dyn std::error::Error>> {
        Ok(Self {
            db: HashMap::new(),
            global: DstoreClient::connect(addr).await?,
        })
    }

    pub async fn insert(
        &mut self,
        key: String,
        value: Bytes,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if self.db.contains_key(&key) {
            Err(Box::new(DstoreError::new("Key occupied!".to_string())))
        } else {
            let req = Request::new(SetArg {
                key: key.clone(),
                value: value.to_vec(),
            });
            let res = self.global.set(req).await?.into_inner();
            if res.success {
                self.db.insert(key, value);
                Ok(eprintln!("Database updated"))
            } else {
                let req = Request::new(GetArg { key: key.clone() });
                let res = self.global.get(req).await?.into_inner();
                self.db.insert(key, Bytes::from(res.value));
                Err(Box::new(DstoreError::new(
                    "Local updated, Key occupied!".to_string(),
                )))
            }
        }
    }

    pub async fn get(&mut self, key: &String) -> Result<Bytes, Box<dyn std::error::Error>> {
        match self.db.get(key) {
            Some(value) => Ok(value.clone()),
            None => {
                let req = Request::new(GetArg { key: key.clone() });
                let res = self.global.get(req).await?.into_inner();
                if res.success {
                    eprintln!("Updating Local");
                    self.db.insert(key.clone(), Bytes::from(res.value.clone()));
                    Ok(Bytes::from(res.value))
                } else {
                    Err(Box::new(DstoreError::new(
                        "Key-Value mapping doesn't exist".to_string(),
                    )))
                }
            }
        }
    }

    pub fn remove(&mut self, key: String) {
        match self.db.get(&key) {
            Some(value) => {
                eprintln!(
                    "({} -> {}) Removing local mapping!",
                    key,
                    String::from_utf8(value.to_vec()).unwrap()
                );
                self.db.remove(&key);
            }
            None => eprintln!("Key-Value mapping doesn't exist"),
        }
    }
}
