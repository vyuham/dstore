use bytes::Bytes;
use std::error::Error;
use tonic::{transport::Channel, Request};

use crate::{
    dstore_proto::{dstore_client::DstoreClient, Byte, KeyValue},
    DstoreError,
};

pub struct Queue {
    global: DstoreClient<Channel>,
}

impl Queue {
    pub async fn connect(global_addr: &str) -> Result<Self, Box<dyn Error>> {
        Ok(Self {
            global: DstoreClient::connect(format!("http://{}", global_addr)).await?,
        })
    }

    pub async fn push_back(&mut self, key: Bytes, value: Bytes) -> Result<(), Box<dyn Error>> {
        match self
            .global
            .en_queue(Request::new(KeyValue {
                key: key.to_vec(),
                value: value.to_vec(),
            }))
            .await
        {
            Ok(_) => Ok(()),
            Err(e) => Err(Box::new(DstoreError(e.message().to_string()))),
        }
    }

    pub async fn pop_front(&mut self, key: Bytes) -> Result<Bytes, Box<dyn Error>> {
        match self
            .global
            .de_queue(Request::new(Byte { body: key.to_vec() }))
            .await
        {
            Ok(value) => Ok(Bytes::from(value.into_inner().body)),
            Err(e) => Err(Box::new(DstoreError(e.message().to_string()))),
        }
    }
}
