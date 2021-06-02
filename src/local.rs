use bytes::Bytes;
use futures::{stream, StreamExt};
use std::{collections::HashMap, error::Error, sync::Arc};
use tokio::{
    sync::Mutex,
    time::{self, Duration},
};
use tonic::{transport::Channel, Request};

use crate::{
    dstore_proto::{dstore_client::DstoreClient, Byte, KeyValue},
    DstoreError, MAX_BYTE_SIZE,
};

/// Maintain state of Local cache
pub struct Local {
    /// Local, cached in-memory database
    db: HashMap<Bytes, Bytes>,
    /// Stores client connection with Global
    global: DstoreClient<Channel>,
    /// Using an address as UID
    pub addr: String,
}

impl Local {
    /// Generate reference counted pointer to datastructure maintaining Local state
    pub async fn new(
        global_addr: &str,
        local_addr: &str,
    ) -> Result<Arc<Mutex<Self>>, Box<dyn Error>> {
        // Client connection to Global server
        let mut global = DstoreClient::connect(format!("http://{}", global_addr)).await?;

        // Check if Local is allowed to join Global's cluster
        match global
            .join(Request::new(Byte {
                body: local_addr.as_bytes().to_vec(),
            }))
            .await
        {
            Ok(_) => {
                // If able to join, create reference counted pointer to Local state
                let node = Arc::new(Mutex::new(Self {
                    db: HashMap::new(),
                    global,
                    addr: local_addr.to_string(),
                }));

                // Start a timer at intervals of 5 seconds, create clone of Local pointer
                let mut timer = time::interval(Duration::from_secs(5));
                let updater = node.clone();

                // Start thread to concurrently update cache by refering Global invalidation queue
                tokio::spawn(async move {
                    loop {
                        timer.tick().await;
                        updater.lock().await.update().await;
                    }
                });

                Ok(node)
            }
            Err(_) => Err(Box::new(DstoreError("Couldn't join cluster".to_string()))),
        }
    }

    /// Remove cached mappings as per directions from Global Invalidation queue
    pub async fn update(&mut self) {
        while let Ok(key) = self
            .global
            .update(Request::new(Byte {
                body: self.addr.as_bytes().to_vec(),
            }))
            .await
        {
            self.db.remove(&key.into_inner().body[..]);
        }
    }

    /// Insert VALUEs onto Global in either a single packet or as a stream as per it's size
    pub async fn insert(&mut self, key: Bytes, value: Bytes) -> Result<&str, Box<dyn Error>> {
        if value.len() < MAX_BYTE_SIZE {
            self.insert_single(key, value).await
        } else {
            self.insert_file(key, value).await
        }
    }

    /// Insert a single packet sized KEY->VALUE mapping onto Global and store in cache
    pub async fn insert_single(&mut self, key: Bytes, value: Bytes) -> Result<&str, Box<dyn Error>> {
        // Check if LOCAL already contains KEY
        if self.db.contains_key(&key) {
            return Err(Box::new(DstoreError("Key occupied!".to_string())));
        } else {
            // If not, consult Global
            let req = Byte { body: key.to_vec() };
            match self.global.contains(Request::new(req)).await {
                Ok(size) => {
                    // If Global contains KEY, update LOCAL cache
                    if size.into_inner().size as usize > MAX_BYTE_SIZE {
                        self.get_file(&key).await?;
                    } else {
                        self.get_single(&key).await?;
                    }
                    Err(Box::new(DstoreError(
                        "Local updated, Key occupied!".to_string(),
                    )))
                }
                Err(_) => {
                    // Else push a single packet KEY -> VALUE to update GLOBAL
                    let req = Request::new(KeyValue {
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
                        // If Global updated successfully, add mapping to cache
                        self.db.insert(key, value);
                        Ok("Database updated")
                    }
                }
            }
        }
    }

    /// Insert large KEY -> VALUE mappings on Global and store in cache
    pub async fn insert_file(&mut self, key: Bytes, value: Bytes) -> Result<&str, Box<dyn Error>> {
        // Check if LOCAL already contains KEY
        if self.db.contains_key(&key) {
            return Err(Box::new(DstoreError("Key occupied!".to_string())));
        } else {
            // If not, consult Global
            let req = Byte { body: key.to_vec() };
            match self.global.contains(Request::new(req.clone())).await {
                Ok(size) => {
                    // If Global contains KEY, update LOCAL cache
                    if size.into_inner().size as usize > MAX_BYTE_SIZE {
                        self.get_file(&key).await?;
                    } else {
                        self.get_single(&key).await?;
                    }
                    Err(Box::new(DstoreError(
                        "Local updated, Key occupied!".to_string(),
                    )))
                }
                Err(_) => {
                    // Else push steam of packets ordered as `KEY, VALUE(1), VALUE(2)..` frames, to update GLOBAL
                    let mut frames = vec![Byte { body: key.to_vec() }];
                    // Size each frame upto MAX_BYTE_SIZE
                    for i in 0..value.len() / MAX_BYTE_SIZE {
                        frames.push(Byte {
                            body: value[i * MAX_BYTE_SIZE..(i + 1) * MAX_BYTE_SIZE].to_vec(),
                        })
                    }

                    // If global accepts stream, update cache, else fail task
                    match self
                        .global
                        .push_file(Request::new(stream::iter(frames)))
                        .await
                    {
                        Ok(_) => {
                            self.db.insert(key, value);
                            Ok("Database updated")
                        }
                        Err(e) => Err(Box::new(DstoreError(format!(
                            "Couldn't update Global: {}",
                            e
                        )))),
                    }
                }
            }
        }
    }

    /// Get VALUE associated with KEY from system
    pub async fn get(&mut self, key: &Bytes) -> Result<(&str, Bytes), Box<dyn Error>> {
        // Check cache for KEY, if it exists, return associated VALUE
        match self.db.get(key) {
            Some(value) => Ok(("", value.clone())),
            None => {
                // If KEY in Global, extract VALUE byte size
                let size = match self
                    .global
                    .contains(Request::new(Byte { body: key.to_vec() }))
                    .await
                {
                    Ok(res) => res.into_inner().size,
                    Err(e) => return Err(Box::new(DstoreError(format!("Global: {}", e)))),
                } as usize;
                // If VALUE sized larger than single packet transportable, use get_file(), else use get_single()
                if size < MAX_BYTE_SIZE {
                    self.get_single(key).await
                } else {
                    self.get_file(key).await
                }
            }
        }
    }

    /// Get VALUES that can fit in a single packet
    pub async fn get_single(&mut self, key: &Bytes) -> Result<(&str, Bytes), Box<dyn Error>> {
        // Check if KEY is present in cache, else consult Global
        match self.db.get(key) {
            Some(value) => Ok(("", value.clone())),
            None => {
                // Send pull request to Global, update cache if successful
                let req = Request::new(Byte { body: key.to_vec() });
                match self.global.pull(req).await {
                    Ok(res) => {
                        let res = res.into_inner();
                        self.db.insert(key.clone(), Bytes::from(res.body.clone()));
                        Ok(("Updated Local", Bytes::from(res.body)))
                    }
                    Err(_) => Err(Box::new(DstoreError(
                        "Key-Value mapping doesn't exist".to_string(),
                    ))),
                }
            }
        }
    }

    /// Get VALUES that don't fit in a single packet
    pub async fn get_file(&mut self, key: &Bytes) -> Result<(&str, Bytes), Box<dyn Error>> {
        // Check if KEY is present in cache, else consult Global
        match self.db.get(key) {
            Some(value) => Ok(("", value.clone())),
            None => {
                // Send pull_file request to Global, update cache with streamed response
                let req = Request::new(Byte { body: key.to_vec() });
                let mut stream = self.global.pull_file(req).await.unwrap().into_inner();
                let mut value = vec![];
                while let Some(frame) = stream.next().await {
                    let mut frame = frame?;
                    value.append(&mut frame.body);
                }
                self.db.insert(key.clone(), Bytes::from(value.clone()));
                Ok(("Updated Local", Bytes::from(value)))
            }
        }
    }

    /// Remove a KEY from the system
    pub async fn remove(&mut self, key: &Bytes) -> Result<&str, Box<dyn Error>> {
        // Store erroring locations to write error message
        let mut err = vec![];
        // Send remove request to Global
        let req = Request::new(Byte { body: key.to_vec() });
        if let Err(_) = self.global.remove(req).await {
            err.push("global");
        }

        // Check if mapping exists locally, let `update()` remove KEY if it does
        if !self.db.contains_key(key) {
            err.push("local");
        }

        // Return error message based on contents of err
        match err.len() {
            0 => Ok("Mappings removed"),
            _ => Err(Box::new(DstoreError(format!(
                "Key missing from {}!",
                err.join(" and ")
            )))),
        }
    }
}
