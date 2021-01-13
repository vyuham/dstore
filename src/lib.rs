use std::error::Error;
use std::fmt;

#[derive(Debug)]
pub struct DstoreError {
    msg: String,
}

impl DstoreError {
    pub fn new(msg: String) -> Self {
        Self { msg }
    }
}

impl fmt::Display for DstoreError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "dstore error: {}", self.msg)
    }
}

impl Error for DstoreError {}

pub mod dstore_proto {
    tonic::include_proto!("dstore");
}

pub mod global;
pub mod local;
