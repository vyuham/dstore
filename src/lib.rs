use std::{error::Error, fmt};

/// Error type for Dstore, contains message as string
#[derive(Debug)]
pub struct DstoreError(pub String);

impl fmt::Display for DstoreError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "dstore error: {}", self.0)
    }
}

impl Error for DstoreError {}

pub mod dstore_proto {
    tonic::include_proto!("dstore");
}

/// Maximum size of contents in a gRPC packet as per standard
pub const MAX_BYTE_SIZE: usize = 4_194_304;

pub mod global;
pub mod local;
