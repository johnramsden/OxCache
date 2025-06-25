use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct GetRequest {
    pub key: String,
    pub offset: usize,
    pub size: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum GetResponse {
    Error(String),
    Response(Vec<u8>),
}

#[derive(Debug, Serialize,Deserialize)]
pub enum Request {
    Get(GetRequest),
    Close
}