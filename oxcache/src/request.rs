use bytes::{Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct GetRequest {
    pub key: String,
    pub offset: usize,
    pub size: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum GetResponse {
    Error(String),
    Response(Bytes),
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Request {
    Get(GetRequest),
    Close,
}

impl GetRequest {
    pub fn validate(&self, chunk_size: usize) -> tokio::io::Result<()> {
        if self.offset % chunk_size != 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "Offset must be aligned, recieved {}, not aligned to {}",
                    self.offset, chunk_size
                ),
            ));
        }
        if self.size > chunk_size || self.size == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "Request size {} can not be larger than chunk size {} or smaller than 1",
                    self.size, chunk_size
                ),
            ));
        }

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_get_request_validation_unaligned_offset() {
        let req = GetRequest {
            key: String::from("TEST"),
            offset: 5,
            size: 4096,
        };
        assert!(
            req.validate(4096).is_err(),
            "Expected error for unaligned offset, recieved Ok(())"
        )
    }
    #[test]
    fn test_get_request_validation_too_large() {
        let req = GetRequest {
            key: String::from("TEST"),
            offset: 0,
            size: 8192,
        };
        assert!(
            req.validate(4096).is_err(),
            "Expected error for size larger than chunk, recieved Ok(())"
        )
    }

    #[test]
    fn test_get_request_validation_chunk_size_zero() {
        let req = GetRequest {
            key: String::from("TEST"),
            offset: 0,
            size: 0,
        };
        assert!(
            req.validate(4096).is_err(),
            "Expected error for size 0, recieved Ok(())"
        )
    }

    #[test]
    fn test_get_request_validation_match_chunk_size() {
        let req = GetRequest {
            key: String::from("TEST"),
            offset: 0,
            size: 4096,
        };
        let res = req.validate(4096);
        assert!(
            !res.is_err(),
            "Expected success for size larger than chunk, recieved {:?}",
            res
        )
    }
}
