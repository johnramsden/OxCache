use bytes::Bytes;
use nvme::types::Byte;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct GetRequest {
    pub key: String,
    pub offset: Byte,
    pub size: Byte,
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
    pub fn validate(&self, chunk_size: Byte) -> tokio::io::Result<()> {
        // Allow subset reads within a chunk - offset + size should not exceed chunk_size
        if self.offset + self.size > chunk_size {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "Request extends beyond chunk boundary: offset {} + size {} > chunk_size {}",
                    self.offset, self.size, chunk_size
                ),
            ));
        }
        if self.size == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Request size cannot be 0",
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
