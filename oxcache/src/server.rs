use crate::cache::{Cache};

#[derive(Debug)]
pub struct ServerConfig {
    pub socket: String,
    pub disk: String
}

struct Server {
    cache: Cache,
    config: ServerConfig
}

impl Server {
    
    fn new(config: ServerConfig) -> Self {
        Self {
            cache: Cache::new(),
            config
        }
    }
    fn run(&self) {

    }
}