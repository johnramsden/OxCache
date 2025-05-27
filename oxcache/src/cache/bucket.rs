use dashmap::DashMap;

pub struct Bucket {
    state: DashMap<String, String>,
}

impl Bucket {
    pub fn new() -> Self {
        Self {
            state: DashMap::new(),
        }
    }
}