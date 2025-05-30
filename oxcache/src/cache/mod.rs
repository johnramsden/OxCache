pub mod bucket;

pub struct Cache {
    buckets: bucket::Bucket,
}

impl Cache {
    pub fn new() -> Self {
        Self {
            buckets: bucket::Bucket::new(),
        }
    }
}
