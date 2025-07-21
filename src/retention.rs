use std::time::{Duration, Instant};

pub struct RetentionConfig {
    pub max_messages_per_partition: usize,
    pub max_age: Duration,
    pub cleanup_interval: Duration,
}

impl Default for RetentionConfig {
    fn default() -> Self {
        Self {
            max_messages_per_partition: 10000,  // Keep last 10k messages per partition
            max_age: Duration::from_secs(3600),  // 1 hour
            cleanup_interval: Duration::from_secs(60),  // Clean up every minute
        }
    }
}

#[derive(Clone)]
pub struct TimestampedRecord {
    pub key: Option<bytes::Bytes>,
    pub value: bytes::Bytes,
    pub headers: indexmap::IndexMap<kafka_protocol::protocol::StrBytes, Option<bytes::Bytes>>,
    pub timestamp: Instant,
}

impl TimestampedRecord {
    pub fn new(
        key: Option<bytes::Bytes>,
        value: bytes::Bytes,
        headers: indexmap::IndexMap<kafka_protocol::protocol::StrBytes, Option<bytes::Bytes>>,
    ) -> Self {
        Self {
            key,
            value,
            headers,
            timestamp: Instant::now(),
        }
    }
}