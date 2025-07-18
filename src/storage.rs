use bytes::{Bytes, BytesMut, BufMut};
use std::collections::HashMap;
use thiserror::Error;
use kafka_protocol::records::{
    Record as KafkaRecord, RecordBatchEncoder, RecordEncodeOptions, Compression, TimestampType
};
use indexmap::IndexMap;
use kafka_protocol::protocol::StrBytes;

// Simple CRC32 implementation for Kafka message format
fn crc32(data: &[u8]) -> u32 {
    let mut crc: u32 = 0xffffffff;
    for byte in data {
        crc ^= *byte as u32;
        for _ in 0..8 {
            crc = if crc & 1 != 0 {
                (crc >> 1) ^ 0xedb88320
            } else {
                crc >> 1
            };
        }
    }
    !crc
}

#[derive(Error, Debug)]
pub enum StorageError {
    #[error("Topic not found: {0}")]
    TopicNotFound(String),
    
    #[error("Partition not found: topic={0}, partition={1}")]
    PartitionNotFound(String, i32),
}

#[derive(Default)]
pub struct InMemoryStorage {
    topics: HashMap<String, Topic>,
}

struct Topic {
    partitions: HashMap<i32, Partition>,
}

#[derive(Clone)]
struct Record {
    key: Option<Bytes>,
    value: Bytes,
    headers: IndexMap<StrBytes, Option<Bytes>>,
}

struct Partition {
    records: Vec<Record>,
    next_offset: i64,
}

impl InMemoryStorage {
    pub fn new() -> Self {
        Self::default()
    }
    
    pub fn get_all_topics(&self) -> Vec<String> {
        self.topics.keys().cloned().collect()
    }
    
    pub fn get_partitions(&self, topic: &str) -> Vec<i32> {
        self.topics
            .get(topic)
            .map(|t| t.partitions.keys().cloned().collect())
            .unwrap_or_default()
    }
    
    pub fn append_records(&mut self, topic: &str, partition: i32, key: Option<Bytes>, value: Bytes) -> i64 {
        self.append_records_with_headers(topic, partition, key, value, IndexMap::new())
    }
    
    pub fn append_records_with_headers(&mut self, topic: &str, partition: i32, key: Option<Bytes>, value: Bytes, headers: IndexMap<StrBytes, Option<Bytes>>) -> i64 {
        let topic_entry = self.topics.entry(topic.to_string()).or_insert_with(|| {
            let mut partitions = HashMap::new();
            // Auto-create 3 partitions by default
            for i in 0..3 {
                partitions.insert(i, Partition {
                    records: Vec::new(),
                    next_offset: 0,
                });
            }
            Topic { partitions }
        });
        
        let partition_entry = topic_entry.partitions.entry(partition).or_insert_with(|| {
            Partition {
                records: Vec::new(),
                next_offset: 0,
            }
        });
        
        let offset = partition_entry.next_offset;
        
        partition_entry.records.push(Record { key, value, headers });
        partition_entry.next_offset += 1;
        
        offset
    }
    

    // Fetch multiple records in MessageSet format for API v3 and below
    pub fn fetch_batch_legacy(
        &self,
        topic: &str,
        partition: i32,
        offset: i64,
        max_bytes: i32,
    ) -> Result<(i64, Option<Bytes>), StorageError> {
        let topic_data = self.topics
            .get(topic)
            .ok_or_else(|| StorageError::TopicNotFound(topic.to_string()))?;
            
        let partition_data = topic_data.partitions
            .get(&partition)
            .ok_or_else(|| StorageError::PartitionNotFound(topic.to_string(), partition))?;
        
        let high_watermark = partition_data.next_offset;
        
        if offset < 0 || offset >= high_watermark {
            return Ok((high_watermark, None));
        }
        
        let mut message_set = BytesMut::new();
        let mut current_offset = offset;
        let mut total_size = 0i32;
        let mut record_count = 0;
        
        // Build a MessageSet with multiple messages
        while current_offset < high_watermark && total_size < max_bytes {
            let record_index = current_offset as usize;
            if record_index >= partition_data.records.len() {
                break;
            }
            
            let record = &partition_data.records[record_index];
            
            // Create a single message in MessageSet format
            let mut message = BytesMut::new();
            
            // Message format (v0/v1):
            // CRC (4 bytes)
            // Magic (1 byte) - 0 for v0, 1 for v1
            // Attributes (1 byte) - 0 for no compression
            // Key length (4 bytes) - -1 for null
            // Key (variable) - empty for null
            // Value length (4 bytes)
            // Value (variable)
            
            // For now, use magic 0 (v0) for simplicity
            let magic: u8 = 0;
            let attributes: u8 = 0;
            
            // Build the message content (after CRC)
            let mut msg_content = BytesMut::new();
            msg_content.put_u8(magic);
            msg_content.put_u8(attributes);
            
            // Key handling
            if let Some(key) = &record.key {
                msg_content.put_i32(key.len() as i32); // key length
                msg_content.extend_from_slice(key); // key
            } else {
                msg_content.put_i32(-1); // null key
            }
            
            // Value handling
            msg_content.put_i32(record.value.len() as i32); // value length
            msg_content.extend_from_slice(&record.value); // value
            
            // Calculate CRC32 of the message content
            let crc = crc32(&msg_content);
            
            // Build the complete message
            message.put_u32(crc);
            message.extend_from_slice(&msg_content);
            
            // MessageSet entry format:
            // Offset (8 bytes)
            // Message size (4 bytes)
            // Message (variable)
            
            let message_size = message.len() as i32;
            
            // Check if this message would exceed max_bytes
            if record_count > 0 && total_size + 12 + message_size > max_bytes {
                break;
            }
            
            // Write MessageSet entry
            message_set.put_i64(current_offset); // offset
            message_set.put_i32(message_size); // message size
            message_set.extend_from_slice(&message); // message
            
            total_size += 12 + message_size; // 8 (offset) + 4 (size) + message
            record_count += 1;
            current_offset += 1;
            
            // Limit batch size
            if record_count >= 50 {
                break;
            }
        }
        
        if message_set.is_empty() {
            Ok((high_watermark, None))
        } else {
            Ok((high_watermark, Some(message_set.freeze())))
        }
    }
    
    // Fetch multiple records in RecordBatch format for API v4 and above
    pub fn fetch_batch_recordbatch(
        &self,
        topic: &str,
        partition: i32,
        offset: i64,
        max_bytes: i32,
    ) -> Result<(i64, Option<Bytes>), StorageError> {
        let topic_data = self.topics
            .get(topic)
            .ok_or_else(|| StorageError::TopicNotFound(topic.to_string()))?;
            
        let partition_data = topic_data.partitions
            .get(&partition)
            .ok_or_else(|| StorageError::PartitionNotFound(topic.to_string(), partition))?;
        
        let high_watermark = partition_data.next_offset;
        
        if offset < 0 || offset >= high_watermark {
            return Ok((high_watermark, None));
        }
        
        // Collect records to batch
        let mut records = Vec::new();
        let mut current_offset = offset;
        let mut estimated_size = 0;
        
        while current_offset < high_watermark && estimated_size < max_bytes as usize {
            let record_index = current_offset as usize;
            if record_index >= partition_data.records.len() {
                break;
            }
            
            let stored_record = &partition_data.records[record_index];
            
            // Convert to kafka_protocol Record format
            let record = KafkaRecord {
                transactional: false,
                control: false,
                partition_leader_epoch: 0,
                producer_id: -1,
                producer_epoch: -1,
                timestamp_type: TimestampType::Creation,
                offset: current_offset,
                sequence: current_offset as i32,
                timestamp: current_offset * 1000, // Simple timestamp based on offset
                key: stored_record.key.clone(),
                value: Some(stored_record.value.clone()),
                headers: stored_record.headers.clone(),
            };
            
            // Estimate size (rough approximation)
            estimated_size += 50; // Base overhead
            if let Some(ref key) = stored_record.key {
                estimated_size += key.len();
            }
            estimated_size += stored_record.value.len();
            
            records.push(record);
            current_offset += 1;
            
            // Ensure we have at least one record
            if records.len() > 0 && estimated_size >= max_bytes as usize {
                break;
            }
        }
        
        if records.is_empty() {
            return Ok((high_watermark, None));
        }
        
        // Encode records as RecordBatch v2
        let mut buf = BytesMut::new();
        let options = RecordEncodeOptions {
            version: 2,
            compression: Compression::None,
        };
        
        match RecordBatchEncoder::encode(&mut buf, &records, &options) {
            Ok(_) => Ok((high_watermark, Some(buf.freeze()))),
            Err(e) => {
                eprintln!("Failed to encode RecordBatch: {:?}", e);
                Ok((high_watermark, None))
            }
        }
    }
    
    // Methods fetch_records, fetch_single_record, and fetch_records_multi were removed
    // as they were not used in production code
    
    pub fn topic_has_headers(&self, topic: &str, partition: i32) -> bool {
        if let Some(topic_data) = self.topics.get(topic) {
            if let Some(partition_data) = topic_data.partitions.get(&partition) {
                // Check if any record has headers
                return partition_data.records.iter().any(|r| !r.headers.is_empty());
            }
        }
        false
    }
    
    pub fn get_high_watermark(&self, topic: &str, partition: i32) -> Result<i64, StorageError> {
        let topic_data = self.topics
            .get(topic)
            .ok_or_else(|| StorageError::TopicNotFound(topic.to_string()))?;
            
        let partition_data = topic_data.partitions
            .get(&partition)
            .ok_or_else(|| StorageError::PartitionNotFound(topic.to_string(), partition))?;
        
        Ok(partition_data.next_offset)
    }
}
