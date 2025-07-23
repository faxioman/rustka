use bytes::{Bytes, BytesMut, BufMut};
use std::collections::HashMap;
use thiserror::Error;
use kafka_protocol::records::{
    Record as KafkaRecord, RecordBatchEncoder, RecordEncodeOptions, Compression, TimestampType
};
use indexmap::IndexMap;
use kafka_protocol::protocol::StrBytes;
use std::time::Instant;
use std::collections::VecDeque;
use tracing::debug;

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
    timestamp: Instant,
    offset: i64,
}

struct Partition {
    records: VecDeque<Record>,
    next_offset: i64,
    base_offset: i64,  // First offset in the deque
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
                    records: VecDeque::new(),
                    next_offset: 0,
                    base_offset: 0,
                });
            }
            Topic { partitions }
        });
        
        let partition_entry = topic_entry.partitions.entry(partition).or_insert_with(|| {
            Partition {
                records: VecDeque::new(),
                next_offset: 0,
                base_offset: 0,
            }
        });
        
        let offset = partition_entry.next_offset;
        
        let headers_count = headers.len();
        if headers_count > 0 {
            debug!("Storing record at offset {} with {} headers for topic={}, partition={}", 
                   offset, headers_count, topic, partition);
        }
        
        partition_entry.records.push_back(Record { 
            key, 
            value, 
            headers,
            timestamp: Instant::now(),
            offset,
        });
        partition_entry.next_offset += 1;
        
        // Retention policy: size and time based
        const MAX_MESSAGES_PER_PARTITION: usize = 1000;
        const MAX_MESSAGE_AGE: std::time::Duration = std::time::Duration::from_secs(300); // 5 minutes
        
        // Remove old messages based on time
        let now = Instant::now();
        while let Some(front) = partition_entry.records.front() {
            if now.duration_since(front.timestamp) > MAX_MESSAGE_AGE {
                if let Some(removed) = partition_entry.records.pop_front() {
                    partition_entry.base_offset = removed.offset + 1;
                }
            } else {
                break; // Messages are ordered, so we can stop here
            }
        }
        
        if partition_entry.records.len() > MAX_MESSAGES_PER_PARTITION {
            if let Some(removed) = partition_entry.records.pop_front() {
                partition_entry.base_offset = removed.offset + 1;
            }
        }
        
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
        
        while current_offset < high_watermark && total_size < max_bytes {
            // Skip if offset is before our base_offset (already deleted)
            if current_offset < partition_data.base_offset {
                current_offset += 1;
                continue;
            }
            
            let record_index = (current_offset - partition_data.base_offset) as usize;
            if record_index >= partition_data.records.len() {
                break;
            }
            
            let record = &partition_data.records[record_index];
            
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
            // Skip if offset is before our base_offset (already deleted)
            if current_offset < partition_data.base_offset {
                current_offset += 1;
                continue;
            }
            
            let record_index = (current_offset - partition_data.base_offset) as usize;
            if record_index >= partition_data.records.len() {
                break;
            }
            
            let stored_record = &partition_data.records[record_index];
            
            let headers_count = stored_record.headers.len();
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
            
            if headers_count > 0 {
                debug!("Creating RecordBatch record at offset {} with {} headers", current_offset, headers_count);
            }
            
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
            Err(_) => {
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
    
    pub fn cleanup_old_messages(&mut self) {
        const MAX_MESSAGE_AGE: std::time::Duration = std::time::Duration::from_secs(300); // 5 minutes
        let now = Instant::now();
        
        for topic in self.topics.values_mut() {
            for partition in topic.partitions.values_mut() {
                // Remove messages older than MAX_MESSAGE_AGE
                while let Some(front) = partition.records.front() {
                    if now.duration_since(front.timestamp) > MAX_MESSAGE_AGE {
                        if let Some(removed) = partition.records.pop_front() {
                            partition.base_offset = removed.offset + 1;
                        }
                    } else {
                        break;
                    }
                }
                
                // More aggressive VecDeque capacity management to prevent memory bloat
                let len = partition.records.len();
                let cap = partition.records.capacity();
                
                // Shrink if using less than 25% of capacity and capacity is significant
                if cap > 32 && len < cap / 4 {
                    // Shrink to 2x current size to leave some headroom
                    let new_cap = (len * 2).max(16);
                    partition.records.shrink_to(new_cap);
                } else if partition.records.is_empty() && cap > 16 {
                    // If completely empty, shrink to minimal size
                    partition.records.shrink_to_fit();
                }
            }
        }
    }
    
    pub fn clear_all_messages(&mut self) {
        for topic in self.topics.values_mut() {
            for partition in topic.partitions.values_mut() {
                // Clear all messages but keep the offset history
                partition.records.clear();
                partition.records.shrink_to_fit();
                partition.base_offset = partition.next_offset;
            }
        }
    }
    
    pub fn cleanup_empty_topics(&mut self) -> usize {
        let mut empty_topics = Vec::new();
        
        for (topic_name, topic) in self.topics.iter() {
            let mut all_empty = true;
            
            for partition in topic.partitions.values() {
                // A partition is empty if it has no records
                // We don't check next_offset because it keeps the history
                if !partition.records.is_empty() {
                    all_empty = false;
                    break;
                }
            }
            
            if all_empty {
                empty_topics.push(topic_name.clone());
            }
        }
        
        let count = empty_topics.len();
        
        // Remove empty topics
        for topic_name in empty_topics {
            self.topics.remove(&topic_name);
        }
        
        count
    }
    
    pub fn get_storage_stats(&self) -> StorageStats {
        let mut total_messages = 0;
        let total_topics = self.topics.len();
        let mut total_partitions = 0;
        let mut oldest_message_age = None;
        
        for topic in self.topics.values() {
            for partition in topic.partitions.values() {
                total_partitions += 1;
                total_messages += partition.records.len();
                
                // Check oldest message age
                if let Some(first_record) = partition.records.front() {
                    let age = first_record.timestamp.elapsed();
                    oldest_message_age = Some(oldest_message_age.map_or(age, |old: std::time::Duration| old.max(age)));
                }
            }
        }
        
        StorageStats {
            total_messages,
            total_topics,
            total_partitions,
            oldest_message_age,
        }
    }
}

pub struct StorageStats {
    pub total_messages: usize,
    pub total_topics: usize,
    pub total_partitions: usize,
    pub oldest_message_age: Option<std::time::Duration>,
}

#[allow(dead_code)]
impl StorageStats {
    pub fn total_topics(&self) -> usize {
        self.total_topics
    }
}
