use std::sync::Arc;
use tokio::sync::RwLock;
use serde::{Serialize, Deserialize};
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BrokerMetrics {
    pub total_connections: u64,
    pub active_connections: u64,
    pub total_requests: u64,
    pub total_messages_produced: u64,
    pub total_messages_fetched: u64,
    pub topics_count: usize,
    pub consumer_groups_count: usize,
    pub uptime_seconds: u64,
    pub start_time: u64,
    pub storage_total_messages: usize,
    pub storage_total_partitions: usize,
    pub storage_oldest_message_seconds: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicMetrics {
    pub name: String,
    pub partitions: Vec<PartitionMetrics>,
    pub total_messages: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionMetrics {
    pub id: i32,
    pub high_watermark: i64,
    pub message_count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumerGroupMetrics {
    pub group_id: String,
    pub state: String,
    pub generation_id: i32,
    pub members_count: usize,
    pub leader_id: String,
}

pub struct MetricsCollector {
    pub broker_metrics: Arc<RwLock<BrokerMetrics>>,
    pub topic_metrics: Arc<RwLock<HashMap<String, TopicMetrics>>>,
    pub consumer_group_metrics: Arc<RwLock<HashMap<String, ConsumerGroupMetrics>>>,
}

impl MetricsCollector {
    pub fn new() -> Self {
        let start_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
            
        Self {
            broker_metrics: Arc::new(RwLock::new(BrokerMetrics {
                total_connections: 0,
                active_connections: 0,
                total_requests: 0,
                total_messages_produced: 0,
                total_messages_fetched: 0,
                topics_count: 0,
                consumer_groups_count: 0,
                uptime_seconds: 0,
                start_time,
                storage_total_messages: 0,
                storage_total_partitions: 0,
                storage_oldest_message_seconds: None,
            })),
            topic_metrics: Arc::new(RwLock::new(HashMap::new())),
            consumer_group_metrics: Arc::new(RwLock::new(HashMap::new())),
        }
    }
    
    pub async fn increment_connections(&self) {
        let mut metrics = self.broker_metrics.write().await;
        metrics.total_connections += 1;
        metrics.active_connections += 1;
    }
    
    pub async fn decrement_connections(&self) {
        let mut metrics = self.broker_metrics.write().await;
        if metrics.active_connections > 0 {
            metrics.active_connections -= 1;
        }
    }
    
    pub async fn increment_requests(&self) {
        let mut metrics = self.broker_metrics.write().await;
        metrics.total_requests += 1;
    }
    
    pub async fn increment_messages_produced(&self, count: u64) {
        let mut metrics = self.broker_metrics.write().await;
        metrics.total_messages_produced += count;
    }
    
    pub async fn increment_messages_fetched(&self, count: u64) {
        let mut metrics = self.broker_metrics.write().await;
        metrics.total_messages_fetched += count;
    }
    
    pub async fn update_topic_metrics(&self, topic: String, partition: i32, high_watermark: i64) {
        let mut topics = self.topic_metrics.write().await;
        let topic_metrics = topics.entry(topic.clone()).or_insert_with(|| {
            TopicMetrics {
                name: topic,
                partitions: Vec::new(),
                total_messages: 0,
            }
        });
        
        // Update or add partition metrics
        if let Some(part) = topic_metrics.partitions.iter_mut().find(|p| p.id == partition) {
            part.high_watermark = high_watermark;
            part.message_count = high_watermark as u64;
        } else {
            topic_metrics.partitions.push(PartitionMetrics {
                id: partition,
                high_watermark,
                message_count: high_watermark as u64,
            });
        }
        
        // Update total messages
        topic_metrics.total_messages = topic_metrics.partitions
            .iter()
            .map(|p| p.message_count)
            .sum();
    }
    
    pub async fn update_consumer_group_metrics(
        &self,
        group_id: String,
        state: String,
        generation_id: i32,
        members_count: usize,
        leader_id: String,
    ) {
        let mut groups = self.consumer_group_metrics.write().await;
        groups.insert(group_id.clone(), ConsumerGroupMetrics {
            group_id,
            state,
            generation_id,
            members_count,
            leader_id,
        });
    }
    
    pub async fn update_storage_stats(&self, stats: crate::storage::StorageStats) {
        let mut metrics = self.broker_metrics.write().await;
        metrics.storage_total_messages = stats.total_messages;
        metrics.storage_total_partitions = stats.total_partitions;
        metrics.storage_oldest_message_seconds = stats.oldest_message_age.map(|d| d.as_secs());
    }
    
    pub async fn update_counts(&self) {
        let mut metrics = self.broker_metrics.write().await;
        metrics.topics_count = self.topic_metrics.read().await.len();
        metrics.consumer_groups_count = self.consumer_group_metrics.read().await.len();
        
        // Update uptime
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        metrics.uptime_seconds = now - metrics.start_time;
    }
    
    pub async fn get_all_metrics(&self) -> AllMetrics {
        self.update_counts().await;
        
        AllMetrics {
            broker: self.broker_metrics.read().await.clone(),
            topics: self.topic_metrics.read().await.clone(),
            consumer_groups: self.consumer_group_metrics.read().await.clone(),
        }
    }
    
    pub async fn cleanup_topic_metrics(&self, existing_topics: &[String]) {
        let mut topics = self.topic_metrics.write().await;
        
        // Remove any topics that no longer exist in storage
        topics.retain(|topic_name, _| existing_topics.contains(topic_name));
    }
    
    pub async fn cleanup_consumer_group_metrics(&self, existing_groups: &[String]) {
        let mut groups = self.consumer_group_metrics.write().await;
        
        // Remove any consumer groups that no longer exist
        groups.retain(|group_id, _| existing_groups.contains(group_id));
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AllMetrics {
    pub broker: BrokerMetrics,
    pub topics: HashMap<String, TopicMetrics>,
    pub consumer_groups: HashMap<String, ConsumerGroupMetrics>,
}