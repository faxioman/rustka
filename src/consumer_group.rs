use std::collections::HashMap;
use std::time::{Duration, Instant};
use bytes::{Bytes, BytesMut};
use kafka_protocol::messages::{
    consumer_protocol_subscription::ConsumerProtocolSubscription,
    consumer_protocol_assignment::{ConsumerProtocolAssignment, TopicPartition as AssignmentTopicPartition},
    TopicName,
};
use kafka_protocol::protocol::{Decodable, Encodable, StrBytes};
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Debug, Clone)]
pub struct ConsumerGroup {
    pub _group_id: String,  // Kept for debugging/future use
    pub protocol_type: String,
    pub generation_id: i32,
    pub leader_id: String,
    pub members: HashMap<String, GroupMember>,
    pub _assignments: HashMap<String, Vec<TopicPartition>>,  // Currently unused, kept for future use
}

#[derive(Debug, Clone)]
pub struct GroupMember {
    pub _member_id: String,  // Redundant with HashMap key, kept for debugging
    pub _client_id: String,
    pub _client_host: String,
    pub session_timeout_ms: i32,
    pub _rebalance_timeout_ms: i32,
    pub last_heartbeat: Instant,
    pub metadata: Bytes,
    pub assignment: Option<Bytes>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TopicPartition {
    pub topic: String,
    pub partition: i32,
}

pub struct ConsumerGroupManager {
    groups: HashMap<String, ConsumerGroup>,
    member_to_group: HashMap<String, String>,
    offsets: HashMap<(String, String, i32), i64>, // (group_id, topic, partition) -> offset
    next_member_id: i32,
    storage: Option<Arc<Mutex<crate::storage::InMemoryStorage>>>, // For writing to __commit_log
}

fn parse_consumer_metadata(metadata: &Bytes) -> Vec<String> {
    if metadata.len() < 2 {
        return Vec::new();
    }
    
    // Skip version (2 bytes) that kafka-python includes but kafka-protocol-rs doesn't expect
    let mut buf = BytesMut::from(&metadata[2..]);
    
    match ConsumerProtocolSubscription::decode(&mut buf, 0) {
        Ok(subscription) => {
            subscription.topics.into_iter()
                .map(|t| t.to_string())
                .collect()
        }
        Err(_) => Vec::new(),
    }
}

fn create_consumer_assignment(assignments: &[(String, Vec<i32>)]) -> Bytes {
    let mut topic_partitions = Vec::new();
    
    // Group partitions by topic
    for (topic, partitions) in assignments {
        let topic_partition = AssignmentTopicPartition::default()
            .with_topic(TopicName::from(StrBytes::from(topic.clone())))
            .with_partitions(partitions.clone());
        topic_partitions.push(topic_partition);
    }
    
    let assignment = ConsumerProtocolAssignment::default()
        .with_assigned_partitions(topic_partitions)
        .with_user_data(Some(Bytes::new())); // Empty user data
    
    // kafka-python expects version prefix, but kafka-protocol-rs doesn't include it
    let mut final_buf = BytesMut::new();
    
    // Add version (Int16)
    final_buf.extend_from_slice(&[0, 0]); // version 0
    
    // Encode the assignment
    let mut assignment_buf = BytesMut::new();
    match assignment.encode(&mut assignment_buf, 0) {
        Ok(_) => {
            // Append the encoded assignment after the version
            final_buf.extend_from_slice(&assignment_buf);
        }
        Err(_) => {
            // Fallback: create minimal valid structure
            final_buf.clear();
            final_buf.extend_from_slice(&[0, 0]); // version 0
            final_buf.extend_from_slice(&[0, 0, 0, 0]); // 0 partitions
            final_buf.extend_from_slice(&[0, 0, 0, 0]); // 0 user data
        }
    }
    
    final_buf.freeze()
}

impl Default for ConsumerGroupManager {
    fn default() -> Self {
        Self {
            groups: HashMap::new(),
            member_to_group: HashMap::new(),
            offsets: HashMap::new(),
            next_member_id: 1,
            storage: None,
        }
    }
}

impl ConsumerGroupManager {
    pub fn new_with_storage(storage: Arc<Mutex<crate::storage::InMemoryStorage>>) -> Self {
        Self {
            groups: HashMap::new(),
            member_to_group: HashMap::new(),
            offsets: HashMap::new(),
            next_member_id: 1,
            storage: Some(storage),
        }
    }
}

impl ConsumerGroupManager {
    pub fn generate_member_id(&mut self, client_id: &str) -> String {
        let member_id = format!("{}-{}", client_id, self.next_member_id);
        self.next_member_id += 1;
        member_id
    }


    #[allow(clippy::too_many_arguments)]
    pub fn join_group(
        &mut self,
        group_id: String,
        member_id: Option<String>,
        client_id: String,
        client_host: String,
        session_timeout_ms: i32,
        rebalance_timeout_ms: i32,
        protocol_type: String,
        protocols: Vec<(String, Bytes)>,
    ) -> Result<JoinGroupResult, GroupError> {
        let member_id = member_id.unwrap_or_else(|| self.generate_member_id(&client_id));
        
        let group = self.groups.entry(group_id.clone()).or_insert_with(|| {
            ConsumerGroup {
                _group_id: group_id.clone(),
                protocol_type: protocol_type.clone(),
                generation_id: 0,
                leader_id: String::new(),
                members: HashMap::new(),
                _assignments: HashMap::new(),
            }
        });

        if !group.members.is_empty() && group.protocol_type != protocol_type {
            return Err(GroupError::InconsistentGroupProtocol);
        }

        let member = GroupMember {
            _member_id: member_id.clone(),
            _client_id: client_id,
            _client_host: client_host,
            session_timeout_ms,
            _rebalance_timeout_ms: rebalance_timeout_ms,
            last_heartbeat: Instant::now(),
            metadata: protocols.first().map(|(_, m)| m.clone()).unwrap_or_default(),
            assignment: None,
        };

        let is_new_member = !group.members.contains_key(&member_id);
        group.members.insert(member_id.clone(), member);
        self.member_to_group.insert(member_id.clone(), group_id.clone());

        if is_new_member {
            group.generation_id += 1;
            if group.leader_id.is_empty() {
                group.leader_id = member_id.clone();
            }
        }

        Ok(JoinGroupResult {
            error_code: 0,
            generation_id: group.generation_id,
            protocol_name: protocols.first().map(|(name, _)| name.clone()).unwrap_or_default(),
            leader_id: group.leader_id.clone(),
            member_id: member_id.clone(),
            members: if member_id == group.leader_id {
                // Only leader receives member list
                group.members.iter().map(|(id, m)| {
                    (id.clone(), m.metadata.clone())
                }).collect()
            } else {
                vec![]
            },
        })
    }

    pub fn sync_group(
        &mut self,
        group_id: &str,
        generation_id: i32,
        member_id: &str,
        assignments: HashMap<String, Bytes>,
    ) -> Result<Bytes, GroupError> {
        let group = self.groups.get_mut(group_id)
            .ok_or(GroupError::UnknownGroup)?;

        if group.generation_id != generation_id {
            return Err(GroupError::IllegalGeneration);
        }

        // SIMPLIFIED LOGIC for single-node Rustka:
        // 1. If leader has explicit assignments, save them
        // 2. Otherwise, auto-assign ALL partitions to EACH member for their topics
        
        if member_id == group.leader_id && !assignments.is_empty() {
            // Leader provided explicit assignments
            for (mid, assignment) in assignments {
                if let Some(member) = group.members.get_mut(&mid) {
                    member.assignment = Some(assignment);
                }
            }
        }
        
        // Ensure EVERY member gets an assignment (avoid race conditions)
        for (_mid, member) in group.members.iter_mut() {
            if member.assignment.is_none() {
                let topics = parse_consumer_metadata(&member.metadata);
                if !topics.is_empty() {
                    
                    // In single-node Rustka, just give ALL partitions (0,1,2) to each member
                    let mut topic_partitions: Vec<(String, Vec<i32>)> = Vec::new();
                    for topic in topics {
                        topic_partitions.push((topic, vec![0, 1, 2]));
                    }
                    
                    member.assignment = Some(create_consumer_assignment(&topic_partitions));
                }
            }
        }

        let member = group.members.get(member_id)
            .ok_or(GroupError::UnknownMember)?;

        // Always ensure we have a valid assignment
        let assignment = if let Some(ref assignment) = member.assignment {
            assignment.clone()
        } else {
            // Return valid empty assignment for kafka-python compatibility
            create_consumer_assignment(&[])
        };

        Ok(assignment)
    }

    pub fn heartbeat(
        &mut self,
        group_id: &str,
        generation_id: i32,
        member_id: &str,
    ) -> Result<(), GroupError> {
        let group = self.groups.get_mut(group_id)
            .ok_or(GroupError::UnknownGroup)?;

        if group.generation_id != generation_id {
            return Err(GroupError::IllegalGeneration);
        }

        let member = group.members.get_mut(member_id)
            .ok_or(GroupError::UnknownMember)?;

        member.last_heartbeat = Instant::now();
        Ok(())
    }

    pub fn leave_group(
        &mut self,
        group_id: &str,
        member_id: &str,
    ) -> Result<(), GroupError> {
        let group = self.groups.get_mut(group_id)
            .ok_or(GroupError::UnknownGroup)?;

        group.members.remove(member_id);
        self.member_to_group.remove(member_id);

        // If there are no more members, remove the group
        if group.members.is_empty() {
            self.groups.remove(group_id);
        } else {
            group.generation_id += 1;
            if group.leader_id == member_id {
                if let Some(new_leader) = group.members.keys().next() {
                    group.leader_id = new_leader.clone();
                }
            }
        }

        Ok(())
    }

    fn create_commit_log_key(group_id: &str, topic: &str, partition: i32) -> Bytes {
        let mut buf = BytesMut::new();
        
        // Version (2 bytes)
        buf.extend_from_slice(&[0, 1]);
        
        // Group ID length (2 bytes) + Group ID
        let group_bytes = group_id.as_bytes();
        buf.extend_from_slice(&(group_bytes.len() as u16).to_be_bytes());
        buf.extend_from_slice(group_bytes);
        
        // Topic length (2 bytes) + Topic
        let topic_bytes = topic.as_bytes();
        buf.extend_from_slice(&(topic_bytes.len() as u16).to_be_bytes());
        buf.extend_from_slice(topic_bytes);
        
        // Partition (4 bytes)
        buf.extend_from_slice(&partition.to_be_bytes());
        
        buf.freeze()
    }
    
    fn create_commit_log_value(offset: i64, metadata: &str, timestamp: i64) -> Bytes {
        let mut buf = BytesMut::new();
        
        // Version (2 bytes)
        buf.extend_from_slice(&[0, 1]);
        
        // Offset (8 bytes)
        buf.extend_from_slice(&offset.to_be_bytes());
        
        // Metadata length (2 bytes) + Metadata
        let metadata_bytes = metadata.as_bytes();
        buf.extend_from_slice(&(metadata_bytes.len() as u16).to_be_bytes());
        buf.extend_from_slice(metadata_bytes);
        
        // Timestamp (8 bytes)
        buf.extend_from_slice(&timestamp.to_be_bytes());
        
        buf.freeze()
    }

    pub async fn commit_offsets(
        &mut self,
        group_id: &str,
        _generation_id: i32,
        offsets: Vec<(String, i32, i64)>,
    ) -> Result<(), GroupError> {
        if !self.groups.contains_key(group_id) {
            return Err(GroupError::UnknownGroup);
        }

        // Get current timestamp
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        // Update offsets in memory
        for (topic, partition, offset) in &offsets {
            self.offsets.insert((group_id.to_string(), topic.clone(), *partition), *offset);
        }
        
        // Write to __commit_log if storage is available
        if let Some(storage) = &self.storage {
            let mut storage = storage.lock().await;
            
            for (topic, partition, offset) in offsets {
                let key = Self::create_commit_log_key(group_id, &topic, partition);
                let value = Self::create_commit_log_value(offset, "", timestamp);
                
                // Append to __commit_log topic with key and value properly separated
                storage.append_records("__commit_log", 0, Some(key), value);
            }
        }

        Ok(())
    }

    pub fn fetch_offsets(
        &self,
        group_id: &str,
        topics: Vec<(String, Vec<i32>)>,
    ) -> Vec<(String, Vec<(i32, i64)>)> {
        topics.into_iter().map(|(topic, partitions)| {
            let partition_offsets = partitions.into_iter().map(|partition| {
                let offset = self.offsets
                    .get(&(group_id.to_string(), topic.clone(), partition))
                    .copied()
                    .unwrap_or(-1); // -1 means no committed offset
                (partition, offset)
            }).collect();
            (topic, partition_offsets)
        }).collect()
    }

    pub fn check_expired_members(&mut self) {
        let now = Instant::now();
        let mut expired_members = Vec::new();

        for (group_id, group) in &self.groups {
            for (member_id, member) in &group.members {
                let timeout = Duration::from_millis(member.session_timeout_ms as u64);
                if now.duration_since(member.last_heartbeat) > timeout {
                    expired_members.push((group_id.clone(), member_id.clone()));
                }
            }
        }

        for (group_id, member_id) in expired_members {
            let _ = self.leave_group(&group_id, &member_id);
        }
    }
}

#[derive(Debug)]
pub struct JoinGroupResult {
    pub error_code: i16,
    pub generation_id: i32,
    pub protocol_name: String,
    pub leader_id: String,
    pub member_id: String,
    pub members: Vec<(String, Bytes)>, // member_id -> metadata
}

#[derive(Debug, Clone)]
pub enum GroupError {
    UnknownGroup,
    UnknownMember,
    IllegalGeneration,
    InconsistentGroupProtocol,
}
