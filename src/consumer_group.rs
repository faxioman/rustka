use std::collections::HashMap;
use std::time::{Duration, Instant};
use bytes::{Bytes, BytesMut};
use kafka_protocol::messages::consumer_protocol_assignment::ConsumerProtocolAssignment;
use kafka_protocol::protocol::Encodable;
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Debug, Clone)]
pub struct ConsumerGroup {
    pub _group_id: String,
    pub protocol_type: String,
    pub generation_id: i32,
    pub leader_id: String,
    pub members: HashMap<String, GroupMember>,
    pub state: GroupState,
    pub rebalance_start: Option<Instant>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum GroupState {
    Empty,
    Stable,
    PreparingRebalance,
    AwaitingSync,
}

#[derive(Debug, Clone)]
pub struct GroupMember {
    pub _member_id: String,
    pub _client_id: String,
    pub _client_host: String,
    pub session_timeout_ms: i32,
    pub _rebalance_timeout_ms: i32,
    pub last_heartbeat: Instant,
    pub metadata: Bytes,
    pub assignment: Option<Bytes>,
}

pub struct ConsumerGroupManager {
    groups: HashMap<String, ConsumerGroup>,
    member_to_group: HashMap<String, String>,
    offsets: HashMap<(String, String, i32), i64>,
    next_member_id: i32,
    storage: Option<Arc<Mutex<crate::storage::InMemoryStorage>>>,
}

impl ConsumerGroupManager {
    pub fn new() -> Self {
        Self {
            groups: HashMap::new(),
            member_to_group: HashMap::new(),
            offsets: HashMap::new(),
            next_member_id: 1,
            storage: None,
        }
    }
    
    pub fn new_with_storage(storage: Arc<Mutex<crate::storage::InMemoryStorage>>) -> Self {
        let mut mgr = Self::new();
        mgr.storage = Some(storage);
        mgr
    }

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
        let _is_new_member = member_id.is_none();
        let member_id = member_id.unwrap_or_else(|| self.generate_member_id(&client_id));
        
        let group = self.groups.entry(group_id.clone()).or_insert_with(|| {
            ConsumerGroup {
                _group_id: group_id.clone(),
                protocol_type: protocol_type.clone(),
                generation_id: 0,
                leader_id: String::new(),
                members: HashMap::new(),
                state: GroupState::Empty,
                rebalance_start: None,
            }
        });

        if !group.members.is_empty() && group.protocol_type != protocol_type {
            return Err(GroupError::InconsistentGroupProtocol);
        }

        match group.state {
            GroupState::Empty => {
                // First member joining - becomes leader immediately
                
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
                
                group.members.insert(member_id.clone(), member);
                self.member_to_group.insert(member_id.clone(), group_id.clone());
                
                group.leader_id = member_id.clone();
                group.generation_id = 1;
                group.state = GroupState::AwaitingSync;
                
                // Leader gets the member list (just itself for now)
                Ok(JoinGroupResult {
                    error_code: 0,
                    generation_id: group.generation_id,
                    protocol_name: protocols.first().map(|(name, _)| name.clone()).unwrap_or_default(),
                    leader_id: group.leader_id.clone(),
                    member_id: member_id.clone(),
                    members: vec![(member_id, protocols.first().map(|(_, m)| m.clone()).unwrap_or_default())],
                })
            }
            GroupState::Stable => {
                // New member joining - trigger rebalance
                
                group.state = GroupState::PreparingRebalance;
                group.generation_id += 1;
                group.rebalance_start = Some(Instant::now());
                
                // Clear all assignments and members to force everyone to rejoin
                group.members.clear();
                
                // Add this new member
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
                
                group.members.insert(member_id.clone(), member);
                self.member_to_group.insert(member_id.clone(), group_id.clone());
                
                // Return error to wait for more members
                Err(GroupError::RebalanceInProgress)
            }
            GroupState::PreparingRebalance => {
                
                // Set rebalance start time if not set
                if group.rebalance_start.is_none() {
                    group.rebalance_start = Some(Instant::now());
                }
                
                // Add or update member
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
                
                group.members.insert(member_id.clone(), member);
                self.member_to_group.insert(member_id.clone(), group_id.clone());
                
                // Wait 1 second to collect joining members before completing rebalance
                // This balances responsiveness with avoiding multiple rapid rebalances
                let elapsed = group.rebalance_start
                    .map(|start| start.elapsed())
                    .unwrap_or(Duration::from_secs(0));
                
                let should_complete = elapsed > Duration::from_millis(1000);
                
                if should_complete {
                    
                    // Elect leader (first member alphabetically)
                    if let Some(leader_id) = group.members.keys().min().cloned() {
                        group.leader_id = leader_id.clone();
                    }
                    
                    group.state = GroupState::AwaitingSync;
                    group.rebalance_start = None;
                    
                    // Return success to all members
                    Ok(JoinGroupResult {
                        error_code: 0,
                        generation_id: group.generation_id,
                        protocol_name: protocols.first().map(|(name, _)| name.clone()).unwrap_or_default(),
                        leader_id: group.leader_id.clone(),
                        member_id: member_id.clone(),
                        members: if member_id == group.leader_id {
                            // Leader gets full member list
                            group.members.iter().map(|(id, m)| {
                                (id.clone(), m.metadata.clone())
                            }).collect()
                        } else {
                            // Others get empty list
                            vec![]
                        },
                    })
                } else {
                    // Still waiting for members
                    Err(GroupError::RebalanceInProgress)
                }
            }
            GroupState::AwaitingSync => {
                // Check if this is an existing member rejoining
                if group.members.contains_key(&member_id) {
                    // Return the join result again
                    Ok(JoinGroupResult {
                        error_code: 0,
                        generation_id: group.generation_id,
                        protocol_name: protocols.first().map(|(name, _)| name.clone()).unwrap_or_default(),
                        leader_id: group.leader_id.clone(),
                        member_id: member_id.clone(),
                        members: if member_id == group.leader_id {
                            // Leader gets full member list
                            group.members.iter().map(|(id, m)| {
                                (id.clone(), m.metadata.clone())
                            }).collect()
                        } else {
                            // Others get empty list
                            vec![]
                        },
                    })
                } else {
                    // New member during sync - need to restart rebalance
                    group.state = GroupState::PreparingRebalance;
                    group.generation_id += 1;
                    group.rebalance_start = Some(Instant::now());
                    group.members.clear();
                    
                    // Add this new member
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
                    
                    group.members.insert(member_id.clone(), member);
                    self.member_to_group.insert(member_id.clone(), group_id.clone());
                    
                    Err(GroupError::RebalanceInProgress)
                }
            }
        }
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

        if !group.members.contains_key(member_id) {
            return Err(GroupError::UnknownMember);
        }


        // Leader provides assignments for ALL members
        if member_id == group.leader_id && !assignments.is_empty() {
            for (mid, assignment) in assignments {
                if let Some(member) = group.members.get_mut(&mid) {
                    member.assignment = Some(assignment);
                }
            }
            
            group.state = GroupState::Stable;
        }

        let member = group.members.get(member_id).unwrap();
        Ok(member.assignment.clone().unwrap_or_else(create_empty_assignment))
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

        // During rebalance, return error to force rejoin
        if group.state == GroupState::PreparingRebalance {
            return Err(GroupError::RebalanceInProgress);
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
        let should_remove = {
            let group = self.groups.get_mut(group_id)
                .ok_or(GroupError::UnknownGroup)?;

            group.members.remove(member_id);
            self.member_to_group.remove(member_id);

            if group.members.is_empty() {
                true
            } else {
                // Trigger rebalance
                group.state = GroupState::PreparingRebalance;
                group.generation_id += 1;
                false
            }
        };

        if should_remove {
            self.groups.remove(group_id);
        }

        Ok(())
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

        for (topic, partition, offset) in offsets {
            self.offsets.insert((group_id.to_string(), topic.clone(), partition), offset);
            
            // Write to __commit_log for Snuba compatibility
            if let Some(storage) = &self.storage {
                let key = create_commit_log_key(group_id, &topic, partition);
                let value = create_commit_log_value(offset);
                
                let mut storage = storage.lock().await;
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
                    .unwrap_or(-1);
                (partition, offset)
            }).collect();
            (topic, partition_offsets)
        }).collect()
    }

    pub fn check_expired_members(&mut self) {
        let now = Instant::now();
        let mut expired_members = Vec::new();

        for (group_id, group) in &self.groups {
            if group.state == GroupState::Stable {
                for (member_id, member) in &group.members {
                    let timeout = Duration::from_millis(member.session_timeout_ms as u64);
                    if now.duration_since(member.last_heartbeat) > timeout {
                        expired_members.push((group_id.clone(), member_id.clone()));
                    }
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
    pub members: Vec<(String, Bytes)>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TopicPartition {
    pub topic: String,
    pub partition: i32,
}

#[derive(Debug, Clone)]
pub enum GroupError {
    UnknownGroup,
    UnknownMember,
    IllegalGeneration,
    RebalanceInProgress,
    InconsistentGroupProtocol,
}

fn create_empty_assignment() -> Bytes {
    let assignment = ConsumerProtocolAssignment::default()
        .with_assigned_partitions(vec![])
        .with_user_data(Some(Bytes::new()));
    
    let mut final_buf = BytesMut::new();
    final_buf.extend_from_slice(&[0, 0]); // version 0
    
    let mut assignment_buf = BytesMut::new();
    if assignment.encode(&mut assignment_buf, 0).is_ok() {
        final_buf.extend_from_slice(&assignment_buf);
    } else {
        final_buf.extend_from_slice(&[0, 0, 0, 0]); // 0 partitions
        final_buf.extend_from_slice(&[0, 0, 0, 0]); // 0 user data
    }
    
    final_buf.freeze()
}

fn create_commit_log_key(group_id: &str, topic: &str, partition: i32) -> Bytes {
    let mut buf = BytesMut::new();
    
    // Version (2 bytes)
    buf.extend_from_slice(&1u16.to_be_bytes());
    
    // Group ID length (2 bytes) + Group ID
    buf.extend_from_slice(&(group_id.len() as u16).to_be_bytes());
    buf.extend_from_slice(group_id.as_bytes());
    
    // Topic length (2 bytes) + Topic
    buf.extend_from_slice(&(topic.len() as u16).to_be_bytes());
    buf.extend_from_slice(topic.as_bytes());
    
    // Partition (4 bytes)
    buf.extend_from_slice(&partition.to_be_bytes());
    
    buf.freeze()
}

fn create_commit_log_value(offset: i64) -> Bytes {
    let mut buf = BytesMut::new();
    
    // Version (2 bytes)
    buf.extend_from_slice(&1u16.to_be_bytes());
    
    // Offset (8 bytes)
    buf.extend_from_slice(&offset.to_be_bytes());
    
    // Metadata length (2 bytes) + Metadata (empty)
    buf.extend_from_slice(&0u16.to_be_bytes());
    
    // Timestamp (8 bytes) - current time in milliseconds
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    buf.extend_from_slice(&timestamp.to_be_bytes());
    
    buf.freeze()
}