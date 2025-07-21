use std::collections::HashMap;
use std::time::{Duration, Instant};
use bytes::{Bytes, BytesMut};
use kafka_protocol::messages::consumer_protocol_assignment::ConsumerProtocolAssignment;
use kafka_protocol::protocol::Encodable;
use std::sync::Arc;
use tokio::sync::Mutex;
use crate::metrics::MetricsCollector;
use tracing::debug;

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
    offsets: HashMap<(String, String, i32), (i64, Instant)>, // offset, last_commit_time
    next_member_id: i32,
    storage: Option<Arc<Mutex<crate::storage::InMemoryStorage>>>,
    metrics: Option<Arc<MetricsCollector>>,
    // Configuration
    offset_retention_ms: u64,  // Default: 3600000 (1 hour)
    empty_group_retention_ms: u64,  // Default: 600000 (10 minutes)
}

impl ConsumerGroupManager {
    pub fn new() -> Self {
        Self {
            groups: HashMap::new(),
            member_to_group: HashMap::new(),
            offsets: HashMap::new(),
            next_member_id: 1,
            storage: None,
            metrics: None,
            offset_retention_ms: 3600000,  // 1 hour
            empty_group_retention_ms: 600000,  // 10 minutes
        }
    }
    
    pub fn new_with_storage(storage: Arc<Mutex<crate::storage::InMemoryStorage>>) -> Self {
        let mut mgr = Self::new();
        mgr.storage = Some(storage);
        mgr
    }
    
    pub fn set_metrics(&mut self, metrics: Arc<MetricsCollector>) {
        self.metrics = Some(metrics);
    }
    
    /// Set retention times for testing
    pub fn set_retention_times(&mut self, offset_retention_ms: u64, empty_group_retention_ms: u64) {
        self.offset_retention_ms = offset_retention_ms;
        self.empty_group_retention_ms = empty_group_retention_ms;
    }
    
    /// Force cleanup of all empty groups and their offsets (for testing/admin)
    pub fn force_cleanup_empty_groups(&mut self) {
        let groups_to_remove: Vec<String> = self.groups.iter()
            .filter_map(|(group_id, group)| {
                if group.members.is_empty() {
                    Some(group_id.clone())
                } else {
                    None
                }
            })
            .collect();
        
        // Remove all empty groups
        for group_id in &groups_to_remove {
            self.groups.remove(group_id);
        }
        
        // Remove offsets for removed groups
        self.offsets.retain(|(g, _, _), _| {
            !groups_to_remove.contains(g)
        });
        
        debug!("Force cleanup: removed {} empty groups and their offsets", groups_to_remove.len());
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
        let is_new_member = member_id.is_none() || member_id.as_ref().map_or(false, |id| id.is_empty());
        let member_id = if is_new_member {
            self.generate_member_id(&client_id)
        } else {
            member_id.unwrap()
        };
        
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
                // First member joining - start rebalance immediately
                debug!("First member {} joining empty group {}", member_id, group_id);
                
                group.state = GroupState::PreparingRebalance;
                group.generation_id = 1;
                group.rebalance_start = Some(Instant::now());
                
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
                
                // For a single member, complete rebalance immediately
                group.leader_id = member_id.clone();
                group.state = GroupState::AwaitingSync;
                group.rebalance_start = None;
                
                // Update metrics
                if let Some(metrics) = &self.metrics {
                    let runtime = tokio::runtime::Handle::try_current();
                    if let Ok(handle) = runtime {
                        let metrics = metrics.clone();
                        let group_id_clone = group_id.clone();
                        let leader_id_clone = member_id.clone();
                        handle.spawn(async move {
                            metrics.update_consumer_group_metrics(
                                group_id_clone, 
                                "AwaitingSync".to_string(),
                                1,
                                1,
                                leader_id_clone
                            ).await;
                        });
                    }
                }
                
                // Leader gets the full member list
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
                debug!("Member {} attempting to join stable group {}", member_id, group_id);
                
                // Trigger rebalance for any join during stable state
                group.state = GroupState::PreparingRebalance;
                group.generation_id += 1;
                group.rebalance_start = Some(Instant::now());
                
                // CRITICAL: Do NOT clear existing members!
                // Mark all existing members as needing to rejoin
                for member in group.members.values_mut() {
                    member.assignment = None;
                }
                
                // Add or update the joining member
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
                
                // Return REBALANCE_IN_PROGRESS to trigger client rejoin
                Err(GroupError::RebalanceInProgress)
            }
            GroupState::PreparingRebalance => {
                debug!("Member {} joining group {} in PreparingRebalance state", member_id, group_id);
                
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
                
                // Check if we should complete rebalance
                let elapsed = group.rebalance_start
                    .map(|start| start.elapsed())
                    .unwrap_or(Duration::from_secs(0));
                
                // Adaptive rebalance timing based on group size
                let (min_wait, max_wait) = if group.members.len() > 50 {
                    // Many members - complete quickly to avoid accumulating more
                    (Duration::from_millis(10), Duration::from_millis(50))
                } else if group.members.len() > 10 {
                    // Medium group - moderate wait
                    (Duration::from_millis(30), Duration::from_millis(150))
                } else {
                    // Small group - normal wait times
                    (Duration::from_millis(50), Duration::from_millis(300))
                };
                
                // Complete if:
                // 1. Maximum wait time exceeded
                // 2. Minimum wait time passed AND we have at least one member
                let should_complete = elapsed >= max_wait || 
                                    (elapsed >= min_wait && group.members.len() >= 1);
                
                if should_complete {
                    debug!("Completing rebalance for group {} with {} members", group_id, group.members.len());
                    
                    // Elect leader (first member by ID for determinism)
                    if let Some(leader_id) = group.members.keys().min().cloned() {
                        group.leader_id = leader_id.clone();
                    }
                    
                    group.state = GroupState::AwaitingSync;
                    group.rebalance_start = None;
                    
                    // All members that made it into this rebalance get success
                    Ok(JoinGroupResult {
                        error_code: 0,
                        generation_id: group.generation_id,
                        protocol_name: protocols.first().map(|(name, _)| name.clone()).unwrap_or_default(),
                        leader_id: group.leader_id.clone(),
                        member_id: member_id.clone(),
                        members: if member_id == group.leader_id {
                            // Only leader gets full member list per Kafka protocol
                            group.members.iter().map(|(id, m)| {
                                (id.clone(), m.metadata.clone())
                            }).collect()
                        } else {
                            // Non-leaders get empty list
                            vec![]
                        },
                    })
                } else {
                    // Still collecting members
                    debug!("Still waiting for more members to join group {}", group_id);
                    Err(GroupError::RebalanceInProgress)
                }
            }
            GroupState::AwaitingSync => {
                debug!("Member {} attempting to join group {} in AwaitingSync state", member_id, group_id);
                
                // If this member is part of current generation, return success
                if group.members.contains_key(&member_id) {
                    debug!("Member {} is already in current generation", member_id);
                    Ok(JoinGroupResult {
                        error_code: 0,
                        generation_id: group.generation_id,
                        protocol_name: protocols.first().map(|(name, _)| name.clone()).unwrap_or_default(),
                        leader_id: group.leader_id.clone(),
                        member_id: member_id.clone(),
                        members: if member_id == group.leader_id {
                            group.members.iter().map(|(id, m)| {
                                (id.clone(), m.metadata.clone())
                            }).collect()
                        } else {
                            vec![]
                        },
                    })
                } else {
                    // New member trying to join - must trigger new rebalance
                    debug!("New member {} triggering rebalance", member_id);
                    group.state = GroupState::PreparingRebalance;
                    group.generation_id += 1;
                    group.rebalance_start = Some(Instant::now());
                    
                    // Start fresh with just this member
                    group.members.clear();
                    
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
        debug!("SyncGroup for member {} in group {} gen {}", member_id, group_id, generation_id);
        
        let group = self.groups.get_mut(group_id)
            .ok_or(GroupError::UnknownGroup)?;

        if group.generation_id != generation_id {
            debug!("Generation mismatch: expected {} got {}", group.generation_id, generation_id);
            return Err(GroupError::IllegalGeneration);
        }

        if !group.members.contains_key(member_id) {
            debug!("Unknown member {} in group {}", member_id, group_id);
            return Err(GroupError::UnknownMember);
        }

        // Only accept sync in AwaitingSync state
        if group.state != GroupState::AwaitingSync {
            debug!("Group {} not in AwaitingSync state, current state: {:?}", group_id, group.state);
            return Err(GroupError::RebalanceInProgress);
        }

        // Leader provides assignments for ALL members
        if member_id == group.leader_id && !assignments.is_empty() {
            debug!("Leader {} providing assignments for {} members", member_id, assignments.len());
            
            for (mid, assignment) in assignments {
                if let Some(member) = group.members.get_mut(&mid) {
                    member.assignment = Some(assignment);
                    debug!("Assigned partitions to member {}", mid);
                }
            }
            
            group.state = GroupState::Stable;
            
            // Update metrics when group becomes stable
            if let Some(metrics) = &self.metrics {
                let runtime = tokio::runtime::Handle::try_current();
                if let Ok(handle) = runtime {
                    let metrics = metrics.clone();
                    let group_id_clone = group_id.to_string();
                    let member_count = group.members.len();
                    let generation_id = group.generation_id;
                    let leader_id = group.leader_id.clone();
                    handle.spawn(async move {
                        metrics.update_consumer_group_metrics(
                            group_id_clone, 
                            "Stable".to_string(),
                            generation_id,
                            member_count,
                            leader_id
                        ).await;
                    });
                }
            }
        }

        // Wait for leader to provide assignments if we're not the leader
        if member_id != group.leader_id && group.state == GroupState::AwaitingSync {
            // Check if we have an assignment yet
            let member = group.members.get(member_id).unwrap();
            if member.assignment.is_none() {
                debug!("Member {} waiting for assignment from leader", member_id);
                // Return empty assignment for now - client will retry
                return Ok(create_empty_assignment());
            }
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

        // During rebalance, return error to force rejoin
        if group.state == GroupState::PreparingRebalance {
            debug!("Heartbeat during rebalance for member {} in group {}", member_id, group_id);
            return Err(GroupError::RebalanceInProgress);
        }

        if group.generation_id != generation_id {
            debug!("Heartbeat with wrong generation: {} vs {}", generation_id, group.generation_id);
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
            // Remove the group but keep offsets for a while
            // They will be cleaned up by cleanup_orphaned_offsets() after retention expires
            debug!("Group {} is now empty, removing group (keeping offsets temporarily)", group_id);
            self.groups.remove(group_id);
            
            // Update metrics to reflect group removal
            if let Some(metrics) = &self.metrics {
                let runtime = tokio::runtime::Handle::try_current();
                if let Ok(handle) = runtime {
                    let metrics = metrics.clone();
                    let group_id_clone = group_id.to_string();
                    handle.spawn(async move {
                        // Remove group from metrics
                        let mut groups = metrics.consumer_group_metrics.write().await;
                        groups.remove(&group_id_clone);
                    });
                }
            }
        }

        Ok(())
    }

    pub async fn commit_offsets(
        &mut self,
        group_id: &str,
        _generation_id: i32,
        offsets: Vec<(String, i32, i64)>,
    ) -> Result<(), GroupError> {
        debug!("Committing offsets for group {}: {:?}", group_id, offsets);
        
        if !self.groups.contains_key(group_id) {
            debug!("Creating group {} for offset commit", group_id);
            // Create group if it doesn't exist - this is needed for simple consumers
            self.groups.insert(group_id.to_string(), ConsumerGroup {
                _group_id: group_id.to_string(),
                state: GroupState::Stable,
                generation_id: 0,
                protocol_type: "consumer".to_string(),
                leader_id: "".to_string(),
                members: HashMap::new(),
                rebalance_start: None,
            });
        }

        for (topic, partition, offset) in offsets {
            debug!("Storing offset for {}/{}/{}: {}", group_id, topic, partition, offset);
            self.offsets.insert(
                (group_id.to_string(), topic.clone(), partition), 
                (offset, Instant::now())
            );
            
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
        debug!("Fetching offsets for group {} topics {:?}", group_id, topics);
        
        topics.into_iter().map(|(topic, partitions)| {
            let partition_offsets = partitions.into_iter().map(|partition| {
                let key = (group_id.to_string(), topic.clone(), partition);
                let offset = self.offsets
                    .get(&key)
                    .map(|(offset, _)| *offset)
                    .unwrap_or(-1);
                debug!("Lookup {}/{}/{}: found {}", group_id, topic, partition, offset);
                (partition, offset)
            }).collect();
            (topic, partition_offsets)
        }).collect()
    }
    
    pub fn get_all_offsets_for_group(&self, group_id: &str) -> Vec<(String, Vec<i32>)> {
        let mut topic_partitions: HashMap<String, Vec<i32>> = HashMap::new();
        
        for ((g_id, topic, partition), (_offset, _)) in &self.offsets {
            if g_id == group_id {
                topic_partitions
                    .entry(topic.clone())
                    .or_insert_with(Vec::new)
                    .push(*partition);
            }
        }
        
        topic_partitions.into_iter()
            .map(|(topic, mut partitions)| {
                partitions.sort();
                partitions.dedup();
                (topic, partitions)
            })
            .collect()
    }

    pub fn check_expired_members(&mut self) {
        let now = Instant::now();
        let mut expired_members = Vec::new();

        for (group_id, group) in &self.groups {
            // Check for expired members in ALL states, not just Stable
            // This is critical to prevent phantom members during rebalancing
            for (member_id, member) in &group.members {
                let timeout = Duration::from_millis(member.session_timeout_ms as u64);
                // Use a shorter timeout during rebalancing to clean up faster
                let effective_timeout = if group.state == GroupState::PreparingRebalance {
                    timeout.min(Duration::from_secs(3)) // Max 3 seconds during rebalance
                } else {
                    timeout
                };
                
                if now.duration_since(member.last_heartbeat) > effective_timeout {
                    expired_members.push((group_id.clone(), member_id.clone()));
                    debug!("Member {} expired in group {} (state: {:?})", member_id, group_id, group.state);
                }
            }
        }

        for (group_id, member_id) in expired_members {
            let _ = self.leave_group(&group_id, &member_id);
        }
        
        // Also clean up groups with too many members (likely phantom members)
        let mut groups_to_clean = Vec::new();
        for (group_id, group) in &self.groups {
            if group.members.len() > 100 {  // Suspicious number of members
                debug!("Group {} has {} members - likely has phantom members", group_id, group.members.len());
                groups_to_clean.push(group_id.clone());
            }
        }
        
        // Force rebalance for groups with too many members
        for group_id in groups_to_clean {
            if let Some(group) = self.groups.get_mut(&group_id) {
                if group.state == GroupState::Stable {
                    group.state = GroupState::PreparingRebalance;
                    group.generation_id += 1;
                    group.rebalance_start = Some(Instant::now());
                }
            }
        }
    }
    
    /// Get the number of stored offsets (for monitoring)
    pub fn get_offset_count(&self) -> usize {
        self.offsets.len()
    }
    
    /// Get all consumer group IDs (for metrics cleanup)
    pub fn get_all_group_ids(&self) -> Vec<String> {
        self.groups.keys().cloned().collect()
    }
    
    /// Clean up offsets for groups that no longer exist or old offsets
    /// This prevents unbounded memory growth from offset accumulation
    pub fn cleanup_orphaned_offsets(&mut self) {
        let initial_count = self.offsets.len();
        let initial_groups = self.groups.len();
        let now = Instant::now();
        
        // Use configured retention times
        let offset_retention_with_group = Duration::from_millis(self.offset_retention_ms);
        let offset_retention_no_group = Duration::from_millis(self.offset_retention_ms / 12); // 1/12th for orphaned
        let empty_group_retention = Duration::from_millis(self.empty_group_retention_ms);
        
        // First, clean up empty groups that are old enough
        let groups_to_remove: Vec<String> = self.groups.iter()
            .filter_map(|(group_id, group)| {
                // Remove groups that:
                // 1. Have no members
                // 2. Have been empty for a while (check state)
                if group.members.is_empty() && group.state != GroupState::PreparingRebalance {
                    // Check if this group has recent offset activity
                    let has_recent_offsets = self.offsets.iter().any(|((g, _, _), (_, last_commit))| {
                        g == group_id && now.duration_since(*last_commit) < empty_group_retention
                    });
                    
                    if !has_recent_offsets {
                        Some(group_id.clone())
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
            .collect();
        
        // Remove empty groups
        for group_id in groups_to_remove {
            self.groups.remove(&group_id);
            debug!("Removed empty group: {}", group_id);
        }
        
        // Now clean up offsets
        let existing_groups: std::collections::HashSet<_> = self.groups.keys().cloned().collect();
        
        self.offsets.retain(|(group_id, _, _), (_, last_commit)| {
            let age = now.duration_since(*last_commit);
            
            if existing_groups.contains(group_id) {
                // Group exists - use longer retention
                age < offset_retention_with_group
            } else {
                // Group doesn't exist - use shorter retention
                age < offset_retention_no_group
            }
        });
        
        let removed_offsets = initial_count - self.offsets.len();
        let removed_groups = initial_groups - self.groups.len();
        
        if removed_offsets > 0 || removed_groups > 0 {
            debug!("Cleanup: removed {} offsets and {} empty groups", removed_offsets, removed_groups);
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