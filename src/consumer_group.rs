use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};
use bytes::{Bytes, BytesMut, Buf};
use kafka_protocol::messages::consumer_protocol_assignment::ConsumerProtocolAssignment;
use kafka_protocol::protocol::{Encodable, Decodable};
use std::sync::Arc;
use tokio::sync::{Mutex, oneshot};
use crate::metrics::MetricsCollector;
use tracing::debug;

#[derive(Debug)]
pub struct PendingJoinRequest {
    pub member_id: Option<String>,
    pub client_id: String,
    pub client_host: String,
    pub session_timeout_ms: i32,
    pub rebalance_timeout_ms: i32,
    pub protocol_type: String,
    pub protocols: Vec<(String, Bytes)>,
    pub response_tx: oneshot::Sender<JoinGroupResult>,
}

#[derive(Debug, Clone)]
pub struct ConsumerGroup {
    pub _group_id: String,
    pub protocol_type: String,
    pub generation_id: i32,
    pub leader_id: String,
    pub members: HashMap<String, GroupMember>,
    pub state: GroupState,
    pub rebalance_start: Option<Instant>,
    pub members_joining: HashSet<String>,  // Members actively joining
    pub members_synced: HashSet<String>,   // Members that have called sync_group
    pub pending_sync_assignments: HashMap<String, Bytes>,
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
    client_to_member: HashMap<(String, String), String>, // (group_id, client_id) -> member_id
    offsets: HashMap<(String, String, i32), (i64, Instant)>, // offset, last_commit_time
    next_member_id: i32,
    storage: Option<Arc<Mutex<crate::storage::InMemoryStorage>>>,
    metrics: Option<Arc<MetricsCollector>>,
    offset_retention_ms: u64,  // Default: 3600000 (1 hour)
    empty_group_retention_ms: u64,  // Default: 600000 (10 minutes)
    pending_joins: HashMap<String, Vec<PendingJoinRequest>>,
}

impl ConsumerGroupManager {
    pub fn new() -> Self {
        Self {
            groups: HashMap::new(),
            member_to_group: HashMap::new(),
            client_to_member: HashMap::new(),
            offsets: HashMap::new(),
            next_member_id: 1,
            storage: None,
            metrics: None,
            offset_retention_ms: 3600000,  // 1 hour
            empty_group_retention_ms: 600000,  // 10 minutes
            pending_joins: HashMap::new(),
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
    
    pub fn set_retention_times(&mut self, offset_retention_ms: u64, empty_group_retention_ms: u64) {
        self.offset_retention_ms = offset_retention_ms;
        self.empty_group_retention_ms = empty_group_retention_ms;
    }
    
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
        
        for group_id in &groups_to_remove {
            self.groups.remove(group_id);
        }
        
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
    
    pub fn should_delay_join_response(&self, group_id: &str) -> bool {
        if let Some(group) = self.groups.get(group_id) {
            group.state == GroupState::PreparingRebalance
        } else {
            false
        }
    }
    
    pub fn get_members_for_response(&self, group_id: &str) -> Vec<String> {
        if let Some(group) = self.groups.get(group_id) {
            group.members.keys().cloned().collect()
        } else {
            vec![]
        }
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
        response_tx: oneshot::Sender<JoinGroupResult>,
    ) {
        let is_new_member = member_id.is_none() || member_id.as_ref().map_or(false, |id| id.is_empty());
        
        let member_id = if is_new_member {
            let client_key = (group_id.clone(), client_id.clone());
            if let Some(existing_member_id) = self.client_to_member.get(&client_key) {
                debug!("Reusing existing member_id {} for client {}", existing_member_id, client_id);
                existing_member_id.clone()
            } else {
                let new_member_id = self.generate_member_id(&client_id);
                self.client_to_member.insert(client_key, new_member_id.clone());
                debug!("Generated new member_id {} for client {}", new_member_id, client_id);
                new_member_id
            }
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
                members_joining: HashSet::new(),
                members_synced: HashSet::new(),
                pending_sync_assignments: HashMap::new(),
            }
        });

        if !group.members.is_empty() && group.protocol_type != protocol_type {
            let _ = response_tx.send(JoinGroupResult {
                error_code: 17,
                generation_id: 0,
                protocol_name: String::new(),
                leader_id: String::new(),
                member_id: String::new(),
                members: vec![],
                rebalance_completed: false,
            });
            return;
        }

        match group.state {
            GroupState::Empty => {
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
                
                group.leader_id = member_id.clone();
                group.state = GroupState::AwaitingSync;
                group.rebalance_start = None;
                group.members_joining.clear(); // No one else needs to join
                group.members_synced.clear(); // Clear synced members for new sync phase
                
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
                
                let _ = response_tx.send(JoinGroupResult {
                    error_code: 0,
                    generation_id: group.generation_id,
                    protocol_name: protocols.first().map(|(name, _)| name.clone()).unwrap_or_default(),
                    leader_id: group.leader_id.clone(),
                    member_id: member_id.clone(),
                    members: vec![(member_id, protocols.first().map(|(_, m)| m.clone()).unwrap_or_default())],
                    rebalance_completed: false,
                });
            }
            GroupState::Stable => {
                debug!("Member {} attempting to join stable group {}", member_id, group_id);
                
                let is_new_member = !group.members.contains_key(&member_id);
                
                if is_new_member {
                    debug!("New member {} joining stable group, triggering rebalance", member_id);
                    
                    let old_members: Vec<String> = group.members.keys().cloned().collect();
                    debug!("Existing members that must rejoin: {:?}", old_members);
                    
                    group.generation_id += 1;
                    
                    group.members_joining = old_members.into_iter().collect();
                    
                    for (_, member) in group.members.iter_mut() {
                        member.assignment = None;
                    }
                    group.pending_sync_assignments.clear();
                    group.members_synced.clear();
                    
                    group.state = GroupState::PreparingRebalance;
                    group.rebalance_start = Some(Instant::now());
                    
                    debug!("Waiting for {} members to rejoin", group.members_joining.len());
                    
                    debug!("Adding new member {} to rebalance", member_id);
                    group.members_joining.insert(member_id.clone());
                }
                
                let member = GroupMember {
                    _member_id: member_id.clone(),
                    _client_id: client_id.clone(),
                    _client_host: client_host.clone(),
                    session_timeout_ms,
                    _rebalance_timeout_ms: rebalance_timeout_ms,
                    last_heartbeat: Instant::now(),
                    metadata: protocols.first().map(|(_, m)| m.clone()).unwrap_or_default(),
                    assignment: None,
                };
                
                group.members.insert(member_id.clone(), member);
                self.member_to_group.insert(member_id.clone(), group_id.clone());
                
                if group.state == GroupState::PreparingRebalance {
                    debug!("Member {} joined during rebalance, group has {} members", 
                           member_id, group.members.len());
                    
                    let pending_request = PendingJoinRequest {
                        member_id: Some(member_id.clone()),
                        client_id,
                        client_host,
                        session_timeout_ms,
                        rebalance_timeout_ms,
                        protocol_type: protocols.first().map(|(name, _)| name.clone()).unwrap_or_default(),
                        protocols,
                        response_tx,
                    };
                    
                    self.pending_joins.entry(group_id.clone())
                        .or_insert_with(Vec::new)
                        .push(pending_request);
                } else {
                    debug!("Existing member {} rejoined stable group", member_id);
                    
                    let _ = response_tx.send(JoinGroupResult {
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
                        rebalance_completed: false,
                    });
                }
            }
            GroupState::PreparingRebalance => {
                debug!("Member {} joining group {} in PreparingRebalance state", member_id, group_id);
                debug!("Current members before: {:?}", group.members.keys().collect::<Vec<_>>());
                debug!("Waiting for members: {:?}", group.members_joining);
                
                if group.members.contains_key(&member_id) {
                    debug!("Member {} rejoining with new generation", member_id);
                    group.members.remove(&member_id);
                }
                
                let member = GroupMember {
                    _member_id: member_id.clone(),
                    _client_id: client_id.clone(),
                    _client_host: client_host.clone(),
                    session_timeout_ms,
                    _rebalance_timeout_ms: rebalance_timeout_ms,
                    last_heartbeat: Instant::now(),
                    metadata: protocols.first().map(|(_, m)| m.clone()).unwrap_or_default(),
                    assignment: None,
                };
                
                group.members_joining.remove(&member_id);
                
                group.members.insert(member_id.clone(), member);
                self.member_to_group.insert(member_id.clone(), group_id.clone());
                
                debug!("Member {} added, still waiting for {} members", 
                       member_id, group.members_joining.len());
                
                let should_complete = if let Some(start) = group.rebalance_start {
                    let elapsed = start.elapsed();
                    let all_joined = group.members_joining.is_empty();
                    
                    if all_joined {
                        debug!("All expected members have rejoined!");
                    }
                    
                    all_joined || elapsed > Duration::from_secs(2)
                } else {
                    true
                };
                
                if !should_complete {
                    debug!("Still waiting for {} members: {:?}", 
                           group.members_joining.len(), group.members_joining);
                    let pending_request = PendingJoinRequest {
                        member_id: Some(member_id.clone()),
                        client_id,
                        client_host,
                        session_timeout_ms,
                        rebalance_timeout_ms,
                        protocol_type: protocols.first().map(|(name, _)| name.clone()).unwrap_or_default(),
                        protocols,
                        response_tx,
                    };
                    
                    self.pending_joins.entry(group_id.clone())
                        .or_insert_with(Vec::new)
                        .push(pending_request);
                    return;
                }
                
                debug!("Completing rebalance for group {} with {} members", 
                       group_id, group.members.len());
                
                if let Some(leader_id) = group.members.keys().min().cloned() {
                    group.leader_id = leader_id.clone();
                }
                
                group.state = GroupState::AwaitingSync;
                group.rebalance_start = None;
                group.members_synced.clear();
                
                let _completed_members: Vec<(String, i32, String, Vec<(String, Bytes)>)> = 
                    group.members.iter().map(|(id, _member)| {
                        (
                            id.clone(),
                            group.generation_id,
                            if id == &group.leader_id {
                                group.leader_id.clone()
                            } else {
                                group.leader_id.clone()
                            },
                            if id == &group.leader_id {
                                group.members.iter().map(|(mid, m)| {
                                    (mid.clone(), m.metadata.clone())
                                }).collect()
                            } else {
                                vec![]
                            }
                        )
                    }).collect();
                
                let _all_members: Vec<(String, Bytes)> = group.members.iter()
                    .map(|(id, m)| (id.clone(), m.metadata.clone()))
                    .collect();
                
                debug!("Rebalance complete for group {} with final members: {:?}", 
                       group_id, group.members.keys().collect::<Vec<_>>());
                
                let current_result = JoinGroupResult {
                    error_code: 0,
                    generation_id: group.generation_id,
                    protocol_name: protocols.first().map(|(name, _)| name.clone()).unwrap_or_default(),
                    leader_id: group.leader_id.clone(),
                    member_id: member_id.clone(),
                    members: if member_id == group.leader_id {
                        let member_list: Vec<_> = group.members.iter().map(|(id, m)| {
                            (id.clone(), m.metadata.clone())
                        }).collect();
                        debug!("Leader {} gets member list with {} members: {:?}", 
                               member_id, member_list.len(), 
                               member_list.iter().map(|(id, _)| id).collect::<Vec<_>>());
                        member_list
                    } else {
                        vec![]
                    },
                    rebalance_completed: true,
                };
                
                let _ = response_tx.send(current_result);
                
                if let Some(pending_requests) = self.pending_joins.remove(&group_id) {
                    debug!("Sending responses to {} pending join requests", pending_requests.len());
                    
                    for pending in pending_requests {
                        if let Some(pending_member_id) = &pending.member_id {
                            let pending_result = JoinGroupResult {
                                error_code: 0,
                                generation_id: group.generation_id,
                                protocol_name: pending.protocol_type.clone(),
                                leader_id: group.leader_id.clone(),
                                member_id: pending_member_id.clone(),
                                members: if pending_member_id == &group.leader_id {
                                    let member_list: Vec<_> = group.members.iter().map(|(id, m)| {
                                        (id.clone(), m.metadata.clone())
                                    }).collect();
                                    member_list
                                } else {
                                    vec![]
                                },
                                rebalance_completed: true,
                            };
                            
                            let _ = pending.response_tx.send(pending_result);
                        }
                    }
                }
            }
            GroupState::AwaitingSync => {
                debug!("Member {} attempting to join group {} in AwaitingSync state", member_id, group_id);
                
                if group.members.contains_key(&member_id) {
                    debug!("Member {} is part of current generation, returning join success", member_id);
                    let _ = response_tx.send(JoinGroupResult {
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
                        rebalance_completed: false,
                    });
                } else {
                    debug!("New member {} trying to join during AwaitingSync", member_id);
                    
                    
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
                    
                    if !group.pending_sync_assignments.is_empty() && !group.leader_id.is_empty() {
                        debug!("Leader already sent assignments, need to recalculate for new member");
                        
                        let mut topic_name = String::new();
                        for (_, assignment) in &group.pending_sync_assignments {
                            if let Ok(decoded) = decode_assignment(assignment) {
                                if let Some((topic, _)) = decoded.first() {
                                    topic_name = topic.clone();
                                    break;
                                }
                            }
                        }
                        
                        if !topic_name.is_empty() {
                            let all_member_ids: Vec<String> = group.members.keys().cloned().collect();
                            debug!("Recalculating assignments for {} members on topic {}", all_member_ids.len(), topic_name);
                            
                            let num_partitions = 3;
                            let mut new_assignments = HashMap::new();
                            
                            for (idx, mid) in all_member_ids.iter().enumerate() {
                                let mut member_partitions = Vec::new();
                                for p in 0..num_partitions {
                                    if p % all_member_ids.len() == idx {
                                        member_partitions.push(p as i32);
                                    }
                                }
                                
                                if !member_partitions.is_empty() {
                                    let assignment = create_assignment_for_partitions(&topic_name, member_partitions.clone());
                                    new_assignments.insert(mid.clone(), assignment);
                                    debug!("  Recalculated assignment for {}: partitions {:?}", mid, member_partitions);
                                } else {
                                    new_assignments.insert(mid.clone(), create_empty_assignment());
                                    debug!("  Recalculated assignment for {}: no partitions", mid);
                                }
                            }
                            
                            group.pending_sync_assignments = new_assignments;
                        }
                    }
                    
                    debug!("Added member {} to AwaitingSync group, now have {} members", 
                           member_id, group.members.len());
                    
                    let _ = response_tx.send(JoinGroupResult {
                        error_code: 0,
                        generation_id: group.generation_id,
                        protocol_name: protocols.first().map(|(name, _)| name.clone()).unwrap_or_default(),
                        leader_id: group.leader_id.clone(),
                        member_id: member_id.clone(),
                        members: vec![],
                        rebalance_completed: false,
                    });
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
        debug!("SyncGroup for member {} in group {} gen {} with {} assignments", 
               member_id, group_id, generation_id, assignments.len());
        
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

        if group.state != GroupState::AwaitingSync && group.state != GroupState::Stable {
            debug!("Group {} in unexpected state {:?} for sync_group", group_id, group.state);
            return Ok(create_empty_assignment());
        }

        if member_id == group.leader_id {
            debug!("Leader {} sync_group with {} assignments", member_id, assignments.len());
            debug!("Group has {} total members", group.members.len());
            debug!("Members in group: {:?}", group.members.keys().collect::<Vec<_>>());
            
            if assignments.len() < group.members.len() {
                debug!("WARNING: Leader has {} assignments but group has {} members!", 
                       assignments.len(), group.members.len());
                debug!("Missing assignments for members: {:?}", 
                       group.members.keys()
                           .filter(|id| !assignments.contains_key(*id))
                           .collect::<Vec<_>>());
            }
            
            for (mid, assignment) in &assignments {
                debug!("  Assignment for member {}: {} bytes", mid, assignment.len());
                if let Ok(decoded) = decode_assignment(assignment) {
                    debug!("    Decoded assignment: {:?}", decoded);
                } else {
                    debug!("    Failed to decode assignment, raw bytes: {:?}", assignment.as_ref());
                }
            }
            
            group.pending_sync_assignments = assignments.clone();
            
            debug!("Leader {} sent assignments for {} members:", member_id, assignments.len());
            for (mid, assignment) in &assignments {
                debug!("  Assignment for {}: {} bytes", mid, assignment.len());
                if let Ok(decoded) = decode_assignment(assignment) {
                    for (topic, partitions) in decoded {
                        debug!("    Topic {}: partitions {:?}", topic, partitions);
                    }
                }
            }
            
            for (mid, assignment) in assignments {
                if let Some(member) = group.members.get_mut(&mid) {
                    member.assignment = Some(assignment.clone());
                    debug!("Saved assignment to existing member {}", mid);
                } else {
                    debug!("WARNING: Assignment for unknown member {} - this member may have been cleared during rebalance!", mid);
                }
            }
            
            debug!("Leader has distributed assignments, staying in AwaitingSync until all members sync");
            
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

        debug!("Checking pending assignments for member {}", member_id);
        debug!("Pending assignments available for: {:?}", group.pending_sync_assignments.keys().collect::<Vec<_>>());
        
        if let Some(pending_assignment) = group.pending_sync_assignments.get(member_id) {
            debug!("Found pending assignment for member {} from leader", member_id);
            if let Some(member) = group.members.get_mut(member_id) {
                member.assignment = Some(pending_assignment.clone());
                debug!("Successfully assigned {} bytes to member {}", pending_assignment.len(), member_id);
            }
        } else {
            debug!("No pending assignment found for member {}", member_id);
        }
        
        debug!("Current members in group: {:?}", group.members.keys().collect::<Vec<_>>());
        
        let member_assignment = group.members.get(member_id)
            .and_then(|m| m.assignment.clone());
            
        debug!("SyncGroup: member {} in state {:?}, has assignment: {}", 
               member_id, group.state, member_assignment.is_some());
        
        if let Some(assignment) = member_assignment {
            debug!("Returning assignment for member {} ({} bytes)", member_id, assignment.len());
            
            group.members_synced.insert(member_id.to_string());
            debug!("Member {} has synced, {} out of {} members synced", 
                   member_id, group.members_synced.len(), group.members.len());
            
            if group.state == GroupState::AwaitingSync && 
               group.members_synced.len() == group.members.len() {
                debug!("All {} members have synced, transitioning to Stable", group.members.len());
                group.state = GroupState::Stable;
                
                group.members_synced.clear();
                
                let unassigned_members: Vec<_> = group.members.iter()
                    .filter(|(_, m)| m.assignment.is_none())
                    .map(|(id, _)| id.clone())
                    .collect();
                    
                if !unassigned_members.is_empty() {
                    debug!("Found {} unassigned members after sync, triggering new rebalance", unassigned_members.len());
                    group.state = GroupState::PreparingRebalance;
                    group.generation_id += 1;
                    group.rebalance_start = Some(Instant::now());
                    group.members_joining = group.members.keys().cloned().collect();
                    
                    for (_, member) in group.members.iter_mut() {
                        member.assignment = None;
                    }
                    group.pending_sync_assignments.clear();
                }
            }
            
            return Ok(assignment);
        }
        
        let pending_assignment = group.pending_sync_assignments.get(member_id).cloned();
        
        if let Some(pending) = pending_assignment {
            debug!("Found pending assignment for member {}", member_id);
            if let Some(member) = group.members.get_mut(member_id) {
                member.assignment = Some(pending.clone());
                
                group.members_synced.insert(member_id.to_string());
                
                if group.state == GroupState::AwaitingSync && 
                   group.members_synced.len() == group.members.len() {
                    debug!("All {} members have synchronized, transitioning to Stable", group.members.len());
                    group.state = GroupState::Stable;
                    group.members_synced.clear();
                    
                        let unassigned_members: Vec<_> = group.members.iter()
                        .filter(|(_, m)| m.assignment.is_none())
                        .map(|(id, _)| id.clone())
                        .collect();
                        
                    if !unassigned_members.is_empty() {
                        debug!("Found {} unassigned members after sync, triggering new rebalance", unassigned_members.len());
                            group.state = GroupState::PreparingRebalance;
                        group.generation_id += 1;
                        group.rebalance_start = Some(Instant::now());
                        group.members_joining = group.members.keys().cloned().collect();
                        
                            for (_, member) in group.members.iter_mut() {
                            member.assignment = None;
                        }
                        group.pending_sync_assignments.clear();
                    }
                }
            }
            return Ok(pending);
        }
        
        if group.state == GroupState::AwaitingSync {
            debug!("Member {} called sync_group before leader", member_id);
            return Err(GroupError::RebalanceInProgress);
        } else {
            debug!("Member {} has no assignment in state {:?}, THIS IS A BUG!", member_id, group.state);
        }
        
        Ok(create_empty_assignment())
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

            if let Some(member) = group.members.remove(member_id) {
                let client_key = (group_id.to_string(), member._client_id.clone());
                self.client_to_member.remove(&client_key);
            }
            self.member_to_group.remove(member_id);

            if group.members.is_empty() {
                true
            } else {
                group.state = GroupState::PreparingRebalance;
                group.generation_id += 1;
                group.rebalance_start = Some(Instant::now());
                group.members_synced.clear();
                
                for (_, member) in group.members.iter_mut() {
                    member.assignment = None;
                }
                false
            }
        };

        if should_remove {
            debug!("Group {} is now empty, removing group (keeping offsets temporarily)", group_id);
            self.groups.remove(group_id);
            
            if let Some(metrics) = &self.metrics {
                let runtime = tokio::runtime::Handle::try_current();
                if let Ok(handle) = runtime {
                    let metrics = metrics.clone();
                    let group_id_clone = group_id.to_string();
                    handle.spawn(async move {
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
            self.groups.insert(group_id.to_string(), ConsumerGroup {
                _group_id: group_id.to_string(),
                state: GroupState::Stable,
                generation_id: 0,
                protocol_type: "consumer".to_string(),
                leader_id: "".to_string(),
                members: HashMap::new(),
                rebalance_start: None,
                members_joining: HashSet::new(),
                members_synced: HashSet::new(),
                pending_sync_assignments: HashMap::new(),
            });
        }

        for (topic, partition, offset) in offsets {
            debug!("Storing offset for {}/{}/{}: {}", group_id, topic, partition, offset);
            self.offsets.insert(
                (group_id.to_string(), topic.clone(), partition), 
                (offset, Instant::now())
            );
            
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


    pub fn complete_timed_out_rebalances(&mut self) {
        let _now = Instant::now();
        let timeout = Duration::from_secs(2);
        
        let groups_to_complete: Vec<String> = self.groups.iter()
            .filter_map(|(group_id, group)| {
                if group.state == GroupState::PreparingRebalance {
                    if let Some(start) = group.rebalance_start {
                        if start.elapsed() > timeout {
                            Some(group_id.clone())
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
            .collect();
        
        for group_id in groups_to_complete {
            debug!("Completing timed out rebalance for group {}", group_id);
            self.complete_rebalance(&group_id);
        }
    }
    
    fn complete_rebalance(&mut self, group_id: &str) {
        if let Some(group) = self.groups.get_mut(group_id) {
            if group.state != GroupState::PreparingRebalance {
                return;
            }
            
            debug!("Force completing rebalance for group {} with {} members", 
                   group_id, group.members.len());
            
            if let Some(leader_id) = group.members.keys().min().cloned() {
                group.leader_id = leader_id.clone();
            }
            
            group.state = GroupState::AwaitingSync;
            group.rebalance_start = None;
            group.members_synced.clear();
            group.members_joining.clear();
            
            if let Some(pending_requests) = self.pending_joins.remove(group_id) {
                debug!("Sending timeout responses to {} pending join requests", pending_requests.len());
                
                let member_list: Vec<_> = group.members.iter().map(|(id, m)| {
                    (id.clone(), m.metadata.clone())
                }).collect();
                
                for pending in pending_requests {
                    if let Some(pending_member_id) = &pending.member_id {
                        let is_leader = pending_member_id == &group.leader_id;
                        let result = JoinGroupResult {
                            error_code: 0,
                            generation_id: group.generation_id,
                            protocol_name: pending.protocol_type.clone(),
                            leader_id: group.leader_id.clone(),
                            member_id: pending_member_id.clone(),
                            members: if is_leader {
                                member_list.clone()
                            } else {
                                vec![]
                            },
                            rebalance_completed: true,
                        };
                        
                        let _ = pending.response_tx.send(result);
                    }
                }
            }
        }
    }
    
    pub fn check_expired_members(&mut self) {
        let now = Instant::now();
        let mut expired_members = Vec::new();

        for (group_id, group) in &self.groups {
            for (member_id, member) in &group.members {
                let timeout = Duration::from_millis(member.session_timeout_ms as u64);
                let effective_timeout = if group.state == GroupState::PreparingRebalance {
                    timeout.min(Duration::from_secs(10))
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
        
    }
    
    pub fn get_offset_count(&self) -> usize {
        self.offsets.len()
    }
    
    pub fn get_all_group_ids(&self) -> Vec<String> {
        self.groups.keys().cloned().collect()
    }
    
    pub fn cleanup_orphaned_offsets(&mut self) {
        let initial_count = self.offsets.len();
        let initial_groups = self.groups.len();
        let now = Instant::now();
        
        let offset_retention_with_group = Duration::from_millis(self.offset_retention_ms);
        let offset_retention_no_group = Duration::from_millis(self.offset_retention_ms / 12); // 1/12th for orphaned
        let empty_group_retention = Duration::from_millis(self.empty_group_retention_ms);
        
        let groups_to_remove: Vec<String> = self.groups.iter()
            .filter_map(|(group_id, group)| {
                if group.members.is_empty() && group.state != GroupState::PreparingRebalance {
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
        
        for group_id in groups_to_remove {
            self.groups.remove(&group_id);
            debug!("Removed empty group: {}", group_id);
        }
        
        let existing_groups: std::collections::HashSet<_> = self.groups.keys().cloned().collect();
        
        self.offsets.retain(|(group_id, _, _), (_, last_commit)| {
            let age = now.duration_since(*last_commit);
            
            if existing_groups.contains(group_id) {
                age < offset_retention_with_group
            } else {
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
    pub rebalance_completed: bool,
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

fn create_assignment_for_partitions(topic: &str, partitions: Vec<i32>) -> Bytes {
    use kafka_protocol::messages::consumer_protocol_assignment::TopicPartition as AssignmentTopicPartition;
    use kafka_protocol::messages::TopicName;
    use kafka_protocol::protocol::StrBytes;
    
    let topic_assignment = AssignmentTopicPartition::default()
        .with_topic(TopicName(StrBytes::from_string(topic.to_string())))
        .with_partitions(partitions);
    
    let assignment = ConsumerProtocolAssignment::default()
        .with_assigned_partitions(vec![topic_assignment])
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
    
    buf.extend_from_slice(&1u16.to_be_bytes());
    
    buf.extend_from_slice(&(group_id.len() as u16).to_be_bytes());
    buf.extend_from_slice(group_id.as_bytes());
    
    buf.extend_from_slice(&(topic.len() as u16).to_be_bytes());
    buf.extend_from_slice(topic.as_bytes());
    
    buf.extend_from_slice(&partition.to_be_bytes());
    
    buf.freeze()
}

fn create_commit_log_value(offset: i64) -> Bytes {
    let mut buf = BytesMut::new();
    
    buf.extend_from_slice(&1u16.to_be_bytes());
    
    buf.extend_from_slice(&offset.to_be_bytes());
    
    buf.extend_from_slice(&0u16.to_be_bytes());
    
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    buf.extend_from_slice(&timestamp.to_be_bytes());
    
    buf.freeze()
}

fn decode_assignment(assignment: &Bytes) -> Result<Vec<(String, Vec<i32>)>, Box<dyn std::error::Error>> {
    let mut buf = assignment.clone();
    
    if buf.remaining() < 2 {
        return Err("Not enough data for version".into());
    }
    let _version = buf.get_i16();
    
    let mut assignment_buf = buf.clone();
    match ConsumerProtocolAssignment::decode(&mut assignment_buf, 0) {
        Ok(decoded) => {
            let mut result = Vec::new();
            for topic_partition in decoded.assigned_partitions {
                let topic = topic_partition.topic.to_string();
                let partitions = topic_partition.partitions;
                result.push((topic, partitions));
            }
            Ok(result)
        }
        Err(e) => {
            debug!("Failed to decode as ConsumerProtocolAssignment: {}", e);
            Err(format!("Decode error: {}", e).into())
        }
    }
}