use crate::storage::InMemoryStorage;
use crate::consumer_group::{ConsumerGroupManager, GroupError};
use bytes::{BytesMut, Bytes, Buf};
use kafka_protocol::messages::{
    BrokerId, TopicName,
    api_versions_response::{ApiVersion, ApiVersionsResponse},
    metadata_request::MetadataRequest,
    metadata_response::{MetadataResponse, MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic},
    produce_request::ProduceRequest,
    produce_response::{ProduceResponse, PartitionProduceResponse, TopicProduceResponse},
    fetch_request::FetchRequest,
    fetch_response::{FetchResponse, FetchableTopicResponse, PartitionData},
    find_coordinator_request::FindCoordinatorRequest,
    find_coordinator_response::FindCoordinatorResponse,
    join_group_request::JoinGroupRequest,
    join_group_response::{JoinGroupResponse, JoinGroupResponseMember},
    sync_group_request::SyncGroupRequest,
    sync_group_response::SyncGroupResponse,
    heartbeat_request::HeartbeatRequest,
    heartbeat_response::HeartbeatResponse,
    offset_commit_request::OffsetCommitRequest,
    offset_commit_response::{OffsetCommitResponse, OffsetCommitResponseTopic, OffsetCommitResponsePartition},
    offset_fetch_request::OffsetFetchRequest,
    offset_fetch_response::{OffsetFetchResponse, OffsetFetchResponseTopic, OffsetFetchResponsePartition},
    leave_group_request::LeaveGroupRequest,
    leave_group_response::{LeaveGroupResponse, MemberResponse},
    list_offsets_request::ListOffsetsRequest,
    list_offsets_response::{ListOffsetsResponse, ListOffsetsPartitionResponse, ListOffsetsTopicResponse},
    request_header::RequestHeader,
    response_header::ResponseHeader,
};
use kafka_protocol::protocol::{Decodable, Encodable, StrBytes};
use std::sync::Arc;
use std::collections::HashMap;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tracing::{debug, error, info};
use std::env;
use local_ip_address::local_ip;

fn get_advertised_host() -> String {
    // First check environment variable
    if let Ok(host) = env::var("KAFKA_ADVERTISED_HOST") {
        return host;
    }
    
    // Try to get the local IP address
    match local_ip() {
        Ok(ip) => ip.to_string(),
        Err(e) => {
            debug!("Failed to get local IP: {}", e);
            "localhost".to_string()
        }
    }
}

pub struct KafkaBroker {
    addr: String,
    storage: Arc<Mutex<InMemoryStorage>>,
    group_manager: Arc<Mutex<ConsumerGroupManager>>,
    broker_id: BrokerId,
}

impl KafkaBroker {
    pub fn new(addr: impl Into<String>) -> Self {
        let storage = Arc::new(Mutex::new(InMemoryStorage::new()));
        let group_manager = Arc::new(Mutex::new(
            ConsumerGroupManager::new_with_storage(storage.clone())
        ));
        
        Self {
            addr: addr.into(),
            storage,
            group_manager,
            broker_id: BrokerId(0),
        }
    }

    pub async fn run(&self) -> Result<(), Box<dyn std::error::Error>> {
        let listener = TcpListener::bind(&self.addr).await?;
        info!("Kafka broker listening on {}", self.addr);

        // Spawn task to check expired members
        let group_manager_clone = self.group_manager.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));
            loop {
                interval.tick().await;
                let mut manager = group_manager_clone.lock().await;
                manager.check_expired_members();
            }
        });

        loop {
            let (socket, addr) = listener.accept().await?;
            info!("New connection from: {}", addr);

            let storage = self.storage.clone();
            let group_manager = self.group_manager.clone();
            let broker_id = self.broker_id;
            
            tokio::spawn(async move {
                if let Err(e) = handle_connection(socket, storage, group_manager, broker_id).await {
                    error!("Connection error: {}", e);
                }
            });
        }
    }
}

async fn handle_connection(
    mut socket: TcpStream,
    storage: Arc<Mutex<InMemoryStorage>>,
    group_manager: Arc<Mutex<ConsumerGroupManager>>,
    _broker_id: BrokerId,
) -> Result<(), Box<dyn std::error::Error>> {
    let peer_addr = socket.peer_addr().ok().map(|a| a.to_string()).unwrap_or_else(|| "unknown".to_string());
    let mut size_buf = [0u8; 4];

    loop {
        if socket.read_exact(&mut size_buf).await.is_err() {
            debug!("Client disconnected");
            break;
        }

        let size = i32::from_be_bytes(size_buf) as usize;
        debug!("Incoming message size: {}", size);

        let mut message_buf = vec![0u8; size];
        socket.read_exact(&mut message_buf).await?;
        
        let mut buf = BytesMut::from(&message_buf[..]);
        
        // Debug output for protocol analysis
        if size >= 8 {
            let api_key = i16::from_be_bytes([message_buf[0], message_buf[1]]);
            let api_version = i16::from_be_bytes([message_buf[2], message_buf[3]]);
            let correlation_id = i32::from_be_bytes([message_buf[4], message_buf[5], message_buf[6], message_buf[7]]);
            debug!("Raw message: api_key={}, api_version={}, correlation_id={}", api_key, api_version, correlation_id);
            
            if size > 8 {
                let next_bytes: Vec<u8> = message_buf[8..std::cmp::min(28, size)].to_vec();
                debug!("Bytes after correlation_id: {:?}", next_bytes);
            }
        }
        
        // kafka-python uses header v1 which includes client_id
        let header = RequestHeader::decode(&mut buf, 1)?;
        debug!("Parsed header: api_key={}, version={}, correlation_id={}, client_id={:?}, remaining_bytes={}", 
               header.request_api_key, header.request_api_version, header.correlation_id, 
               header.client_id, buf.remaining());

        // Handle request based on API key
        let response_bytes = match header.request_api_key {
            18 => handle_api_versions(&header).await?, // ApiVersions
            3 => handle_metadata(&header, &mut buf, storage.clone()).await?, // Metadata
            0 => handle_produce(&header, &mut buf, storage.clone()).await?, // Produce
            1 => handle_fetch(&header, &mut buf, storage.clone()).await?, // Fetch
            2 => handle_list_offsets(&header, &mut buf, storage.clone()).await?, // ListOffsets
            10 => handle_find_coordinator(&header, &mut buf, group_manager.clone()).await?, // FindCoordinator
            11 => handle_join_group(&header, &mut buf, group_manager.clone(), &peer_addr).await?, // JoinGroup
            14 => handle_sync_group(&header, &mut buf, group_manager.clone()).await?, // SyncGroup
            12 => handle_heartbeat(&header, &mut buf, group_manager.clone()).await?, // Heartbeat
            8 => handle_offset_commit(&header, &mut buf, group_manager.clone()).await?, // OffsetCommit
            9 => handle_offset_fetch(&header, &mut buf, group_manager.clone()).await?, // OffsetFetch
            13 => handle_leave_group(&header, &mut buf, group_manager.clone()).await?, // LeaveGroup
            _ => {
                error!("Unsupported API key: {}", header.request_api_key);
                continue;
            }
        };

        // Send response
        let response_size = (response_bytes.len() as i32).to_be_bytes();
        socket.write_all(&response_size).await?;
        socket.write_all(&response_bytes).await?;
        socket.flush().await?;
    }

    Ok(())
}

// Parse a MessageSet and extract individual messages with key and value
fn parse_message_set(data: &Bytes) -> Vec<(Option<Bytes>, Bytes)> {
    let mut messages = Vec::new();
    let mut offset = 0;
    let data_slice = data.as_ref();
    
    debug!("Parsing MessageSet of {} bytes", data_slice.len());
    
    while offset + 12 <= data_slice.len() {
        // Read message offset (8 bytes)
        let msg_offset = i64::from_be_bytes([
            data_slice[offset], data_slice[offset+1], data_slice[offset+2], data_slice[offset+3],
            data_slice[offset+4], data_slice[offset+5], data_slice[offset+6], data_slice[offset+7]
        ]);
        offset += 8;
        
        // Read message size (4 bytes)
        let msg_size = i32::from_be_bytes([
            data_slice[offset], data_slice[offset+1], data_slice[offset+2], data_slice[offset+3]
        ]) as usize;
        offset += 4;
        
        debug!("MessageSet entry: offset={}, size={}", msg_offset, msg_size);
        
        if offset + msg_size > data_slice.len() {
            debug!("MessageSet truncated, stopping parse");
            break;
        }
        
        // Parse the message to extract the value
        let msg_data = &data_slice[offset..offset + msg_size];
        offset += msg_size;
        
        // Message format:
        // CRC (4 bytes)
        // Magic (1 byte)
        // Attributes (1 byte)
        // Key length (4 bytes) - can be -1 for null
        // Key (variable) - absent if key length is -1
        // Value length (4 bytes)
        // Value (variable)
        
        if msg_data.len() < 14 {  // Minimum size for a message with null key
            debug!("Message too short, skipping");
            continue;
        }
        
        let mut msg_offset = 4; // Skip CRC
        let magic = msg_data[msg_offset];
        msg_offset += 1;
        let _attributes = msg_data[msg_offset];
        msg_offset += 1;
        
        debug!("Message: magic={}, attributes={}, msg_len={}", magic, _attributes, msg_data.len());
        
        // For magic byte 1, there's a timestamp field
        if magic == 1 {
            // Skip timestamp (8 bytes)
            if msg_offset + 8 > msg_data.len() {
                debug!("Can't read timestamp, skipping");
                continue;
            }
            msg_offset += 8;
        }
        
        // Read key length
        if msg_offset + 4 > msg_data.len() {
            debug!("Can't read key length, skipping");
            continue;
        }

        let key_len = i32::from_be_bytes([
            msg_data[msg_offset], msg_data[msg_offset+1],
            msg_data[msg_offset+2], msg_data[msg_offset+3]
        ]);
        msg_offset += 4;

        // Extract key if present
        let key = if key_len >= 0 {
            if msg_offset + key_len as usize > msg_data.len() {
                debug!("Key truncated, skipping");
                continue;
            }
            let key_data = &msg_data[msg_offset..msg_offset + key_len as usize];
            msg_offset += key_len as usize;
            Some(Bytes::copy_from_slice(key_data))
        } else {
            None
        };
        
        // Read value length
        if msg_offset + 4 > msg_data.len() {
            debug!("Can't read value length, skipping");
            continue;
        }
        
        let value_len = i32::from_be_bytes([
            msg_data[msg_offset], msg_data[msg_offset+1], 
            msg_data[msg_offset+2], msg_data[msg_offset+3]
        ]);
        msg_offset += 4;
        
        // Extract value
        if value_len >= 0 && msg_offset + value_len as usize <= msg_data.len() {
            let value = &msg_data[msg_offset..msg_offset + value_len as usize];
            let key_len_for_debug = key.as_ref().map(|k| k.len()).unwrap_or(0);
            messages.push((key, Bytes::copy_from_slice(value)));
            debug!("Extracted message: key={} bytes, value={} bytes", 
                   key_len_for_debug, value_len);
        } else {
            debug!("Invalid value length or truncated value");
        }
    }
    
    debug!("Parsed {} messages from MessageSet", messages.len());
    messages
}

async fn handle_api_versions(
    header: &RequestHeader,
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    debug!("Handling ApiVersions request version {}", header.request_api_version);
    
    let mut api_versions = Vec::new();
    
    // Supported API - declare support for all versions we actually implement
    let supported_apis = vec![
        (18, 0, 4),  // ApiVersions - support all versions
        (3, 0, 2),   // Metadata
        (0, 0, 2),   // Produce
        (1, 0, 3),   // Fetch
        (2, 0, 1),   // ListOffsets - MISSING! Added now
        (10, 0, 1),  // FindCoordinator
        (11, 0, 2),  // JoinGroup
        (14, 0, 1),  // SyncGroup
        (12, 0, 1),  // Heartbeat
        (8, 0, 2),   // OffsetCommit
        (9, 0, 2),   // OffsetFetch
        (13, 0, 1),  // LeaveGroup
    ];
    
    for (api_key, min_version, max_version) in supported_apis {
        let v = ApiVersion::default()
            .with_api_key(api_key)
            .with_min_version(min_version)
            .with_max_version(max_version);
        api_versions.push(v);
    }
    
    // Build response with all required fields
    let mut response = ApiVersionsResponse::default()
        .with_error_code(0)
        .with_api_keys(api_versions);
        
    // Add throttle_time_ms for v1+
    if header.request_api_version >= 1 {
        response = response.with_throttle_time_ms(0);
    }
    
    // For v3+, we need to handle additional fields properly
    if header.request_api_version >= 3 {
        // Set default values for v3+ fields
        response = response
            .with_supported_features(vec![])
            .with_finalized_features_epoch(-1)
            .with_finalized_features(vec![])
            .with_zk_migration_ready(false);
    }

    let mut response_header = ResponseHeader::default();
    response_header.correlation_id = header.correlation_id;

    let mut buf = BytesMut::new();
    
    // ApiVersionsResponse always uses header version 0
    response_header.encode(&mut buf, 0)?;
    
    // Encode the response with the requested version
    response.encode(&mut buf, header.request_api_version)?;
    
    debug!("ApiVersions response for v{}: {} bytes", header.request_api_version, buf.len());
    
    Ok(buf.to_vec())
}

async fn handle_metadata(
    header: &RequestHeader,
    buf: &mut BytesMut,
    storage: Arc<Mutex<InMemoryStorage>>,
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    debug!("Handling Metadata request version {}", header.request_api_version);
    
    // Debug: show remaining bytes
    if buf.remaining() > 0 {
        let remaining_bytes: Vec<u8> = buf.chunk().iter().take(40).copied().collect();
        debug!("Remaining bytes in buffer: {:?} (total: {})", remaining_bytes, buf.remaining());
    }
    
    // Try to decode the request
    let request = match MetadataRequest::decode(buf, header.request_api_version) {
        Ok(req) => req,
        Err(e) => {
            error!("Failed to decode Metadata request: {}", e);
            return Err(e.into());
        }
    };
    
    let storage = storage.lock().await;
    
    let mut topics = Vec::new();
    
    // If topics is None, return metadata for all topics
    let topic_names: Vec<String> = if let Some(req_topics) = request.topics {
        req_topics.into_iter()
            .filter_map(|t| t.name.map(|n| n.to_string()))
            .collect()
    } else {
        storage.get_all_topics()
    };
    
    for topic_name in topic_names {
        let partitions = storage.get_partitions(&topic_name);
        
        debug!("Topic '{}' has {} partitions", topic_name, partitions.len());
        
        // Create missing topic with 3 fixed partition
        if partitions.is_empty() && !topic_name.is_empty() {
            debug!("Auto-creating topic '{}' with 3 partitions", topic_name);
            // We cannot modify storage here because it's immutable
            // We use 3 fixed partition
            let default_partitions = vec![0, 1, 2];
            
            let mut topic = MetadataResponseTopic::default();
            topic.error_code = 0;
            topic.name = Some(TopicName::from(StrBytes::from(topic_name.clone())));
            topic.is_internal = false;
            topic.partitions = default_partitions.into_iter().map(|p| {
                let mut partition = MetadataResponsePartition::default();
                partition.error_code = 0;
                partition.partition_index = p;
                partition.leader_id = BrokerId(0);
                partition.leader_epoch = 0;
                partition.replica_nodes = vec![BrokerId(0)];
                partition.isr_nodes = vec![BrokerId(0)];
                partition.offline_replicas = vec![];
                partition
            }).collect();
            
            topics.push(topic);
        } else if !partitions.is_empty() {
            let mut topic = MetadataResponseTopic::default();
            topic.error_code = 0;
            topic.name = Some(TopicName::from(StrBytes::from(topic_name.clone())));
            topic.is_internal = false;
            topic.partitions = partitions.into_iter().map(|p| {
                let mut partition = MetadataResponsePartition::default();
                partition.error_code = 0;
                partition.partition_index = p;
                partition.leader_id = BrokerId(0);
                partition.leader_epoch = 0;
                partition.replica_nodes = vec![BrokerId(0)];
                partition.isr_nodes = vec![BrokerId(0)];
                partition.offline_replicas = vec![];
                partition
            }).collect();
            
            topics.push(topic);
        }
    }
    
    debug!("Returning metadata for {} topics", topics.len());

    let mut response = MetadataResponse::default();
    response.throttle_time_ms = 0;
    response.brokers = vec![{
        let mut broker = MetadataResponseBroker::default();
        broker.node_id = BrokerId(0);
        broker.host = get_advertised_host().into();
        broker.port = 9092;
        broker.rack = None;
        broker
    }];
    response.cluster_id = Some("rustka-cluster".into());
    response.controller_id = BrokerId(0);
    response.topics = topics;

    let mut response_header = ResponseHeader::default();
    response_header.correlation_id = header.correlation_id;

    let mut response_buf = BytesMut::new();
    response_header.encode(&mut response_buf, 0)?;
    response.encode(&mut response_buf, header.request_api_version)?;
    
    Ok(response_buf.to_vec())
}

async fn handle_produce(
    header: &RequestHeader,
    buf: &mut BytesMut,
    storage: Arc<Mutex<InMemoryStorage>>,
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    debug!("Handling Produce request version {}", header.request_api_version);
    
    let request = ProduceRequest::decode(buf, header.request_api_version)?;
    let mut storage = storage.lock().await;
    
    debug!("Produce request has {} topics", request.topic_data.len());
    
    let mut responses = Vec::new();
    
    for topic_data in request.topic_data {
        let topic_name = topic_data.name.to_string();
        let mut partition_responses = Vec::new();
        
        for partition_data in topic_data.partition_data {
            let partition = partition_data.index;
            
            // Decode the records
            if let Some(records) = partition_data.records {
                // Debug: print first 100 bytes of records
                let preview = &records.as_ref()[..std::cmp::min(100, records.len())];
                debug!("Records preview (first {} bytes): {:?}", preview.len(), preview);
                
                // For API version <= 1, records are in MessageSet format
                // For API version >= 2, records are in RecordBatch format
                let mut base_offset = 0i64;
                
                if header.request_api_version <= 2 {
                    // Parse MessageSet format (used in API v0, v1, v2)
                    let messages = parse_message_set(&records);
                    debug!("Extracted {} messages from MessageSet", messages.len());
                    
                    if !messages.is_empty() {
                        // Store each message individually with key and value
                        for (i, (key, value)) in messages.iter().enumerate() {
                            let offset = storage.append_records(&topic_name, partition, key.clone(), value.clone());
                            if i == 0 {
                                base_offset = offset;
                            }
                            debug!("Stored message {} at offset {}", i, offset);
                        }
                    } else {
                        // If no messages were parsed, store the raw data as fallback
                        base_offset = storage.append_records(&topic_name, partition, None, records);
                    }
                } else {
                    // For newer versions, store as-is (would need RecordBatch parsing)
                    base_offset = storage.append_records(&topic_name, partition, None, records);
                }
                
                debug!("Produced to topic={}, partition={}, base_offset={}", topic_name, partition, base_offset);
                
                let mut partition_response = PartitionProduceResponse::default();
                partition_response.index = partition;
                partition_response.error_code = 0;
                partition_response.base_offset = base_offset;
                partition_response.log_append_time_ms = -1;
                partition_response.log_start_offset = 0;
                
                partition_responses.push(partition_response);
            }
        }
        
        let mut topic_response = TopicProduceResponse::default();
        topic_response.name = TopicName::from(StrBytes::from(topic_name));
        topic_response.partition_responses = partition_responses;
        
        responses.push(topic_response);
    }
    
    let mut response = ProduceResponse::default();
    response.responses = responses;
    response.throttle_time_ms = 0;

    let mut response_header = ResponseHeader::default();
    response_header.correlation_id = header.correlation_id;

    let mut response_buf = BytesMut::new();
    response_header.encode(&mut response_buf, 0)?;
    response.encode(&mut response_buf, header.request_api_version)?;
    
    Ok(response_buf.to_vec())
}

async fn handle_fetch(
    header: &RequestHeader,
    buf: &mut BytesMut,
    storage: Arc<Mutex<InMemoryStorage>>,
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    debug!("Handling Fetch request v{}", header.request_api_version);
    
    let request = FetchRequest::decode(buf, header.request_api_version)?;
    let storage = storage.lock().await;
    
    let mut responses = Vec::new();
    
    for topic in request.topics {
        let topic_name = topic.topic.to_string();
        let mut partition_responses = Vec::new();
        
        for partition in topic.partitions {
            debug!("Fetch partition: topic={}, partition={}, offset={}, max_bytes={}", 
                   topic_name, partition.partition, partition.fetch_offset, 
                   partition.partition_max_bytes);
            
            // For API v4+, use RecordBatch format (not implemented yet)
            // For API v3 and below, we can only return one record at a time
            // This is a limitation of kafka-protocol-rs library
            let use_record_batch = header.request_api_version >= 4;
            
            let (error_code, high_watermark, records_bytes) = if use_record_batch {
                // Use RecordBatch format for API v4+
                let batch_result = storage.fetch_batch_recordbatch(
                    &topic_name,
                    partition.partition,
                    partition.fetch_offset,
                    partition.partition_max_bytes,
                );
                
                match batch_result {
                    Ok((hw, record_batch)) => {
                        debug!("RecordBatch fetch v{}: offset={}, hw={}, has_batch={}", 
                            header.request_api_version, partition.fetch_offset, hw, record_batch.is_some());
                        (0, hw, Some(record_batch.unwrap_or_else(Bytes::new)))
                    },
                    Err(e) => {
                        debug!("Fetch error: {:?}", e);
                        (3, 0, Some(Bytes::new())) // UnknownTopicOrPartition
                    }
                }
            } else {
                // For older API versions, use MessageSet format
                let batch_result = storage.fetch_batch_legacy(
                    &topic_name,
                    partition.partition,
                    partition.fetch_offset,
                    partition.partition_max_bytes,
                );
                
                match batch_result {
                    Ok((hw, message_set)) => {
                        debug!("Legacy fetch v{}: offset={}, hw={}, has_messageset={}", 
                            header.request_api_version, partition.fetch_offset, hw, message_set.is_some());
                        // Always return Some(Bytes), never None
                        (0, hw, Some(message_set.unwrap_or_else(Bytes::new)))
                    },
                    Err(e) => {
                        debug!("Fetch error: {:?}", e);
                        (3, 0, Some(Bytes::new())) // UnknownTopicOrPartition
                    }
                }
            };
            
            let mut partition_data = PartitionData::default();
            partition_data.partition_index = partition.partition;
            partition_data.error_code = error_code;
            partition_data.high_watermark = high_watermark;
            partition_data.last_stable_offset = high_watermark;
            partition_data.log_start_offset = 0;
            partition_data.aborted_transactions = None;
            partition_data.records = records_bytes;
            
            partition_responses.push(partition_data);
        }
        
        let mut topic_response = FetchableTopicResponse::default();
        topic_response.topic = TopicName::from(StrBytes::from(topic_name));
        topic_response.topic_id = Default::default();
        topic_response.partitions = partition_responses;
        
        responses.push(topic_response);
    }
    
    let mut response = FetchResponse::default();
    response.throttle_time_ms = 0;
    response.error_code = 0;
    response.session_id = 0;
    response.responses = responses;

    let mut response_header = ResponseHeader::default();
    response_header.correlation_id = header.correlation_id;

    let mut response_buf = BytesMut::new();
    response_header.encode(&mut response_buf, 0)?;
    response.encode(&mut response_buf, header.request_api_version)?;
    
    Ok(response_buf.to_vec())
}

async fn handle_find_coordinator(
    header: &RequestHeader,
    buf: &mut BytesMut,
    _group_manager: Arc<Mutex<ConsumerGroupManager>>,
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    debug!("Handling FindCoordinator request");
    
    let _request = FindCoordinatorRequest::decode(buf, header.request_api_version)?;
    
    // For simplicity, we are always the coordinator
    let mut response = FindCoordinatorResponse::default();
    response.throttle_time_ms = 0;
    response.error_code = 0;
    response.error_message = None;
    response.node_id = BrokerId(0);
    response.host = get_advertised_host().into();
    response.port = 9092;

    let mut response_header = ResponseHeader::default();
    response_header.correlation_id = header.correlation_id;

    let mut response_buf = BytesMut::new();
    response_header.encode(&mut response_buf, 0)?;
    response.encode(&mut response_buf, header.request_api_version)?;
    
    Ok(response_buf.to_vec())
}

async fn handle_join_group(
    header: &RequestHeader,
    buf: &mut BytesMut,
    group_manager: Arc<Mutex<ConsumerGroupManager>>,
    client_host: &str,
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    debug!("Handling JoinGroup request");
    
    let request = JoinGroupRequest::decode(buf, header.request_api_version)?;
    let mut manager = group_manager.lock().await;
    
    let group_id = request.group_id.to_string();
    let member_id = if request.member_id.is_empty() {
        None
    } else {
        Some(request.member_id.to_string())
    };
    let client_id = "kafka-client".to_string(); // Default client id
    let session_timeout_ms = request.session_timeout_ms;
    let rebalance_timeout_ms = request.rebalance_timeout_ms;
    
    let protocols: Vec<(String, Bytes)> = request.protocols.iter()
        .map(|p| (p.name.to_string(), p.metadata.clone()))
        .collect();
    
    let result = manager.join_group(
        group_id,
        member_id,
        client_id,
        client_host.to_string(),
        session_timeout_ms,
        rebalance_timeout_ms,
        request.protocol_type.to_string(),
        protocols,
    );
    
    let mut response = JoinGroupResponse::default();
    
    match result {
        Ok(join_result) => {
            response.error_code = join_result.error_code;
            response.generation_id = join_result.generation_id;
            response.protocol_type = Some(request.protocol_type.clone());
            response.protocol_name = Some(join_result.protocol_name.into());
            response.leader = join_result.leader_id.clone().into();
            response.member_id = join_result.member_id.into();
            response.members = join_result.members.into_iter().map(|(id, metadata)| {
                let mut member = JoinGroupResponseMember::default();
                member.member_id = id.into();
                member.metadata = metadata;
                member
            }).collect();
        }
        Err(e) => {
            response.error_code = error_code_from_group_error(&e);
        }
    }
    
    response.throttle_time_ms = 0;

    let mut response_header = ResponseHeader::default();
    response_header.correlation_id = header.correlation_id;

    let mut response_buf = BytesMut::new();
    response_header.encode(&mut response_buf, 0)?;
    response.encode(&mut response_buf, header.request_api_version)?;
    
    Ok(response_buf.to_vec())
}

async fn handle_sync_group(
    header: &RequestHeader,
    buf: &mut BytesMut,
    group_manager: Arc<Mutex<ConsumerGroupManager>>,
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    debug!("Handling SyncGroup request");
    
    let request = SyncGroupRequest::decode(buf, header.request_api_version)?;
    let mut manager = group_manager.lock().await;
    
    let group_id = request.group_id.to_string();
    let generation_id = request.generation_id;
    let member_id = request.member_id.to_string();
    
    let assignments: HashMap<String, Bytes> = request.assignments.into_iter()
        .map(|a| (a.member_id.to_string(), a.assignment))
        .collect();
    
    let result = manager.sync_group(&group_id, generation_id, &member_id, assignments);
    
    let mut response = SyncGroupResponse::default();
    response.throttle_time_ms = 0;
    
    match result {
        Ok(assignment) => {
            response.error_code = 0;
            response.assignment = assignment;
        }
        Err(e) => {
            response.error_code = error_code_from_group_error(&e);
            // Even on error, kafka-python expects valid ConsumerProtocol format with version prefix
            let empty_assignment = {
                let mut buf = BytesMut::new();
                buf.extend_from_slice(&[0, 0]); // version 0
                buf.extend_from_slice(&[0, 0, 0, 0]); // 0 topics
                buf.extend_from_slice(&[0, 0, 0, 0]); // 0 user data
                buf.freeze()
            };
            response.assignment = empty_assignment;
        }
    }

    let mut response_header = ResponseHeader::default();
    response_header.correlation_id = header.correlation_id;

    let mut response_buf = BytesMut::new();
    response_header.encode(&mut response_buf, 0)?;
    response.encode(&mut response_buf, header.request_api_version)?;
    
    Ok(response_buf.to_vec())
}

async fn handle_heartbeat(
    header: &RequestHeader,
    buf: &mut BytesMut,
    group_manager: Arc<Mutex<ConsumerGroupManager>>,
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    debug!("Handling Heartbeat request");
    
    let request = HeartbeatRequest::decode(buf, header.request_api_version)?;
    let mut manager = group_manager.lock().await;
    
    let result = manager.heartbeat(
        &request.group_id.to_string(),
        request.generation_id,
        &request.member_id.to_string(),
    );
    
    let mut response = HeartbeatResponse::default();
    response.throttle_time_ms = 0;
    response.error_code = match result {
        Ok(()) => 0,
        Err(e) => error_code_from_group_error(&e),
    };

    let mut response_header = ResponseHeader::default();
    response_header.correlation_id = header.correlation_id;

    let mut response_buf = BytesMut::new();
    response_header.encode(&mut response_buf, 0)?;
    response.encode(&mut response_buf, header.request_api_version)?;
    
    Ok(response_buf.to_vec())
}

async fn handle_offset_commit(
    header: &RequestHeader,
    buf: &mut BytesMut,
    group_manager: Arc<Mutex<ConsumerGroupManager>>,
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    debug!("Handling OffsetCommit request");
    
    let request = OffsetCommitRequest::decode(buf, header.request_api_version)?;
    let mut manager = group_manager.lock().await;
    
    let group_id = request.group_id.to_string();
    let generation_id = request.generation_id_or_member_epoch;
    
    let mut offsets = Vec::new();
    for topic in &request.topics {
        let topic_name = topic.name.to_string();
        for partition in &topic.partitions {
            offsets.push((topic_name.clone(), partition.partition_index, partition.committed_offset));
        }
    }
    
    let result = manager.commit_offsets(&group_id, generation_id, offsets).await;
    
    let mut response = OffsetCommitResponse::default();
    response.throttle_time_ms = 0;
    
    let error_code = match result {
        Ok(()) => 0,
        Err(e) => error_code_from_group_error(&e),
    };
    
    response.topics = request.topics.into_iter().map(|topic| {
        let mut topic_response = OffsetCommitResponseTopic::default();
        topic_response.name = topic.name;
        topic_response.partitions = topic.partitions.into_iter().map(|partition| {
            let mut partition_response = OffsetCommitResponsePartition::default();
            partition_response.partition_index = partition.partition_index;
            partition_response.error_code = error_code;
            partition_response
        }).collect();
        topic_response
    }).collect();

    let mut response_header = ResponseHeader::default();
    response_header.correlation_id = header.correlation_id;

    let mut response_buf = BytesMut::new();
    response_header.encode(&mut response_buf, 0)?;
    response.encode(&mut response_buf, header.request_api_version)?;
    
    Ok(response_buf.to_vec())
}

async fn handle_offset_fetch(
    header: &RequestHeader,
    buf: &mut BytesMut,
    group_manager: Arc<Mutex<ConsumerGroupManager>>,
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    debug!("Handling OffsetFetch request");
    
    let request = OffsetFetchRequest::decode(buf, header.request_api_version)?;
    let manager = group_manager.lock().await;
    
    let group_id = request.group_id.to_string();
    
    let topics: Vec<(String, Vec<i32>)> = if let Some(topics) = request.topics {
        topics.into_iter().map(|topic| {
            let topic_name = topic.name.to_string();
            let partitions = topic.partition_indexes;
            (topic_name, partitions)
        }).collect()
    } else {
        Vec::new()
    };
    
    let fetched_offsets = manager.fetch_offsets(&group_id, topics);
    
    let mut response = OffsetFetchResponse::default();
    response.throttle_time_ms = 0;
    response.error_code = 0;
    
    response.topics = fetched_offsets.into_iter().map(|(topic_name, partitions)| {
        let mut topic_response = OffsetFetchResponseTopic::default();
        topic_response.name = TopicName::from(StrBytes::from(topic_name));
        topic_response.partitions = partitions.into_iter().map(|(partition, offset)| {
            let mut partition_response = OffsetFetchResponsePartition::default();
            partition_response.partition_index = partition;
            partition_response.committed_offset = offset;
            partition_response.metadata = None;
            partition_response.error_code = 0;
            partition_response
        }).collect();
        topic_response
    }).collect();

    let mut response_header = ResponseHeader::default();
    response_header.correlation_id = header.correlation_id;

    let mut response_buf = BytesMut::new();
    response_header.encode(&mut response_buf, 0)?;
    response.encode(&mut response_buf, header.request_api_version)?;
    
    Ok(response_buf.to_vec())
}

async fn handle_leave_group(
    header: &RequestHeader,
    buf: &mut BytesMut,
    group_manager: Arc<Mutex<ConsumerGroupManager>>,
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    debug!("Handling LeaveGroup request");
    
    let request = LeaveGroupRequest::decode(buf, header.request_api_version)?;
    let mut manager = group_manager.lock().await;
    
    let group_id = request.group_id.to_string();
    
    let mut response = LeaveGroupResponse::default();
    response.throttle_time_ms = 0;
    
    // Handle single member_id (older versions)
    if !request.member_id.is_empty() {
        let result = manager.leave_group(&group_id, &request.member_id.to_string());
        response.error_code = match result {
            Ok(()) => 0,
            Err(e) => error_code_from_group_error(&e),
        };
    } else if !request.members.is_empty() {
        // Handle multiple members (newer versions)
        response.members = request.members.into_iter().map(|member| {
            let result = manager.leave_group(&group_id, &member.member_id.to_string());
            let mut member_response = MemberResponse::default();
            member_response.member_id = member.member_id;
            member_response.error_code = match result {
                Ok(()) => 0,
                Err(e) => error_code_from_group_error(&e),
            };
            member_response
        }).collect();
        response.error_code = 0;
    }

    let mut response_header = ResponseHeader::default();
    response_header.correlation_id = header.correlation_id;

    let mut response_buf = BytesMut::new();
    response_header.encode(&mut response_buf, 0)?;
    response.encode(&mut response_buf, header.request_api_version)?;
    
    Ok(response_buf.to_vec())
}

async fn handle_list_offsets(
    header: &RequestHeader,
    buf: &mut BytesMut,
    storage: Arc<Mutex<InMemoryStorage>>,
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    debug!("Handling ListOffsets request version {}", header.request_api_version);
    
    let storage = storage.lock().await;
    
    if header.request_api_version == 0 {
        // Manual parsing for v0 due to kafka-protocol-rs decode issues
        // v0 format: replica_id(4) + topics array
        
        let _replica_id = buf.get_i32();
        let topics_len = buf.get_i32();
        
        let mut response = ListOffsetsResponse::default();
        let mut topic_responses = Vec::new();
        
        for _ in 0..topics_len {
            let topic_name_len = buf.get_i16() as usize;
            let mut topic_name_bytes = vec![0u8; topic_name_len];
            buf.copy_to_slice(&mut topic_name_bytes);
            let topic_name = String::from_utf8_lossy(&topic_name_bytes).to_string();
            
            let partitions_len = buf.get_i32();
            let mut partition_responses = Vec::new();
            
            for _ in 0..partitions_len {
                let partition_index = buf.get_i32();
                let timestamp = buf.get_i64();
                let _max_offsets = buf.get_i32(); // ignored in v0
                
                let mut partition_response = ListOffsetsPartitionResponse::default();
                partition_response.partition_index = partition_index;
                
                let offset = if timestamp == -2 {
                    0 // Earliest
                } else {
                    match storage.get_high_watermark(&topic_name, partition_index) {
                        Ok(hw) => hw,
                        Err(_) => {
                            partition_response.error_code = 3; // UnknownTopicOrPartition
                            0
                        }
                    }
                };
                
                partition_response.error_code = 0;
                partition_response.old_style_offsets = vec![offset];
                
                partition_responses.push(partition_response);
            }
            
            let mut topic_response = ListOffsetsTopicResponse::default();
            topic_response.name = TopicName::from(StrBytes::from(topic_name));
            topic_response.partitions = partition_responses;
            
            topic_responses.push(topic_response);
        }
        
        response.topics = topic_responses;
        
        let mut response_header = ResponseHeader::default();
        response_header.correlation_id = header.correlation_id;

        let mut response_buf = BytesMut::new();
        response_header.encode(&mut response_buf, 0)?;
        response.encode(&mut response_buf, 0)?;
        
        Ok(response_buf.to_vec())
    } else {
        // For newer versions, use the standard decode
        let request = ListOffsetsRequest::decode(buf, header.request_api_version)?;
        
        let mut response = ListOffsetsResponse::default();
        response.throttle_time_ms = 0;
        
        let mut topic_responses = Vec::new();
        
        for topic in request.topics {
            let topic_name = topic.name.to_string();
            let mut partition_responses = Vec::new();
            
            for partition in topic.partitions {
                let mut partition_response = ListOffsetsPartitionResponse::default();
                partition_response.partition_index = partition.partition_index;
                
                let offset = if partition.timestamp == -2 {
                    0
                } else {
                    match storage.get_high_watermark(&topic_name, partition.partition_index) {
                        Ok(hw) => hw,
                        Err(_) => {
                            partition_response.error_code = 3;
                            0
                        }
                    }
                };
                
                partition_response.error_code = 0;
                partition_response.timestamp = partition.timestamp;
                partition_response.offset = offset;
                
                partition_responses.push(partition_response);
            }
            
            let mut topic_response = ListOffsetsTopicResponse::default();
            topic_response.name = TopicName::from(StrBytes::from(topic_name));
            topic_response.partitions = partition_responses;
            
            topic_responses.push(topic_response);
        }
        
        response.topics = topic_responses;

        let mut response_header = ResponseHeader::default();
        response_header.correlation_id = header.correlation_id;

        let mut response_buf = BytesMut::new();
        response_header.encode(&mut response_buf, 0)?;
        response.encode(&mut response_buf, header.request_api_version)?;
        
        Ok(response_buf.to_vec())
    }
}

fn error_code_from_group_error(error: &GroupError) -> i16 {
    match error {
        GroupError::UnknownGroup => 15,           // UNKNOWN_GROUP
        GroupError::UnknownMember => 25,          // UNKNOWN_MEMBER_ID
        GroupError::IllegalGeneration => 22,      // ILLEGAL_GENERATION
        GroupError::InconsistentGroupProtocol => 23, // INCONSISTENT_GROUP_PROTOCOL
    }
}
