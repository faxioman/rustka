# Rustka - Context for Claude

## Overview
Rustka is a minimal Kafka broker implementation in Rust that provides compatibility with Kafka clients (especially kafka-python and confluent-kafka). It's designed to be lightweight and suitable for testing environments like Sentry's.

## Architecture

### Core Components
- **broker.rs**: Main TCP server handling Kafka protocol requests
- **storage.rs**: In-memory storage with retention and cleanup
- **consumer_group.rs**: Consumer group coordination with offset management
- **dashboard.rs**: Web dashboard for monitoring and management (port 8080)
- **metrics.rs**: Metrics collection and reporting system

### Key Features
- Kafka wire protocol compatibility (supports kafka-python, confluent-kafka)
- Consumer groups with rebalancing (fixed and working)
- Offset management
- SASL/PLAIN authentication (accepts any credentials)
- Commit log persistence
- Multi-partition support
- RecordBatch format support with headers (Kafka 0.11+)

## Testing
```bash
# Run all tests
cd examples && python3 run_all_tests.py

# Key tests:
# - test_sentry_compatibility.py: Verifies Sentry's usage patterns work
# - test_consumer_groups.py: Tests consumer group rebalancing
# - test_headers.py: Tests Kafka headers support
# - test_authentication.py: Tests SASL/PLAIN authentication
```

## Common Commands
```bash
# Build and run
cargo build --release
./target/release/rustka

# Run with debug logging
RUST_LOG=debug ./target/release/rustka

# Quick test
python3 examples/test_minimal.py
```

## Recent Fixes
- ✅ Consumer group rebalancing: Fixed REBALANCE_IN_PROGRESS loop by never returning this error from JoinGroup
- ✅ Headers support: Added Produce v3 and Fetch v11 support for headers
- ✅ Authentication tests: Adjusted to match Rustka's behavior (accepts any credentials)

## Important Notes

### Consumer Group Protocol

#### How Kafka Protocol Should Work (Proper Implementation)
According to the official Kafka protocol, JoinGroup should **NEVER** return REBALANCE_IN_PROGRESS. Instead:

1. **When group is in PreparingRebalance state**:
   - JoinGroup requests should **BLOCK** (not return immediately)
   - The broker waits for all members to join OR timeout to expire
   - Only then it sends JoinGroup responses to all members simultaneously
   - The leader gets the full member list, others get empty list

2. **Proper state machine**:
   - Empty → member joins → PreparingRebalance → all members joined/timeout → AwaitingSync → all synced → Stable
   - During PreparingRebalance, new JoinGroup requests are queued, not rejected

3. **Why blocking is required**:
   - Ensures all members participate in rebalance
   - Prevents "rebalance storms" where members continuously retry
   - Provides consistency in partition assignment

#### Our Current Implementation (Simplified)
Due to architectural limitations (synchronous request/response model), we implemented a simplified version:

1. **No blocking**: JoinGroup always returns immediately
2. **Instant rebalancing**: When a member joins, we complete rebalance instantly
3. **No REBALANCE_IN_PROGRESS errors**: We never return this error from JoinGroup
4. **State transitions are immediate**: PreparingRebalance → AwaitingSync happens instantly

**Consequences**:
- Works with real Kafka clients (Sentry, librdkafka, etc.)
- Not 100% protocol compliant
- May have edge cases with very large consumer groups
- Rebalancing is less coordinated than real Kafka

#### Future Implementation Requirements
To implement proper Kafka protocol rebalancing:

1. **Async request handling**: Broker must support holding requests without responding
2. **Request queueing**: Store pending JoinGroup requests per group
3. **Timeout management**: Track rebalance timeout per group
4. **Batch responses**: Send all JoinGroup responses simultaneously

Example structure needed:
```rust
struct PendingJoinRequest {
    member_id: String,
    request_data: JoinGroupRequest,
    response_channel: oneshot::Sender<JoinGroupResponse>,
}

struct ConsumerGroup {
    pending_joins: Vec<PendingJoinRequest>,
    rebalance_deadline: Instant,
    // ... other fields
}
```

The current implementation works well for testing and development but would need significant refactoring for production use with strict Kafka protocol compliance.

### Client Library Behaviors
- kafka-python and librdkafka may create multiple connection attempts during initialization
- Each connection attempt may register as a new member if not handled correctly
- This is due to our implementation not correctly managing the consumer group protocol
- We need to properly implement the state machine and member management

### Testing Tips
- kafka-python requires proper RecordBatch encoding
- Always include correlation_id in responses
- Consumer groups need proper state machine (Empty → PreparingRebalance → AwaitingSync → Stable)
- Use `eprintln!` for debug output that won't interfere with protocol

## Architecture Decisions
- Async Tokio for networking
- In-memory storage by default (fast for testing)
- Optional commit log for persistence
- Minimal dependencies (only kafka-protocol for wire format)
- No external dependencies like Zookeeper

## Known Limitations
- Single broker only (no cluster support)
- No replication
- No transactions
- No ACLs beyond basic SASL
- Consumer group rebalancing is simplified (immediate completion)

## Debugging
```bash
# Enable debug logging
RUST_LOG=debug ./target/release/rustka

# Common issues:
# - "Connection refused": Check if broker is running on :9092
# - "Unknown member": Consumer group state issue, check rebalancing
# - Encoding errors: Check RecordBatch format and API versions
```

## Testing Requirements
- **NEVER use kafka-python for testing**
- Use confluent-kafka (librdkafka) for all Python tests instead
- kafka-python creates multiple connections during initialization which confuses our consumer group implementation

## Code Guidelines
- **DO NOT add unnecessary comments** - Code should be self-documenting
- Only add comments for complex algorithms or non-obvious business logic
- Avoid obvious comments like "// Increment counter" or "// Return result"
- Prefer clear variable/function names over explanatory comments
- Keep comments concise and meaningful when truly needed
- **ALL COMMENTS MUST BE IN ENGLISH** - No Italian comments in code
- **Test Guidelines**:
  - Python tests: ONLY for Kafka client integration testing
  - Rust tests: For all other testing (unit tests, internal logic, non-Kafka features)
  - Dashboard tests, retention tests, etc. should be in Rust, not Python
  - If tests have issues with corrupt/invalid data, the test MUST fail - don't hide errors in tests

## Important Rules
- **NEVER make git commits** - The user will ALWAYS make commits themselves when they decide
- **NEVER hide test failures** - If something is wrong, tests must fail visibly
- **NEVER assume client libraries (librdkafka, kafka-python, confluent-kafka, etc.) are doing something wrong** - These are mature, battle-tested implementations. If there's a compatibility issue, the error is CERTAINLY in Rustka's implementation
- **NO "phantom connections" exist** - If multiple members are being created, it's because Rustka is not correctly implementing the consumer group protocol
- **IF SOMETHING WORKS ON REDPANDA BUT NOT ON RUSTKA, IT'S ALWAYS A BUG IN RUSTKA** - NEVER blame the client, NEVER make excuses. If Redpanda handles it correctly, Rustka MUST handle it correctly too. THE BUG IS ALWAYS IN RUSTKA.