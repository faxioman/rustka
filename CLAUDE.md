# Rustka - Context for Claude

## Overview
Rustka is a minimal Kafka broker implementation in Rust that provides compatibility with Kafka clients (especially kafka-python). It's designed to be lightweight and suitable for testing environments like Sentry's.

## Architecture

### Core Components
- **broker.rs**: Main TCP server handling Kafka protocol requests
- **storage.rs**: In-memory storage with retention and cleanup
- **consumer_group.rs**: Consumer group coordination with offset management
- **dashboard.rs**: Web dashboard for monitoring and management (port 8080)
- **metrics.rs**: Metrics collection and reporting system

### Key Features
- Kafka wire protocol compatibility (supports kafka-python)
- Consumer groups with rebalancing
- Offset management
- SASL/PLAIN authentication
- Commit log persistence
- Multi-partition support
- RecordBatch format support (Kafka 0.11+)

## Testing
```bash
# Run all tests
cd examples && python3 run_all_tests.py

# Key tests:
# - test_sentry_compatibility.py: Verifies Sentry's usage patterns work
# - test_consumer_groups.py: Tests consumer group rebalancing
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

## Recent Work
- Fixed consumer group protocol to properly handle rebalancing
- Consumer groups now correctly:
  - Elect a single leader per generation
  - Distribute partitions among members
  - Handle member joins/leaves with proper rebalancing
  - Wait for members during rebalance (1 second timeout)

## Important Notes

### Consumer Group Protocol
The Kafka consumer group protocol works as follows:
1. First consumer joins → becomes leader
2. New consumer joins → triggers rebalance
3. During rebalance: all members rejoin with new generation
4. Leader receives member list and calculates assignments
5. Leader sends assignments via SyncGroup
6. Each member receives only its own assignment

### kafka-python Behavior
- Creates many temporary connections during initialization
- Each connection gets a new member ID
- This can cause thousands of members during rebalancing
- Solution: 1-second timeout to collect members before completing rebalance

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

## Code Guidelines
- **DO NOT add unnecessary comments** - Code should be self-documenting
- Only add comments for complex algorithms or non-obvious business logic
- Avoid obvious comments like "// Increment counter" or "// Return result"
- Prefer clear variable/function names over explanatory comments
- Keep comments concise and meaningful when truly needed
- **Test Guidelines**:
  - Python tests: ONLY for Kafka client integration testing
  - Rust tests: For all other testing (unit tests, internal logic, non-Kafka features)
  - Dashboard tests, retention tests, etc. should be in Rust, not Python