# Rustka

A lightweight, in-memory Kafka-compatible message broker written in Rust. Designed as a drop-in replacement for Apache Kafka in development environments and simple on-premise deployments like Sentry.

## Key Features

- **Zero Dependencies**: No JVM, Zookeeper, or external services required
- **Kafka Protocol Compatible**: Works with standard Kafka clients  
- **Lightweight**: Minimal resource footprint, perfect for development
- **Fast Startup**: Ready in milliseconds, not seconds
- **Tested with Sentry**: Extensively tested for Sentry compatibility

## Supported Kafka APIs

Rustka implements the core Kafka wire protocol APIs needed for most applications:

- Metadata with auto-topic creation
- Produce/Fetch for message publishing and consumption  
- Consumer Groups with proper rebalancing and offset management
- ListOffsets for offset queries
- Full API version negotiation
- SASL/PLAIN authentication support

## Quick Start

```bash
# Build and run
cargo build --release
cargo run --release

# Rustka listens on port 9092 by default
# Dashboard available at http://localhost:8080
```

## Configuration

### Environment Variables

- **`KAFKA_ADVERTISED_HOST`**: The hostname/IP that clients should use to connect
  - Default: Automatically detects the primary network interface IP
  - In Kubernetes: Set to your Service name (e.g., `rustka-service.default.svc.cluster.local`)
  - In Docker: Set to the container name or host IP
  - For local development: Defaults to your machine's IP (e.g., `192.168.1.100`)

- **`RUST_LOG`**: Controls log verbosity
  - Default: `info`
  - Example: `RUST_LOG=debug` for debug logging
  - Example: `RUST_LOG=rustka=debug,kafka_protocol=info` for selective logging

- **`MALLOC_CONF`**: jemalloc memory allocator configuration
  - Default: Automatically set to `dirty_decay_ms:10000,muzzy_decay_ms:10000`
  - Controls when memory is returned to the OS (10 seconds by default)
  - Note: This is set automatically by Rustka, but can be overridden

### Fixed Configuration

The following values are currently hardcoded but may become configurable in future versions:

- **Kafka Port**: `9092` (standard Kafka port)
- **Dashboard Port**: `8080` 
- **Message Retention**: 5 minutes
- **Max Messages per Partition**: 1,000
- **Consumer Group Offset Retention**: 1 hour (5 minutes for orphaned offsets)
- **Empty Group Retention**: 10 minutes
- **Default Partitions per Topic**: 3

Example:
```bash
# Local development (auto-detects IP)
cargo run --release

# With debug logging
RUST_LOG=debug cargo run --release

# Kubernetes
KAFKA_ADVERTISED_HOST=rustka-service.default.svc.cluster.local cargo run --release

# Docker
KAFKA_ADVERTISED_HOST=rustka-container cargo run --release

# Force specific IP/hostname
KAFKA_ADVERTISED_HOST=10.0.0.5 cargo run --release
```

## Testing

The project includes a comprehensive test suite written in Python:

```bash
# Install dependencies
pip install kafka-python

# Run all tests
python examples/run_all_tests.py
```

## Usage with Sentry

Rustka has been specifically tested to work as a Kafka replacement for on-premise Sentry deployments. Simply point your Sentry configuration to use `localhost:9092` instead of your Kafka cluster.

## Dashboard

Rustka includes a web dashboard for monitoring and management, available at `http://localhost:8080`:

### Metrics
- **Broker Stats**: Uptime, total messages produced/fetched, active connections
- **Topic Metrics**: Messages per topic/partition, high watermarks, bytes in/out
- **Consumer Groups**: Active groups, state, members, and lag information
- **Storage Stats**: Total messages in memory, estimated memory usage

### Cleanup Operations
- **Remove Empty Topics**: Deletes topics with no messages
- **Clear All Messages**: Removes all messages from all topics (keeps topic structure)

## API Endpoints

The dashboard exposes several REST API endpoints:

- `GET /api/metrics` - Returns all broker metrics in JSON format
- `POST /api/cleanup/empty-topics` - Removes topics with no messages
- `POST /api/cleanup/all-messages` - Clears all messages from all topics
- `GET /api/memory-stats` - Returns detailed memory statistics

## Architecture

Rustka is an in-memory message broker that implements the Kafka wire protocol. It automatically creates topics with 3 partitions when first accessed and handles consumer group coordination without external dependencies.

### Memory Management

Rustka uses jemalloc as the memory allocator for better performance and memory control. The system includes:

#### Automatic Retention
- **Message Retention**: 5 minutes (messages older than this are automatically removed)
- **Max Messages per Partition**: 1,000 messages (oldest messages are dropped when exceeded)
- **Consumer Group Offset Retention**: 1 hour for active groups, 5 minutes for orphaned offsets
- **Empty Group Retention**: 10 minutes (empty groups are cleaned up after this period)

#### Memory Behavior
- Freed memory may not immediately return to the OS due to allocator behavior
- jemalloc is configured to return memory after 10 seconds of inactivity
- Memory usage can be monitored via the dashboard's memory stats endpoint

## Acknowledgments

This project wouldn't have been possible without the excellent [kafka-protocol-rs](https://github.com/tychedelia/kafka-protocol-rs) library, which provides the foundation for Kafka protocol parsing and serialization.

## License

MIT