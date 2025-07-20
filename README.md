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
```

## Configuration

Rustka uses environment variables for configuration:

- `KAFKA_ADVERTISED_HOST`: The hostname/IP that clients should use to connect
  - Default: Automatically detects the primary network interface IP
  - In Kubernetes: Set to your Service name (e.g., `rustka-service.default.svc.cluster.local`)
  - In Docker: Set to the container name or host IP
  - For local development: Defaults to your machine's IP (e.g., `192.168.1.100`)

Example:
```bash
# Local development (auto-detects IP)
cargo run --release

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

## Architecture

Rustka is an in-memory message broker that implements the Kafka wire protocol. It automatically creates topics with 3 partitions when first accessed and handles consumer group coordination without external dependencies.

## Acknowledgments

This project wouldn't have been possible without the excellent [kafka-protocol-rs](https://github.com/tychedelia/kafka-protocol-rs) library, which provides the foundation for Kafka protocol parsing and serialization.

## License

MIT