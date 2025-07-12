# Dockerfile for local builds (GitHub Action uses a dynamic Dockerfile)
FROM rust:1-slim-bookworm AS builder

# Install build dependencies
RUN apt-get update && apt-get install -y \
    musl-tools \
    && rm -rf /var/lib/apt/lists/*

# Add musl target
RUN rustup target add x86_64-unknown-linux-musl

WORKDIR /app

# Copy configuration files
COPY Cargo.toml Cargo.lock ./

# Copy source code
COPY src ./src

# Build in release mode with musl for static binary
RUN cargo build --release --target x86_64-unknown-linux-musl

# Final minimal image
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

# Copy compiled binary
COPY --from=builder /app/target/x86_64-unknown-linux-musl/release/rustka /usr/local/bin/rustka

# Expose Kafka port
EXPOSE 9092

ENTRYPOINT ["/usr/local/bin/rustka"]
