# ---- Build stage ----
FROM rust:slim AS builder

WORKDIR /build

# Install musl for static binary
RUN apt-get update && apt-get install -y musl-tools && rm -rf /var/lib/apt/lists/*
RUN rustup target add x86_64-unknown-linux-musl

# Build arg — set to "cluster" or "cluster-http" to enable multinode mode.
# Default: empty (single-node, sled storage).
ARG FEATURES=""

# Cache dependencies — copy manifests and stub sources first
COPY Cargo.toml Cargo.lock ./
COPY server/Cargo.toml server/Cargo.toml
COPY crates/meridian-storage/Cargo.toml crates/meridian-storage/Cargo.toml
COPY crates/meridian-cluster/Cargo.toml crates/meridian-cluster/Cargo.toml

RUN mkdir -p \
    server/src \
    server/benches \
    crates/meridian-storage/src \
    crates/meridian-cluster/src \
    && echo "fn main() {}" > server/src/main.rs \
    && echo "fn main() {}" > server/benches/crdt.rs \
    && echo "" > crates/meridian-storage/src/lib.rs \
    && echo "" > crates/meridian-cluster/src/lib.rs

RUN if [ -n "$FEATURES" ]; then \
        cargo build --release --target x86_64-unknown-linux-musl --features "$FEATURES" 2>/dev/null || true; \
    else \
        cargo build --release --target x86_64-unknown-linux-musl 2>/dev/null || true; \
    fi

# Build the real binary
COPY server/src server/src
COPY crates/meridian-storage/src crates/meridian-storage/src
COPY crates/meridian-cluster/src crates/meridian-cluster/src

RUN touch server/src/main.rs && \
    if [ -n "$FEATURES" ]; then \
        cargo build --release --target x86_64-unknown-linux-musl --features "$FEATURES"; \
    else \
        cargo build --release --target x86_64-unknown-linux-musl; \
    fi

# ---- Runtime stage ----
FROM scratch

COPY --from=builder /build/target/x86_64-unknown-linux-musl/release/meridian /meridian

VOLUME ["/data"]
EXPOSE 3000

ENV MERIDIAN_BIND=0.0.0.0:3000
ENV MERIDIAN_DATA_DIR=/data

ENTRYPOINT ["/meridian"]
