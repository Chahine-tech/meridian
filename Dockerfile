# ---- Build stage ----
FROM rust:slim AS builder

WORKDIR /build

# Install musl for static binary
RUN apt-get update && apt-get install -y musl-tools && rm -rf /var/lib/apt/lists/*
RUN rustup target add x86_64-unknown-linux-musl

# Cache dependencies — copy manifests first
COPY Cargo.toml Cargo.lock ./
COPY server/Cargo.toml server/Cargo.toml
RUN mkdir -p server/src server/benches && echo "fn main() {}" > server/src/main.rs && echo "fn main() {}" > server/benches/crdt.rs
RUN cargo build --release --target x86_64-unknown-linux-musl 2>/dev/null || true

# Build the real binary
COPY server/src server/src
RUN touch server/src/main.rs && \
    cargo build --release --target x86_64-unknown-linux-musl

# ---- Runtime stage ----
FROM scratch

COPY --from=builder /build/target/x86_64-unknown-linux-musl/release/meridian /meridian

VOLUME ["/data"]
EXPOSE 3000

ENV MERIDIAN_BIND=0.0.0.0:3000
ENV MERIDIAN_DATA_DIR=/data

ENTRYPOINT ["/meridian"]
