# ---- Build stage ----
FROM rust:bookworm AS builder

WORKDIR /build

# Build arg — controls which feature set to compile.
# Values: "" (default, sled), "cluster", "cluster-http", "pg-sync"
ARG FEATURES=""

# System deps — cmake + perl needed by ring (crypto), pkg-config for openssl
RUN apt-get update && apt-get install -y \
    cmake \
    perl \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# ---------------------------------------------------------------------------
# Dependency cache layer — copy all manifests + stub sources so that
# `cargo build` caches deps without rebuilding when only src changes.
# ---------------------------------------------------------------------------
COPY Cargo.toml Cargo.lock ./

COPY server/Cargo.toml                          server/Cargo.toml
COPY crates/meridian-core/Cargo.toml            crates/meridian-core/Cargo.toml
COPY crates/meridian-storage/Cargo.toml         crates/meridian-storage/Cargo.toml
COPY crates/meridian-cluster/Cargo.toml         crates/meridian-cluster/Cargo.toml
COPY crates/meridian-edge/Cargo.toml            crates/meridian-edge/Cargo.toml
COPY crates/meridian-client/Cargo.toml          crates/meridian-client/Cargo.toml

# Stub sources — only what cargo needs to resolve and cache dependencies.
# We stub all crates as empty libs; the bench stub avoids "file not found" errors.
# ratatui-game is a workspace member but excluded from .dockerignore — stub it inline.
RUN mkdir -p \
    server/src \
    server/benches \
    crates/meridian-core/src \
    crates/meridian-storage/src \
    crates/meridian-cluster/src \
    crates/meridian-edge/src \
    crates/meridian-client/src \
    examples/ratatui-game/src \
    && echo ''              > server/src/lib.rs \
    && echo 'fn main() {}' > server/src/main.rs \
    && echo 'fn main() {}' > server/benches/crdt.rs \
    && echo ''              > crates/meridian-core/src/lib.rs \
    && echo ''              > crates/meridian-storage/src/lib.rs \
    && echo ''              > crates/meridian-cluster/src/lib.rs \
    && echo ''              > crates/meridian-edge/src/lib.rs \
    && echo ''              > crates/meridian-client/src/lib.rs \
    && printf '[package]\nname = "ratatui-game"\nversion = "0.1.0"\nedition = "2021"\n' \
       > examples/ratatui-game/Cargo.toml \
    && echo 'fn main() {}' > examples/ratatui-game/src/main.rs

# Cache deps only — ignore errors from stub sources
RUN cargo fetch

# ---------------------------------------------------------------------------
# Real build — copy actual sources and recompile only changed crates
# ---------------------------------------------------------------------------
COPY server/src                         server/src
COPY crates/meridian-core/src           crates/meridian-core/src
COPY crates/meridian-storage/src        crates/meridian-storage/src
COPY crates/meridian-cluster/src        crates/meridian-cluster/src
COPY crates/meridian-edge/src           crates/meridian-edge/src

RUN touch server/src/main.rs && \
    if [ -n "$FEATURES" ]; then \
        cargo build --release -p meridian-server --features "$FEATURES"; \
    else \
        cargo build --release -p meridian-server; \
    fi

# ---- Runtime stage ----
# debian:slim matches the build stage — same glibc, no musl needed.
# Much smaller than the full rust image while keeping all shared libs.
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y \
    ca-certificates \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/target/release/meridian /meridian

VOLUME ["/data"]
EXPOSE 3000

ENV MERIDIAN_BIND=0.0.0.0:3000
ENV MERIDIAN_DATA_DIR=/data

HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
    CMD curl -f http://localhost:3000/health/live || exit 1

ENTRYPOINT ["/meridian"]
