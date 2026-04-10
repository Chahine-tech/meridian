{ pkgs, craneBuilds }:
pkgs.mkShell {
  name = "meridian-dev";

  nativeBuildInputs = with pkgs; [
    # Rust toolchain: stable + clippy + rustfmt + rust-src + wasm32 target
    craneBuilds.rustToolchain

    # Required by aws-lc-sys (crypto backend for rustls 0.23)
    cmake
    pkg-config

    # Rust dev tools
    cargo-watch   # `cargo watch -x run` for dev reload
    cargo-nextest # faster parallel test runner
    wasm-pack     # build / check the edge wasm crate locally

    # TypeScript / Bun — all TS package operations
    bun

    # PostgreSQL client tools (psql, pg_isready, pg_dump)
    # Not a server — use docker-compose.yml for the actual Postgres instance
    postgresql

    # Redis client tools (redis-cli)
    # Not a server — use docker-compose.yml for a Redis instance
    redis
  ];

  buildInputs = with pkgs; [ openssl ];

  shellHook = ''
    # Help rust-analyzer locate the standard library sources
    export RUST_SRC_PATH="${craneBuilds.rustToolchain}/lib/rustlib/src/rust/library"

    # Point pkg-config at the Nix-provided OpenSSL (required on macOS which
    # has no system openssl in a standard location)
    export PKG_CONFIG_PATH="${pkgs.openssl.dev}/lib/pkgconfig"

    echo "Meridian dev shell"
    echo "  cargo build                        — server (sled, default)"
    echo "  cargo build --features storage-postgres"
    echo "  cargo build --features cluster"
    echo "  cargo watch -x run                 — dev server with auto-reload"
    echo "  cargo check -p meridian-edge --target wasm32-unknown-unknown"
    echo "  bun install && bun test            — TypeScript SDK tests"
    echo "  docker compose up                  — start sled server"
    echo "  docker compose --profile pg up     — start with PostgreSQL"
  '';
}
