{ pkgs, crane }:
let
  # Rust stable with all components needed for dev and CI.
  # rust-src is exposed so rust-analyzer in the devShell finds stdlib sources.
  # wasm32-unknown-unknown is added for `cargo check -p meridian-edge`.
  rustToolchain = pkgs.rust-bin.stable.latest.default.override {
    extensions = [ "rust-src" "clippy" "rustfmt" ];
    targets    = [ "wasm32-unknown-unknown" ];
  };

  craneLib = (crane.mkLib pkgs).overrideToolchain rustToolchain;

  # Filter to only the files cargo needs — avoids rebuilding when docs or
  # TypeScript packages change.
  src = craneLib.cleanCargoSource ../.;

  # Build inputs shared across all variants.
  #
  # cmake:      aws-lc-sys (crypto backend for rustls 0.23) compiles bundled C
  #             and discovers cmake via PATH during its build script.
  # pkg-config: openssl-probe (transitive dep) runs a compile-time detection.
  # openssl:    header files for the above detection pass.
  # macOS:      Security/CoreFoundation/SystemConfiguration for TLS certificate
  #             chain access at link time.
  commonNativeBuildInputs = with pkgs; [ pkg-config cmake ];

  commonBuildInputs = with pkgs; [ openssl ];

  # Packages to build — excludes:
  #   meridian-edge  : cdylib targeting wasm32-unknown-unknown, cannot link natively
  #   meridian-pg    : pgrx extension outside the workspace, separate build system
  workspacePackages = "-p meridian-server -p meridian-core -p meridian-storage -p meridian-cluster -p meridian-client";

  commonArgs = {
    inherit src;
    nativeBuildInputs = commonNativeBuildInputs;
    buildInputs       = commonBuildInputs;
  };

  # Phase 1: compile dependencies only (expensive, Nix-cached across source edits).
  # Each feature variant needs its own artifact set because features control which
  # dependency crates get compiled (e.g. sqlx is absent in the sled variant).

  cargoArtifactsSled = craneLib.buildDepsOnly (commonArgs // {
    pname          = "meridian-deps-sled";
    version        = "0.0.0";
    cargoExtraArgs = "${workspacePackages} --no-default-features --features storage-sled";
  });

  cargoArtifactsPostgres = craneLib.buildDepsOnly (commonArgs // {
    pname          = "meridian-deps-postgres";
    version        = "0.0.0";
    cargoExtraArgs = "${workspacePackages} --no-default-features --features storage-postgres";
  });

  cargoArtifactsCluster = craneLib.buildDepsOnly (commonArgs // {
    pname          = "meridian-deps-cluster";
    version        = "0.0.0";
    cargoExtraArgs = "${workspacePackages} --no-default-features --features storage-sled,cluster";
  });

  cargoArtifactsPgSync = craneLib.buildDepsOnly (commonArgs // {
    pname          = "meridian-deps-pg-sync";
    version        = "0.0.0";
    cargoExtraArgs = "${workspacePackages} --no-default-features --features pg-sync";
  });

  # Phase 2: build the final binary using pre-compiled deps.
  serverVersion = (craneLib.crateNameFromCargoToml { cargoToml = ../server/Cargo.toml; }).version;

  mkMeridianPackage = { pname, features, cargoArtifacts }:
    craneLib.buildPackage (commonArgs // {
      inherit pname cargoArtifacts;
      version = serverVersion;
      cargoExtraArgs = "-p meridian-server --no-default-features --features ${features}";
      meta = {
        description = "Meridian CRDT sync engine (${pname})";
        homepage    = "https://github.com/Chahine-tech/meridian";
        license     = pkgs.lib.licenses.mit;
        mainProgram = "meridian";
      };
    });

in
{
  meridian = mkMeridianPackage {
    pname          = "meridian";
    features       = "storage-sled";
    cargoArtifacts = cargoArtifactsSled;
  };

  meridian-postgres = mkMeridianPackage {
    pname          = "meridian-postgres";
    features       = "storage-postgres";
    cargoArtifacts = cargoArtifactsPostgres;
  };

  meridian-cluster = mkMeridianPackage {
    pname          = "meridian-cluster";
    features       = "storage-sled,cluster";
    cargoArtifacts = cargoArtifactsCluster;
  };

  meridian-pg-sync = mkMeridianPackage {
    pname          = "meridian-pg-sync";
    features       = "pg-sync";
    cargoArtifacts = cargoArtifactsPgSync;
  };

  # Exposed so devshell.nix and checks.nix can reuse them without re-deriving.
  inherit craneLib rustToolchain;
  inherit cargoArtifactsSled;
  inherit commonArgs commonNativeBuildInputs commonBuildInputs workspacePackages;
}
