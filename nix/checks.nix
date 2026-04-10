{ pkgs, craneBuilds }:
let
  craneLib = craneBuilds.craneLib;

  # Reuse sled dep artifacts — fastest variant, no external services needed.
  cargoArtifacts = craneBuilds.cargoArtifactsSled;

  commonArgs = craneBuilds.commonArgs // {
    inherit cargoArtifacts;
    cargoExtraArgs = craneBuilds.workspacePackages;
  };
in
{
  # cargo test — unit tests and bin tests only.
  # WAL/pg-sync integration tests require a live PostgreSQL with wal_level=logical
  # and run exclusively in the GitHub Actions `wal-integration` job.
  meridian-cargo-test = craneLib.cargoTest (commonArgs // {
    cargoTestExtraArgs = "--lib --bins";
  });

  # cargo clippy — matches the CI gate (`-D warnings`)
  meridian-clippy = craneLib.cargoClippy (commonArgs // {
    cargoClippyExtraArgs = "-- -D warnings";
  });

  # cargo clippy with cluster feature — second CI gate
  meridian-clippy-cluster = craneLib.cargoClippy (commonArgs // {
    cargoExtraArgs      = "${craneBuilds.workspacePackages} --features cluster";
    cargoClippyExtraArgs = "-- -D warnings";
  });

  # cargo fmt check — no reformatting, just verification
  meridian-fmt = craneLib.cargoFmt {
    inherit (craneBuilds.commonArgs) src;
  };
}
