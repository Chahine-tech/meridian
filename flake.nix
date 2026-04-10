{
  description = "Meridian — self-hosted real-time CRDT sync engine";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";

    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };

    # crane >= 0.19 required: Cargo.lock v4 support (Rust 1.78+ / edition 2024)
    crane.url = "github:ipetkov/crane";

    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, rust-overlay, crane, flake-utils, ... }:
    let
      supportedSystems = [
        "x86_64-linux"
        "aarch64-linux"
        "x86_64-darwin"
        "aarch64-darwin"
      ];
    in
    flake-utils.lib.eachSystem supportedSystems (system:
      let
        pkgs = import nixpkgs {
          inherit system;
          overlays = [ rust-overlay.overlays.default ];
        };

        craneBuilds = import ./nix/crane.nix { inherit pkgs crane; };
        checks      = import ./nix/checks.nix { inherit pkgs craneBuilds; };
      in
      {
        packages = {
          default           = craneBuilds.meridian;
          meridian          = craneBuilds.meridian;
          meridian-postgres = craneBuilds.meridian-postgres;
          meridian-cluster  = craneBuilds.meridian-cluster;
          meridian-pg-sync  = craneBuilds.meridian-pg-sync;
        };

        devShells.default = import ./nix/devshell.nix { inherit pkgs craneBuilds; };

        checks = checks;

        formatter = pkgs.alejandra;
      }
    )
    // {
      nixosModules.meridian = import ./nix/module.nix;
      nixosModules.default  = import ./nix/module.nix;
    };
}
