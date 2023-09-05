{
  description = "rebacs";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs = {
        nixpkgs.follows = "nixpkgs";
        flake-utils.follows = "flake-utils";
      };
    };
    crane = {
      url = "github:ipetkov/crane";
      inputs = {
        nixpkgs.follows = "nixpkgs";
        flake-utils.follows = "flake-utils";
        rust-overlay.follows = "rust-overlay";
      };
    };
  };

  outputs = { self, nixpkgs, flake-utils, rust-overlay, crane}:
    flake-utils.lib.eachDefaultSystem
      (system:
        let
          overlays = [ (import rust-overlay) ];
          pkgs = import nixpkgs {
            inherit system overlays;
          };

          rustToolchain = pkgs.rust-bin.nightly.latest.default;

          protoFilter = path: _type: builtins.match ".*proto$" path != null;
          protoOrCargo = path: type: (protoFilter path type) || (craneLib.filterCargoSources path type);

          craneLib = (crane.mkLib pkgs).overrideToolchain rustToolchain;
          src = pkgs.lib.cleanSourceWith {
            src = craneLib.path ./.;
            filter = protoOrCargo;
          };

          nativeBuildInputs = with pkgs; [ rustToolchain pkg-config ];
          buildInputs = with pkgs; [ protobuf ];

          commonArgs = {
            inherit src buildInputs nativeBuildInputs;
          };
          cargoArtifacts = craneLib.buildDepsOnly commonArgs;

          bin = craneLib.buildPackage (commonArgs // {
            inherit cargoArtifacts;
          });

          dockerImage = pkgs.dockerTools.buildImage {
            name = "rebacs";
            tag = "latest";
            config = {
              Cmd = [ "${bin}/bin/rebacs_server" ];
            };
          };

        in
        with pkgs;
        {
          packages = {
            inherit bin dockerImage;
            default = bin;
          };
          devShells.default = mkShell {
            inputsFrom = [ bin ];
          };
        }
      );
}
