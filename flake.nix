{
  description = "pitr-tools — Crossplane Composition Job binaries for PITR drill harness (5 binaries, one multi-arch image)";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    substrate = {
      url = "github:pleme-io/substrate";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = { self, nixpkgs, flake-utils, substrate, ... }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs { inherit system; };
        # P3 fills this in: substrate-driven multi-binary single-image build.
        # Placeholder so the flake is parseable end-to-end at P1.
        unimplemented = pkgs.runCommand "pitr-tools-p1-placeholder" {} ''
          echo "P1 scaffold — image build lands at P3" > $out
        '';
      in {
        packages = {
          default = unimplemented;
          dockerImage = unimplemented;
        };
        devShells.default = pkgs.mkShellNoCC {
          packages = with pkgs; [ go gopls gotools golangci-lint ];
        };
      }
    );
}
