{
  description = "pitr-tools — five Go binaries for PITR drill Jobs, shipped as a single multi-arch container image";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    substrate = {
      url = "github:pleme-io/substrate";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    forge = {
      url = "github:pleme-io/forge";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = { self, nixpkgs, flake-utils, substrate, forge, ... }:
    let
      version = "0.1.0";
      registry = "ghcr.io/pleme-io/pitr-tools";
      binNames = [
        "notify"
        "canary-create"
        "canary-delete"
        "verify"
        "diagnostic-collect"
      ];

      # ── Per-arch image builder ────────────────────────────────────────
      # `system` is one of "x86_64-linux" / "aarch64-linux"; substrate's
      # mkImageReleaseApp invokes this for each target arch and feeds
      # both into `forge image-release` for SHA-pinned + -latest tagging
      # and a multi-arch manifest combine.
      mkImage = system: let
        pkgs = import nixpkgs { inherit system; };
        lib = pkgs.lib;
        binaries = pkgs.buildGoModule {
          pname = "pitr-tools";
          inherit version;
          src = self;
          vendorHash = "sha256-vM23v0t/uJNgBoW+uN6k+CW64ePyXmEVoLIm6Ex/ZXk=";
          subPackages = map (n: "cmd/${n}") binNames;
          env.CGO_ENABLED = "0";
          ldflags = [ "-s" "-w" "-X main.version=${version}" ];
        };
        binariesAtRoot = pkgs.runCommand "pitr-tools-binaries" {} ''
          mkdir -p $out
          ${lib.concatMapStringsSep "\n"
            (n: "cp ${binaries}/bin/${n} $out/${n}")
            binNames}
        '';
      in pkgs.dockerTools.buildLayeredImage {
        name = "pitr-tools";
        tag = version;
        contents = [ binariesAtRoot pkgs.cacert ];
        config = {
          Env = [ "SSL_CERT_FILE=/etc/ssl/certs/ca-bundle.crt" ];
          # No Entrypoint — Job sets command=["/notify"] etc. per Job.
          Labels = {
            "org.opencontainers.image.source" =
              "https://github.com/pleme-io/pitr-tools";
            "org.opencontainers.image.description" =
              "Crossplane Composition Job binaries for PITR drill harness";
            "org.opencontainers.image.licenses" = "MIT";
            "org.opencontainers.image.version" = version;
          };
        };
      };
    in
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs { inherit system; };
        lib = pkgs.lib;

        # substrate.libFor exposes the SDLC building blocks — we use
        # mkImageReleaseApp to turn `mkImage` into a `nix run .#release`
        # entry that drives `forge image-release` (amd64+arm64 build,
        # GHCR push, multi-arch manifest combine).
        substrateLib = substrate.libFor {
          inherit pkgs system forge;
        };

        # Per-binary build for local dev (`nix run .#notify -- --help`).
        binaries = pkgs.buildGoModule {
          pname = "pitr-tools";
          inherit version;
          src = self;
          vendorHash = "sha256-vM23v0t/uJNgBoW+uN6k+CW64ePyXmEVoLIm6Ex/ZXk=";
          subPackages = map (n: "cmd/${n}") binNames;
          env.CGO_ENABLED = "0";
          ldflags = [ "-s" "-w" "-X main.version=${version}" ];
          meta = with lib; {
            description = "Crossplane Composition Job binaries for PITR drill harness";
            homepage = "https://github.com/pleme-io/pitr-tools";
            license = licenses.mit;
            platforms = platforms.unix;
          };
        };

        releaseApp = substrateLib.mkImageReleaseApp {
          name = "pitr-tools";
          inherit registry mkImage;
        };

        # Local image build for the host system (debugging / `docker load`).
        # Linux-only — dockerTools requires a linux build.
        dockerImageOpt =
          if pkgs.stdenv.hostPlatform.isLinux
          then { dockerImage = mkImage system; }
          else {};
      in {
        packages = {
          default = binaries;
          inherit binaries;
        } // dockerImageOpt;

        apps = {
          release = releaseApp;
        } // lib.listToAttrs (map
          (n: lib.nameValuePair n {
            type = "app";
            program = "${binaries}/bin/${n}";
          })
          binNames);

        devShells.default = pkgs.mkShellNoCC {
          packages = with pkgs; [ go gopls gotools golangci-lint ];
        };

        checks.go-build = binaries;
      }
    );
}
