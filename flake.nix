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
      #
      # Strategy: cross-compile the Go binaries on the build host (darwin
      # or linux) — the local nix linux-builder VM is fixed at 3GB by
      # Determinate (customizing breaks the cache) which is too small
      # for the akeyless-go SDK's 600+ endpoint compile. Cross-compile
      # from darwin (M-series, 32-48 GB RAM) using GOOS=linux + the
      # right GOARCH. Pure-Go build (CGO_ENABLED=0) means no toolchain
      # cross-deps. dockerTools.buildLayeredImage still runs Linux-side
      # via the VM but only does layering + tarballing — no compilation.
      mkImage = system: let
        # Cross-compile from the host (darwin: 32-48GB) instead of in
        # the 3GB Determinate linux-builder VM — the akeyless-go SDK's
        # 600+ endpoint compile OOMs there. pkgsCross is nixpkgs's
        # canonical cross-compile entrypoint; buildGoModule honors the
        # cross stdenv automatically (CGO_ENABLED=0 keeps it pure-Go,
        # so no toolchain tail). dockerTools.buildLayeredImage still
        # runs Linux-side via the VM but only does layer-tarballing.
        hostSystem = builtins.currentSystem or "aarch64-darwin";
        hostPkgs = import nixpkgs { system = hostSystem; };
        targetPkgs = import nixpkgs { inherit system; };
        lib = hostPkgs.lib;

        # pkgsCross attribute name keyed on target system.
        crossPkgs = if system == "aarch64-linux"
          then hostPkgs.pkgsCross.aarch64-multiplatform
          else if system == "x86_64-linux"
          then hostPkgs.pkgsCross.gnu64
          else throw "pitr-tools: unsupported target system ${system}";

        binaries = crossPkgs.buildGoModule {
          pname = "pitr-tools";
          inherit version;
          src = self;
          vendorHash = "sha256-vM23v0t/uJNgBoW+uN6k+CW64ePyXmEVoLIm6Ex/ZXk=";
          subPackages = map (n: "cmd/${n}") binNames;
          env.CGO_ENABLED = "0";
          ldflags = [ "-s" "-w" "-X main.version=${version}" ];
          doCheck = false;
        };
        binariesAtRoot = hostPkgs.runCommand "pitr-tools-binaries" {} ''
          mkdir -p $out
          ${lib.concatMapStringsSep "\n"
            (n: "cp ${binaries}/bin/${n} $out/${n}")
            binNames}
        '';
      in targetPkgs.dockerTools.buildLayeredImage {
        name = "pitr-tools";
        tag = version;
        contents = [ binariesAtRoot targetPkgs.cacert ];
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
          inherit pkgs system;
          # Substrate's libFor expects the *forge package*, not the
          # flake input — its forgeCmd resolution does
          # `"${forge}/bin/forge"` and would otherwise stringify the
          # source-tree input (no bin/forge in there).
          forge = forge.packages.${system}.default;
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

        # Multi-arch publish (amd64 + arm64). rio (Ryzen 9 9955HX,
        # x86_64 NixOS, 16C/32T, 32 GB) is the build host — handles
        # x86_64-linux natively and cross-compiles aarch64-linux via
        # pkgsCross. The dead quero-x86-builder-ssm ASG path is no
        # longer the dependency.
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
