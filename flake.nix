{
  description = "pitr-tools — six Go binaries for PITR drill Jobs, shipped as a single multi-arch container image";

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
      version = "0.3.4";
      registry = "ghcr.io/pleme-io/pitr-tools";
      binNames = [
        "notify"
        "canary-create"
        "canary-delete"
        "verify"
        "diagnostic-collect"
        "wait-for-deps"
      ];

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

        # ── Per-target-arch image builder ────────────────────────────
        # substrate's mkImageReleaseApp invokes mkImage once per linux
        # target system (x86_64-linux, aarch64-linux). The host doing
        # the compile is the eachDefaultSystem-iterating `pkgs` — i.e.
        # whatever machine ran `nix run .#release` (rio = x86_64-linux,
        # cid = aarch64-darwin, ryn = aarch64-darwin).
        #
        # When host == target we use buildGoModule directly (native).
        # When host != target we use pkgsCross (pure-Go via CGO_ENABLED=0
        # → no toolchain tail beyond the cross-Go binary itself).
        #
        # dockerTools.buildLayeredImage still needs to evaluate the
        # target-system pkgs for the layer-tarballing step.
        mkImage = targetSystem: let
          targetPkgs = import nixpkgs { system = targetSystem; };

          # Cross-stdenv selection — only used when host != target.
          crossPkgs = if targetSystem == "aarch64-linux"
            then pkgs.pkgsCross.aarch64-multiplatform
            else if targetSystem == "x86_64-linux"
            then pkgs.pkgsCross.gnu64
            else throw "pitr-tools: unsupported target ${targetSystem}";

          # Native if host arch == target arch (and both are linux),
          # otherwise cross-compile.
          builderPkgs =
            if (system == targetSystem) then pkgs else crossPkgs;

          binaries = builderPkgs.buildGoModule {
            pname = "pitr-tools";
            inherit version;
            src = self;
            vendorHash = "sha256-2hFSorPEbw85E2oaeymVua3bxkPDUjagXaLP8pM1CDA=";
            subPackages = map (n: "cmd/${n}") binNames;
            env.CGO_ENABLED = "0";
            ldflags = [ "-s" "-w" "-X main.version=${version}" ];
            doCheck = false;
          };

          # Flatten $out/bin/X → /X for the image's root paths. Use
          # the host pkgs (cheaper — no cross stdenv).
          binariesAtRoot = pkgs.runCommand "pitr-tools-binaries" {} ''
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

        # Per-binary build for local dev (`nix run .#notify -- --help`).
        binaries = pkgs.buildGoModule {
          pname = "pitr-tools";
          inherit version;
          src = self;
          vendorHash = "sha256-2hFSorPEbw85E2oaeymVua3bxkPDUjagXaLP8pM1CDA=";
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

        # v0.1.0: x86_64-linux only — rio (Ryzen 9, x86_64 NixOS) is
        # the build host. The aarch64-linux variant needs `boot.binfmt.
        # emulatedSystems = [ "aarch64-linux" ]` on rio so dockerTools.
        # buildLayeredImage's per-arch base-json drv can be evaluated
        # there (Go cross-compile already works via pkgsCross). v0.2
        # picks up arm64 once that's enabled.
        releaseApp = substrateLib.mkImageReleaseApp {
          name = "pitr-tools";
          inherit registry mkImage;
          systems = [ "x86_64-linux" ];
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
