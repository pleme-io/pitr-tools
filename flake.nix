{
  description = "pitr-tools — five Go binaries for PITR drill Jobs, shipped as a single multi-arch container image";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    substrate = {
      url = "github:pleme-io/substrate";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = { self, nixpkgs, flake-utils, substrate, ... }:
    let
      version = "0.1.0";
      binNames = [
        "notify"
        "canary-create"
        "canary-delete"
        "verify"
        "diagnostic-collect"
      ];
    in
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs { inherit system; };
        lib = pkgs.lib;

        # ── Go monorepo build: one derivation, five binaries ─────────
        # buildGoModule with subPackages produces $out/bin/{notify,
        # canary-create, canary-delete, verify, diagnostic-collect}.
        # CGO_ENABLED=0 keeps the binaries fully static for distroless.
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

        # ── Place binaries at root paths of the image filesystem ─────
        # The published image must expose /notify, /canary-create, etc.
        # so the consumer's K8s Job spec can use command=["/notify"]
        # without an unused intermediate directory. buildGoModule emits
        # under $out/bin/, so flatten with cp into a runtime-only deriv.
        binariesAtRoot = pkgs.runCommand "pitr-tools-binaries" {} ''
          mkdir -p $out
          ${lib.concatMapStringsSep "\n"
            (n: "cp ${binaries}/bin/${n} $out/${n}")
            binNames}
        '';

        # ── Per-arch container image ──────────────────────────────────
        # dockerTools.buildLayeredImage produces a tarball compatible
        # with `docker load` + ghcr push via the multi-arch-image-release
        # action. Single image, no ENTRYPOINT — the consumer's K8s Job
        # spec sets `command: ["/notify"]` (or other binary path) per
        # Job. arch defaults to the build host's arch; CI builds amd64
        # + arm64 separately and combines into a manifest list at push.
        mkImage = { arch ? null }: pkgs.dockerTools.buildLayeredImage ({
          name = "pitr-tools";
          tag = version;
          contents = [ binariesAtRoot pkgs.cacert ];
          config = {
            Env = [
              "SSL_CERT_FILE=/etc/ssl/certs/ca-bundle.crt"
            ];
            # No Entrypoint — Job spec sets command=["/<binary>"] per Job.
            # No User — distroless-style nonroot is enforced by the K8s
            # SecurityContext on the consumer side.
            Labels = {
              "org.opencontainers.image.source" =
                "https://github.com/pleme-io/pitr-tools";
              "org.opencontainers.image.description" =
                "Crossplane Composition Job binaries for PITR drill harness";
              "org.opencontainers.image.licenses" = "MIT";
              "org.opencontainers.image.version" = version;
            };
          };
        } // lib.optionalAttrs (arch != null) { architecture = arch; });

        dockerImage = mkImage {};
      in {
        packages = {
          default = binaries;
          inherit binaries dockerImage;
        } // lib.listToAttrs (map
          (n: lib.nameValuePair n (pkgs.writeShellScriptBin n ''
            exec ${binaries}/bin/${n} "$@"
          ''))
          binNames);

        apps = lib.listToAttrs (map
          (n: lib.nameValuePair n {
            type = "app";
            program = "${binaries}/bin/${n}";
          })
          binNames);

        devShells.default = pkgs.mkShellNoCC {
          packages = with pkgs; [ go gopls gotools golangci-lint ];
        };

        checks = {
          go-build = binaries;
          docker-image = dockerImage;
        };
      }
    );
}
