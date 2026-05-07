# pitr-tools

A small set of Go binaries that run as Kubernetes Jobs inside a Crossplane
Composition driving point-in-time-recovery (PITR) drills. Distributed as a
single multi-arch container image — one image, five binaries.

## Binaries

| Binary | Role |
|---|---|
| `notify` | Posts drill phase notifications (Started / Succeeded / Failed / Canceled) to a Slack incoming webhook. |
| `canary-create` | Creates a deterministic canary item in the source secret store before the PITR snapshot. The canary value derives from the drill's correlation_id so the verifier can re-derive what to look for. |
| `canary-delete` | Cleans up the canary after a successful drill. Idempotent on 404. |
| `verify` | Polls the restored secret-store deployment until it accepts auth, then confirms each canary path exists. Exits 0 only when every path is found — proves the restore captured data through the requested `restoreTime`. |
| `diagnostic-collect` | Self-gates on `phase=Failed`. Collects K8s + AWS state into a tarball and uploads to S3 for postmortem. |

## Image

```
ghcr.io/pleme-io/pitr-tools:<version>
```

Multi-arch: `linux/amd64`, `linux/arm64`. Distroless static runtime. Each binary
sits at a fixed path:

```
/notify
/canary-create
/canary-delete
/verify
/diagnostic-collect
```

There is no default `ENTRYPOINT` — the consumer's K8s Job spec sets
`command: ["/notify"]` (or `/canary-create`, etc.) per Job.

## Backend

The first secret-store backend is **akeyless** — `canary-create`,
`canary-delete`, and `verify` use the [`pleme-io/akeyless-go`](https://github.com/pleme-io/akeyless-go)
SDK and authenticate via the akeyless `k8s` auth method. Adding additional
backends is a matter of implementing the `internal/secrets` interface and
selecting at runtime via flag.

## Build (Nix)

```sh
nix build .#dockerImage              # local OCI tarball, current arch
nix run  .#                          # not provided — there is no default app
nix run  .#notify -- --help          # invoke a binary directly
```

The flake is [substrate](https://github.com/pleme-io/substrate)-based — every
Go primitive comes from `substrate/lib/build/go/`.

## Build (without Nix)

```sh
go build ./cmd/notify
go build ./cmd/canary-create
go build ./cmd/canary-delete
go build ./cmd/verify
go build ./cmd/diagnostic-collect
```

## Why one image, five binaries

The five Jobs in a single drill share fate: they're released together,
versioned together, and reasoned about together. Lockstep versioning is
load-bearing — a `notify`-with-old-flags talking to a `verify`-with-new-flags
is a debugging hazard. Single image, single tag.

Disk cost is minimal — five distroless-static Go binaries fit under ~50 MB.
A K8s node pulls the image once and reuses the cache for every Job.

## License

MIT — see [`LICENSE`](./LICENSE).
