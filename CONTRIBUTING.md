# Contributing

Bug reports and pull requests are welcome. For anything beyond a small fix, open an issue first so we can talk it through.

Development uses plain Go tooling:

```sh
make build     # build the plugin binary
make test      # go test ./...
make lint      # golangci-lint plus shellcheck
make e2e       # end-to-end suite; needs a kind cluster as the current context
make snapshot  # check GoReleaser config and build without publishing or signing
```

CI runs formatting and module-tidiness checks, vet, race-enabled tests, golangci-lint, shellcheck, govulncheck, a GoReleaser config check and unsigned snapshot, and the isolated kind end-to-end suite. Please run the relevant local targets before sending a PR. `make snapshot` deliberately uses `--skip=sign`; release signing is keyless and only happens in GitHub Actions.

## End-to-end suite

`hack/e2e.sh` refuses non-kind contexts unless `E2E_ALLOW_CONTEXT=1` is explicitly set. CI creates a fresh, single-node kind cluster with the default dynamic StorageClass. The suite assumes Linux volumes support numeric ownership and POSIX modes, the cluster can pull the pinned rsync image and Alpine image, and 512 MiB PVCs can be dynamically provisioned. It covers dry-run safety, pre-destructive copy failure, root metadata preservation, non-root single-user semantics, rsync selection filters, deterministic interruption/resume, and unsupported StatefulSet refusal.

The workload image is pinned as `alpine:<minor>@sha256:<multi-arch-index>` in `hack/e2e.sh`. Renovate's regex manager in `renovate.json5` updates both tag and digest when Renovate is enabled for the repository. To refresh it manually, inspect the index digest (not a platform manifest), update the assignment, and rerun shellcheck and the kind suite:

```sh
docker buildx imagetools inspect alpine:3.20
shellcheck hack/*.sh
make e2e
```

Never bypass the context guard against a shared or production cluster. `E2E_ALLOW_CONTEXT=1` exists only for an intentionally disposable, isolated test cluster.

## Releases

Pushing a `v*` tag starts the release workflow, but publishing is gated on the same full validation workflow used by pull requests, including isolated kind e2e and an unsigned GoReleaser snapshot. Only the publish job receives release, OIDC, and attestation permissions. It builds signed archives, publishes checksums and the Sigstore bundle, and attaches GitHub build-provenance attestations. `.krew.yaml` is the manifest template used for the Krew index.

Before tagging, ensure the intended commit passed CI on `main`, the version follows the existing `vMAJOR.MINOR.PATCH` convention, and the tag points at that exact commit. After publishing, follow the verification commands in the README against a downloaded archive.
