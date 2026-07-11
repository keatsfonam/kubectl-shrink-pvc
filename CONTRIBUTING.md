# Contributing

Bug reports and pull requests are welcome. For anything beyond a small fix, open an issue first so we can talk it through.

Development uses plain Go tooling:

```sh
make build   # build the plugin binary
make test    # go test ./...
make lint    # golangci-lint run
```

CI runs vet, tests, lint, and a goreleaser snapshot build on every pull request. Please make sure `make test` and `make lint` pass before sending a PR.

Releases are cut by pushing a `v*` tag. The release workflow builds archives for linux, macOS, and Windows and publishes them on GitHub Releases. `.krew.yaml` is the manifest template used for the Krew index.
