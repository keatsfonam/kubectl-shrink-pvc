# kubectl-shrink-pvc

[![ci](https://github.com/keatsfonam/kubectl-shrink-pvc/actions/workflows/ci.yml/badge.svg)](https://github.com/keatsfonam/kubectl-shrink-pvc/actions/workflows/ci.yml)

`kubectl-shrink-pvc` is a kubectl plugin that shrinks a filesystem PVC by copying its data through a temporary smaller PVC. Kubernetes cannot shrink an existing bound volume in place, so the replacement phase is intentionally gated behind an explicit flag.

## Status

Initial implementation:

- Go CLI plugin binary: `kubectl-shrink_pvc`
- Invoked as: `kubectl shrink-pvc ...` when the binary is on `PATH`
- Same-namespace PVCs only
- Filesystem PVCs only
- Deployment consumers only for automatic scale-down / restore
- StatefulSet consumers are detected and refused for now
- Source usage inspection is included before copying
- Original PVC deletion/recreation requires `--replace-original`

## Install

Download the archive for your platform from the [releases page](https://github.com/keatsfonam/kubectl-shrink-pvc/releases), unpack it, and put `kubectl-shrink_pvc` on your `PATH`:

```sh
tar -xzf kubectl-shrink-pvc_v0.1.0_darwin_arm64.tar.gz
install -m 0755 kubectl-shrink_pvc /usr/local/bin/kubectl-shrink_pvc
kubectl shrink-pvc --help
```

Or build from source:

```sh
go build -o kubectl-shrink_pvc ./cmd/kubectl-shrink_pvc
install -m 0755 kubectl-shrink_pvc /usr/local/bin/kubectl-shrink_pvc
```

The plugin is not in the Krew index yet. `.krew.yaml` at the repo root is the manifest template for that submission; release archives are built to match it.

## Usage

Plan only:

```sh
kubectl shrink-pvc data --size 20Gi -n app --dry-run
```

Safer first run: scale Deployment(s) down, inspect the source PVC, create a temporary smaller PVC, copy source data into it, then stop. The original PVC is not deleted.

```sh
kubectl shrink-pvc data --size 20Gi -n app --yes
```

Full replacement path: after the temp copy succeeds, delete and recreate the original PVC at the new size, then copy the data back.

```sh
kubectl shrink-pvc data --size 20Gi -n app --yes --replace-original
```

Useful flags:

- `--replace-original`: opt in to deleting/recreating the original PVC.
- `--keep-temp`: keep the temporary PVC after a successful replacement.
- `--manual-scale`: do not scale Deployments; require the PVC to already be unmounted.
- `--temp-name`: choose the temporary PVC name.
- `--inspect-image`: image for the one-shot usage inspection pod. Default: `alpine:3.20`.
- `--copy-image`: image for rsync copy jobs. Default: `instrumentisto/rsync-ssh:alpine3.23-r3@sha256:6cbad37c2fbdca4ac7ad9d1c1bb8990af9efd4dc76321b349935876cbb1e9e4a`.
- `--safety-margin`: require this additional percentage of measured source usage to fit in the target PVC before copying. Default: `10`.
- `--rsync-extra-args`: append custom rsync arguments. The built-in rsync command already includes `--delete` so reused temp PVCs do not retain stale files.
- `--poll-interval`: interval between Kubernetes status checks. Default: `2s`.

## Safety model

The tool runs in phases:

1. Validate the source PVC and target size.
2. Discover pods mounting the PVC.
3. Refuse unsupported consumers such as StatefulSets in v1.
4. Scale Deployment consumers to zero unless `--manual-scale` is set.
5. Wait until the PVC is unmounted.
6. Run an inspection pod that mounts the source PVC read-only and measures usage with `du`.
7. Create a temporary PVC at the target size.
8. Copy source PVC data to the temporary PVC with rsync.
9. Stop unless `--replace-original` is set.
10. If opted in, delete/recreate the original PVC and copy data back.
11. Restore Deployment replica counts.

The inspection step catches obvious "data cannot fit" cases before migration. By default, the measured usage must fit with a 10% safety margin (`--safety-margin`) to leave room for destination filesystem overhead. Rsync and the destination filesystem remain authoritative; sparse files, filesystem overhead, or unusual metadata can still cause the copy to fail. In the default no-`--replace-original` mode, copy failures leave the original PVC untouched.

## Recovery notes

If the command fails before `--replace-original` deletes the original PVC, the original PVC should remain intact.

If it fails after the original PVC is deleted and before completion:

1. Keep the temporary PVC; it contains the copied data if source-to-temp completed.
2. Recreate the original PVC manually or rerun once the issue is resolved.
3. Copy data from the temporary PVC back to the original PVC.
4. Restore Deployment replicas manually if the tool could not restore them.

Use `--keep-temp` for cautious runs until you are comfortable with the workflow.

## Limitations

- No StatefulSet support yet.
- Same namespace only.
- Filesystem PVCs only; raw block PVCs are refused.
- The built-in data mover is a simple same-namespace rsync Job that mounts both PVCs in one pod. This works for the initial Deployment-focused use case but is not as flexible as `pv-migrate` for cross-cluster or complex topology migrations. The copy image must contain `rsync` and `/bin/sh`.
- Static PVs with selectors or specialized binding requirements may need manual handling.
- Inspect and rsync pods set no node affinity; for `ReadWriteOnce` volumes on multi-node clusters, they may hang until `--wait-timeout` if scheduled away from the node where the volume can attach.
- Deployment replica restoration uses the replica count captured during discovery; it can fight an HPA that manages the same Deployment.

## License

MIT, see [LICENSE](LICENSE).
