# kubectl-shrink-pvc

[![ci](https://github.com/keatsfonam/kubectl-shrink-pvc/actions/workflows/ci.yml/badge.svg)](https://github.com/keatsfonam/kubectl-shrink-pvc/actions/workflows/ci.yml)

`kubectl-shrink-pvc` is a kubectl plugin that shrinks a filesystem PVC by copying its data through a temporary smaller PVC. Kubernetes cannot shrink an existing bound volume in place, so the plugin recreates the original PVC at the new size — after showing you the plan and asking for confirmation.

## Status

Initial implementation:

- Go CLI plugin binary: `kubectl-shrink_pvc`
- Invoked as: `kubectl shrink-pvc ...` when the binary is on `PATH`
- Same-namespace PVCs only
- Filesystem PVCs only
- Deployment consumers only for automatic scale-down / restore
- StatefulSet consumers are detected and refused for now
- Source usage inspection is included before copying
- Prints the full plan and asks for confirmation before changing anything

## Install

Download the archive for your platform from the [releases page](https://github.com/keatsfonam/kubectl-shrink-pvc/releases), unpack it, and put `kubectl-shrink_pvc` on your `PATH`:

```sh
tar -xzf kubectl-shrink-pvc_v0.3.0_darwin_arm64.tar.gz
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

Real run: prints the same plan, asks for confirmation, then scales Deployment(s) down, inspects the source PVC, copies the data through a temporary smaller PVC, recreates the original at the new size, copies the data back, and restores the Deployment replicas.

```sh
kubectl shrink-pvc data --size 20Gi -n app
```

Pass `--yes` to skip the confirmation prompt, and `--keep-temp` to keep the intermediate copy around as a fallback.

Useful flags:

- `--keep-temp`: keep the temporary PVC after a successful shrink.
- `--no-scale`: do not scale Deployments; require the PVC to already be unmounted.
- `--resume`: continue a persisted replacement after an interruption; use the same `--size`, image, UID, and rsync settings as the original run.
- `--temp-name`: choose the temporary PVC name.
- `--image`: image for the inspection pod and rsync copy jobs; needs `rsync`, `du`, and `/bin/sh`. Default: `instrumentisto/rsync-ssh:alpine3.23-r3@sha256:6cbad37c2fbdca4ac7ad9d1c1bb8990af9efd4dc76321b349935876cbb1e9e4a`.
- `--safety-margin`: require this additional percentage of measured source usage to fit in the target PVC before copying. Default: `10`.
- `--rsync-extra-args`: append custom rsync arguments. The built-in rsync command already includes `--delete` so reused temp PVCs do not retain stale files.
- `--run-as-user`: run the inspect and copy pods as this non-root UID so they satisfy the `restricted` PodSecurity profile. Copied files become owned by this UID with umask-derived modes, so it suits volumes owned by a single application user. Without it, pods run as root with a hardened context (seccomp `RuntimeDefault`, no privilege escalation, all capabilities dropped except the handful rsync and du need) and preserve ownership and modes exactly.
- `--fs-group`: `fsGroup` for the inspect and copy pods. Defaults to the `--run-as-user` UID.
- `--timeout`: timeout for pods, jobs, PVC deletion, and workload scaling. Default: `10m`.

## Safety model

The tool runs in phases:

1. Validate the source PVC and target size.
2. Discover pods mounting the PVC.
3. Refuse unsupported consumers such as StatefulSets in v1.
4. Print the plan and wait for confirmation unless `--yes` is set.
5. Scale Deployment consumers to zero unless `--no-scale` is set.
6. Wait until the PVC is unmounted.
7. Run an inspection pod that mounts the source PVC read-only and measures usage with `du`.
8. Create a temporary PVC at the target size.
9. Copy source PVC data to the temporary PVC with rsync.
10. Persist recovery state in a ConfigMap before deleting the original PVC.
11. Delete and recreate the original PVC at the new size, then copy the data back.
12. Restore Deployment replica counts and remove the recovery state.

The inspection step catches obvious "data cannot fit" cases before migration. By default, the measured usage must fit with a 10% safety margin (`--safety-margin`) to leave room for destination filesystem overhead. Rsync and the destination filesystem remain authoritative; sparse files, filesystem overhead, or unusual metadata can still cause the copy to fail. Copy failures before the original PVC is deleted leave it untouched.

## Recovery notes

If the command fails before the original PVC is deleted (validation, inspection, or the copy to the temporary PVC), the original PVC remains intact.

After the source-to-temporary copy succeeds, the plugin persists a ConfigMap named `<pvc>-shrink-state`. If the command is interrupted during replacement or restoration, rerun it with the original options plus `--resume`, for example:

```sh
kubectl shrink-pvc data --size 20Gi -n app --resume
```

Resume validates PVC UIDs and operation annotations before adopting or deleting anything. An unrelated same-name PVC is never deleted. Keep the state ConfigMap and temporary PVC until recovery completes; successful completion removes the state automatically. Use `--keep-temp` for cautious runs until you are comfortable with the workflow.

## Limitations

- No StatefulSet support yet.
- Same namespace only.
- Filesystem PVCs only; raw block PVCs are refused.
- The built-in data mover is a simple same-namespace rsync Job that mounts both PVCs in one pod. This works for the initial Deployment-focused use case but is not as flexible as `pv-migrate` for cross-cluster or complex topology migrations. The copy image must contain `rsync` and `/bin/sh`.
- Static PVs with selectors or specialized binding requirements may need manual handling.
- Namespaces that enforce the `restricted` PodSecurity profile reject the default root inspect/copy pods; use `--run-as-user` there and accept that file ownership is not preserved.
- Inspect and rsync pods set no node affinity; for `ReadWriteOnce` volumes on multi-node clusters, they may hang until `--timeout` if scheduled away from the node where the volume can attach.
- HorizontalPodAutoscalers targeting a Deployment consumer must be suspended before the workflow starts; the plugin refuses to continue while one is active.
- Consumer ownership, replica counts, and the source PVC UID are revalidated after confirmation and immediately before deletion.

## License

MIT, see [LICENSE](LICENSE).
