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
version=v0.5.0
tar -xzf "kubectl-shrink-pvc_${version}_darwin_arm64.tar.gz"
install -m 0755 kubectl-shrink_pvc /usr/local/bin/kubectl-shrink_pvc
kubectl shrink-pvc --help
```

Or build from source:

```sh
go build -o kubectl-shrink_pvc ./cmd/kubectl-shrink_pvc
install -m 0755 kubectl-shrink_pvc /usr/local/bin/kubectl-shrink_pvc
```

The plugin is not in the Krew index yet. `.krew.yaml` at the repo root is the manifest template for that submission; release archives are built to match it.

A complete least-privilege namespaced [Role, RoleBinding, and ServiceAccount example](examples/rbac.yaml) covers the PVC, Pod/log, Job, ConfigMap, Deployment/scale, controller-template discovery, HPA, and Lease permissions used by the workflow. Replace its `app` namespace before applying it. Bind the Role to the user or service account that actually runs the local plugin; the included ServiceAccount subject is an example.

`--dry-run` still performs live planning and therefore needs the read-only `get`/`list` permissions in that Role, but it does not need or exercise the mutating verbs. Real runs and `--resume` need the full manifest. The renewable per-PVC Lease uses `coordination.k8s.io/leases`; after a hard crash, takeover requires observing the Lease record remain unchanged for a full 30 seconds and is safe even when client clocks disagree.

## Usage

Plan only:

```sh
kubectl shrink-pvc data --size 20Gi -n app --dry-run
```

Real run: prints the same plan, asks for confirmation, then scales Deployment(s) down, inspects the source PVC, copies the data through a temporary smaller PVC, recreates the original at the new size, copies the data back, and restores the Deployment replicas.

Before starting, suspend anything that can recreate or rescale consumers or rewrite the PVC: HorizontalPodAutoscalers, GitOps reconcilers, operators, scheduled jobs, and backup/restore automation. Record their prior state so you can restore it after the shrink. Same-PVC operations are serialized by a Lease, but do not overlap operations against different PVCs mounted by the same Deployment; consumer scaling is not coordinated across PVCs.

```sh
kubectl shrink-pvc data --size 20Gi -n app
```

Pass `--yes` to skip the confirmation prompt, and `--keep-temp` to keep the intermediate copy around as a fallback.

Useful flags:

- `--keep-temp`: keep the temporary PVC after a successful shrink.
- `--no-scale`: do not scale Deployments; require the PVC to already be unmounted.
- `--resume`: continue a persisted replacement after an interruption; use the same `--size`, image, UID, and rsync settings as the original run. It cannot be combined with `--dry-run` because resume is mutating.
- `--temp-name`: choose the temporary PVC name.
- `--image`: image for the inspection pod and rsync copy jobs; needs `rsync`, `du`, and `/bin/sh`. Default: `instrumentisto/rsync-ssh:alpine3.23-r3@sha256:6cbad37c2fbdca4ac7ad9d1c1bb8990af9efd4dc76321b349935876cbb1e9e4a`.
- `--safety-margin`: require this additional percentage of measured source usage to fit in the target PVC before copying. Default: `10`.
- `--rsync-arg`: append one structured rsync option; repeat it and use `--rsync-arg=--option=value` when an option has a value. Rsync is executed directly without a shell. Selection filters (`--exclude=`, `--include=`, and `--filter=`) are also applied during verification. Options that override ownership, permissions, timestamps, ACLs, xattrs, or hard-link preservation are rejected because they cannot be verified against the selected root/non-root policy. The deprecated `--rsync-extra-args` form supports basic whitespace-separated options only; quoted values containing spaces must migrate to `--rsync-arg`.
- `--run-as-user`: run the inspect and copy pods as this non-root UID so they satisfy the `restricted` PodSecurity profile. Copied files become owned by this UID with umask-derived modes, so it suits volumes owned by a single application user. Without it, pods run as root with a hardened context (seccomp `RuntimeDefault`, no privilege escalation, all capabilities dropped except the handful rsync and du need) and preserve ownership and modes exactly.
- `--fs-group`: `fsGroup` for the inspect and copy pods. Defaults to the `--run-as-user` UID.
- `--timeout`: timeout for pods, jobs, PVC deletion, and workload scaling. Default: `10m`.

## Safety model

The tool runs in phases:

1. Validate the source PVC and target size.
2. Discover live pods and built-in controller templates that reference the PVC.
3. Refuse unsupported consumers such as StatefulSets, DaemonSets, Jobs, CronJobs, and standalone ReplicaSets in v1.
4. Print the plan and wait for confirmation unless `--yes` is set.
5. Acquire a renewable per-PVC Lease, durably checkpoint the approved source/Deployment UIDs and original replica counts, then scale Deployment consumers to zero unless `--no-scale` is set.
6. Wait until the PVC is unmounted.
7. Run an inspection pod that mounts the source PVC read-only and measures usage with `du`.
8. Create a temporary PVC at the target size.
9. Copy source PVC data to the temporary PVC and verify it with a checksum dry-run.
10. Persist recovery state in a ConfigMap before deleting the original PVC.
11. Delete and recreate the original PVC at the new size, copy the data back, and verify it again.
12. Restore Deployment replica counts and remove the recovery state.

The inspection step catches obvious "data cannot fit" cases before migration. By default, the measured usage must fit with a 10% safety margin (`--safety-margin`) to leave room for destination filesystem overhead. Rsync and the destination filesystem remain authoritative; sparse files, filesystem overhead, or unusual metadata can still cause the copy to fail. Each copy is followed by a read-only checksum dry-run, and recovery data is retained if verification fails. Copy or verification failures before the original PVC is deleted leave it untouched.

## Recovery notes

If the command fails before the original PVC is deleted (validation, inspection, or the copy to the temporary PVC), the original PVC remains intact.

Before the first scale write, the plugin persists a recovery ConfigMap whose generated name ends in `-shrink-state` (long names use a stable hash suffix). If the command is interrupted during replacement or restoration, rerun it with the original options plus `--resume`, for example:

```sh
kubectl shrink-pvc data --size 20Gi -n app --resume
```

Resume validates PVC and Deployment UIDs plus operation annotations before adopting, scaling, or deleting anything. An unrelated same-name object is never mutated. Keep the state ConfigMap and temporary PVC until recovery completes; successful completion removes the state automatically. A hard-killed process can leave its Lease until the 30-second timeout expires, so wait and retry if resume reports that the lock is still held. Use `--keep-temp` for cautious runs until you are comfortable with the workflow.

While recovery state exists, leave Deployment consumers suspended and do not manually create a same-name source or temporary PVC. Inspect `<pvc>-shrink-state`, confirm the temporary PVC still exists, then resume with exactly the original size, image, UID, and rsync options. If resume refuses an ownership or UID check, stop and investigate rather than deleting the conflicting object. After success, confirm the Deployment replica counts and data, then re-enable GitOps, autoscaling, operators, jobs, and backup automation in a controlled order. A resumed operation is mutating; `--dry-run` is not a preview mode for a matching `--resume` operation.

## Verify a release

Each release publishes `checksums.txt`, a keyless Sigstore bundle for it, and GitHub build-provenance attestations for the checksum manifest and archives. After downloading an archive and those files from the release:

```sh
sha256sum --check --ignore-missing checksums.txt
cosign verify-blob --bundle checksums.txt.bundle checksums.txt
gh attestation verify kubectl-shrink-pvc_v0.5.0_linux_amd64.tar.gz \
  --repo keatsfonam/kubectl-shrink-pvc
```

Use `shasum -a 256 -c checksums.txt` instead of `sha256sum` on macOS. Verification requires the `cosign` and GitHub CLI (`gh`) tools; inspect the reported GitHub identity and repository before trusting the artifact.

## Limitations

- No StatefulSet support yet.
- Same namespace only.
- Filesystem PVCs only; raw block PVCs are refused.
- The built-in data mover is a simple same-namespace rsync Job that mounts both PVCs in one pod. This works for the initial Deployment-focused use case but is not as flexible as `pv-migrate` for cross-cluster or complex topology migrations. The copy image must contain `rsync` and `/bin/sh`.
- Static PVs with selectors or specialized binding requirements may need manual handling.
- Namespaces that enforce the `restricted` PodSecurity profile reject the default root inspect/copy pods; use `--run-as-user` there and accept that file ownership is not preserved.
- Inspect and rsync pods set no node affinity; for `ReadWriteOnce` volumes on multi-node clusters, they may hang until `--timeout` if scheduled away from the node where the volume can attach.
- HorizontalPodAutoscalers targeting a Deployment consumer must be suspended before the workflow starts; the plugin refuses to continue while one is active. Other reconcilers and operators are not detected and must be suspended manually.
- Same-PVC operations are serialized with a renewable Lease, but operations are not coordinated across PVCs. Never overlap operations that share a Deployment consumer.
- Built-in controller templates, consumer ownership, replica counts, and the source PVC UID are revalidated around destructive and copy-back boundaries. Custom controllers and CRDs cannot be discovered generically and must be suspended manually.

## License

MIT, see [LICENSE](LICENSE).
