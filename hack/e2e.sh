#!/usr/bin/env bash
set -euo pipefail

# End-to-end test of the shrink workflow against a disposable cluster.
# Refuses to run unless the current kubectl context is a kind cluster so it
# can never touch a real one.

ns=shrink-e2e
ctx=$(kubectl config current-context 2>/dev/null || true)
if [[ $ctx != kind-* && ${E2E_ALLOW_CONTEXT:-} != 1 ]]; then
	echo "current context '$ctx' is not a kind cluster; set E2E_ALLOW_CONTEXT=1 to run anyway" >&2
	exit 1
fi

# renovate: datasource=docker depName=alpine versioning=docker
E2E_ALPINE_IMAGE=${E2E_ALPINE_IMAGE:-alpine:3.20@sha256:d9e853e87e55526f6b2917df91a2115c36dd7c696a35be12163d44e6e2a4b6bc}
root=$(cd "$(dirname "$0")/.." && pwd)
bin=$root/kubectl-shrink_pvc
child_pid=
go build -o "$bin" "$root/cmd/kubectl-shrink_pvc"

fail() {
	echo "FAIL: $*" >&2
	exit 1
}

cleanup() {
	if [[ -n $child_pid ]]; then
		kill "$child_pid" >/dev/null 2>&1 || true
	fi
	kubectl delete namespace "$ns" --ignore-not-found --wait=false >/dev/null 2>&1 || true
}
trap cleanup EXIT

# workload <pvc> <uid> creates a labelled/annotated PVC and a Deployment
# mounting it. uid 0 means the image default (root).
workload() {
	local pvc=$1 uid=$2 sec=""
	if [[ $uid != 0 ]]; then
		sec="securityContext: {runAsUser: $uid, runAsGroup: $uid, fsGroup: $uid}"
	fi
	kubectl apply -n "$ns" -f - <<EOF
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: $pvc
  labels:
    shrink-e2e/keep: "true"
  annotations:
    shrink-e2e/keep: original
spec:
  accessModes: [ReadWriteOnce]
  resources:
    requests:
      storage: 1Gi
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: app-$pvc
spec:
  replicas: 1
  selector:
    matchLabels:
      app: app-$pvc
  template:
    metadata:
      labels:
        app: app-$pvc
    spec:
      $sec
      containers:
        - name: app
          image: $E2E_ALPINE_IMAGE
          command: ["sleep", "infinity"]
          volumeMounts:
            - name: data
              mountPath: /data
      volumes:
        - name: data
          persistentVolumeClaim:
            claimName: $pvc
EOF
	kubectl -n "$ns" rollout status deploy/app-"$pvc" --timeout=180s
}

seed() {
	# shellcheck disable=SC2016  # $i expands inside the pod's shell
	kubectl -n "$ns" exec deploy/app-"$1" -- sh -c '
		mkdir -p /data/files
		i=1
		while [ $i -le 3 ]; do
			dd if=/dev/urandom of=/data/files/blob$i bs=1M count=8 2>/dev/null
			i=$((i + 1))
		done
		cat /data/files/blob* | md5sum | cut -d" " -f1'
}

checksum() {
	kubectl -n "$ns" exec deploy/app-"$1" -- sh -c 'cat /data/files/blob* | md5sum | cut -d" " -f1'
}

file_metadata() {
	kubectl -n "$ns" exec deploy/app-"$1" -- stat -c '%u:%g:%a' /data/files/blob1
}

pvc_size() {
	kubectl -n "$ns" get pvc "$1" -o jsonpath='{.spec.resources.requests.storage}'
}

assert_pvc_metadata() {
	local pvc=$1 label annotation
	label=$(kubectl -n "$ns" get pvc "$pvc" -o go-template='{{index .metadata.labels "shrink-e2e/keep"}}')
	annotation=$(kubectl -n "$ns" get pvc "$pvc" -o go-template='{{index .metadata.annotations "shrink-e2e/keep"}}')
	[[ $label == true ]] || fail "PVC $pvc lost its custom label"
	[[ $annotation == original ]] || fail "PVC $pvc lost its custom annotation"
}

kubectl delete namespace "$ns" --ignore-not-found --wait=true
kubectl create namespace "$ns"

echo "=== dry run makes no changes"
workload data 0
want=$(seed data)
"$bin" data --size 512Mi -n "$ns" --dry-run
[[ $(pvc_size data) == 1Gi ]] || fail "dry run resized the PVC"
kubectl -n "$ns" get pvc data-shrink-tmp >/dev/null 2>&1 && fail "dry run created a temp PVC"

echo "=== copy failure preserves source and restores workload"
workload failcopy 0
fail_want=$(seed failcopy)
if out=$("$bin" failcopy --size 512Mi -n "$ns" --yes --image "$E2E_ALPINE_IMAGE" 2>&1); then
	fail "expected copy failure, got success"
fi
kubectl -n "$ns" rollout status deploy/app-failcopy --timeout=180s
[[ $(pvc_size failcopy) == 1Gi ]] || fail "copy failure changed source PVC size"
fail_got=$(checksum failcopy)
[[ $fail_got == "$fail_want" ]] || fail "copy failure changed source data: $fail_got != $fail_want"
kubectl -n "$ns" get configmap failcopy-shrink-state >/dev/null 2>&1 && fail "copy failure persisted destructive state"

echo "=== full replace as root preserves PVC and filesystem metadata"
kubectl -n "$ns" exec deploy/app-data -- chown 123:234 /data/files/blob1
kubectl -n "$ns" exec deploy/app-data -- chmod 640 /data/files/blob1
"$bin" data --size 512Mi -n "$ns" --yes
kubectl -n "$ns" rollout status deploy/app-data --timeout=180s
[[ $(pvc_size data) == 512Mi ]] || fail "PVC was not resized, got $(pvc_size data)"
got=$(checksum data)
[[ $got == "$want" ]] || fail "checksum mismatch after root shrink: $got != $want"
[[ $(file_metadata data) == 123:234:640 ]] || fail "root copy did not preserve uid:gid:mode"
assert_pvc_metadata data
kubectl -n "$ns" get pvc data-shrink-tmp >/dev/null 2>&1 && fail "temp PVC was not cleaned up"

echo "=== full replace as non-root applies single-user ownership semantics"
workload data2 1000
want=$(seed data2)
"$bin" data2 --size 512Mi -n "$ns" --yes --run-as-user 1000
kubectl -n "$ns" rollout status deploy/app-data2 --timeout=180s
[[ $(pvc_size data2) == 512Mi ]] || fail "PVC was not resized, got $(pvc_size data2)"
got=$(checksum data2)
[[ $got == "$want" ]] || fail "checksum mismatch after non-root shrink: $got != $want"
nonroot_metadata=$(file_metadata data2)
[[ $nonroot_metadata == 1000:*:644 ]] || fail "non-root copy did not apply UID and umask-derived mode semantics (got $nonroot_metadata)"
assert_pvc_metadata data2

echo "=== rsync selection filters are honored during copy and verification"
workload filtered 0
filter_want=$(kubectl -n "$ns" exec deploy/app-filtered -- sh -c 'mkdir -p /data/files; echo keep >/data/files/keep.txt; echo skip >/data/files/skip.tmp; md5sum /data/files/keep.txt | cut -d" " -f1')
"$bin" filtered --size 512Mi -n "$ns" --yes '--rsync-arg=--exclude=*.tmp'
kubectl -n "$ns" rollout status deploy/app-filtered --timeout=180s
filter_got=$(kubectl -n "$ns" exec deploy/app-filtered -- sh -c 'md5sum /data/files/keep.txt | cut -d" " -f1')
[[ $filter_got == "$filter_want" ]] || fail "included file changed during filtered copy"
if kubectl -n "$ns" exec deploy/app-filtered -- test -e /data/files/skip.tmp; then
	fail "excluded file was copied"
fi

echo "=== interruption persists recovery state and resume completes"
workload interrupted 0
resume_want=$(seed interrupted)
# Hold deletion at the destructive boundary so the process can be interrupted
# deterministically after it has persisted a verified temporary copy.
kubectl -n "$ns" patch pvc interrupted --type=merge -p '{"metadata":{"finalizers":["shrink-e2e/hold-deletion"]}}'
"$bin" interrupted --size 512Mi -n "$ns" --yes >"$root/e2e-interrupted.log" 2>&1 &
child_pid=$!
for _ in {1..180}; do
	kubectl -n "$ns" get configmap interrupted-shrink-state >/dev/null 2>&1 && break
	sleep 2
done
kubectl -n "$ns" get configmap interrupted-shrink-state >/dev/null 2>&1 || fail "operation did not persist recovery state"
for _ in {1..30}; do
	[[ -n $(kubectl -n "$ns" get pvc interrupted -o jsonpath='{.metadata.deletionTimestamp}' 2>/dev/null || true) ]] && break
	sleep 1
done
[[ -n $(kubectl -n "$ns" get pvc interrupted -o jsonpath='{.metadata.deletionTimestamp}' 2>/dev/null || true) ]] || fail "operation did not reach held deletion"
kill "$child_pid"
wait "$child_pid" 2>/dev/null || true
child_pid=
kubectl -n "$ns" patch pvc interrupted --type=json -p='[{"op":"remove","path":"/metadata/finalizers"}]'
kubectl -n "$ns" wait --for=delete pvc/interrupted --timeout=120s

state_rv=$(kubectl -n "$ns" get configmap interrupted-shrink-state -o jsonpath='{.metadata.resourceVersion}')
temp_uid=$(kubectl -n "$ns" get pvc interrupted-shrink-tmp -o jsonpath='{.metadata.uid}')
if out=$("$bin" interrupted --size 512Mi -n "$ns" --dry-run --resume 2>&1); then
	fail "expected dry-run resume to be rejected"
fi
echo "$out" | grep -q -- '--dry-run cannot be combined with --resume' || fail "unexpected dry-run resume rejection: $out"
[[ $(kubectl -n "$ns" get configmap interrupted-shrink-state -o jsonpath='{.metadata.resourceVersion}') == "$state_rv" ]] || fail "rejected dry-run resume mutated recovery state"
[[ $(kubectl -n "$ns" get pvc interrupted-shrink-tmp -o jsonpath='{.metadata.uid}') == "$temp_uid" ]] || fail "rejected dry-run resume replaced recovery PVC"
kubectl -n "$ns" get pvc interrupted >/dev/null 2>&1 && fail "rejected dry-run resume recreated source PVC"

# A hard-killed process cannot release its Lease. Retry until the 30-second
# Lease expires, then require the first acquired resume to complete fully.
resumed=false
for _ in {1..45}; do
	if out=$("$bin" interrupted --size 512Mi -n "$ns" --resume 2>&1); then
		resumed=true
		break
	fi
	if ! echo "$out" | grep -q 'operation lock .* is held'; then
		fail "resume failed after interruption: $out"
	fi
	sleep 1
done
[[ $resumed == true ]] || fail "resume never acquired the expired operation Lease"
kubectl -n "$ns" rollout status deploy/app-interrupted --timeout=180s
[[ $(pvc_size interrupted) == 512Mi ]] || fail "resumed PVC has wrong size"
resume_got=$(checksum interrupted)
[[ $resume_got == "$resume_want" ]] || fail "checksum mismatch after resume: $resume_got != $resume_want"
kubectl -n "$ns" get configmap interrupted-shrink-state >/dev/null 2>&1 && fail "resume did not clean recovery state"
rm -f "$root/e2e-interrupted.log"

echo "=== StatefulSet consumers are refused"
kubectl apply -n "$ns" -f - <<EOF
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: sts
spec:
  serviceName: sts
  replicas: 1
  selector:
    matchLabels:
      app: sts
  template:
    metadata:
      labels:
        app: sts
    spec:
      containers:
        - name: app
          image: $E2E_ALPINE_IMAGE
          command: ["sleep", "infinity"]
          volumeMounts:
            - name: data
              mountPath: /data
  volumeClaimTemplates:
    - metadata:
        name: data
      spec:
        accessModes: [ReadWriteOnce]
        resources:
          requests:
            storage: 1Gi
EOF
kubectl -n "$ns" rollout status sts/sts --timeout=180s
if out=$("$bin" data-sts-0 --size 512Mi -n "$ns" --yes 2>&1); then
	fail "expected refusal for StatefulSet consumer, got success"
fi
echo "$out" | grep -q "unsupported" || fail "unexpected refusal message: $out"

echo "PASS"
