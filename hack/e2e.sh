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

root=$(cd "$(dirname "$0")/.." && pwd)
bin=$root/kubectl-shrink_pvc
go build -o "$bin" "$root/cmd/kubectl-shrink_pvc"

fail() {
	echo "FAIL: $*" >&2
	exit 1
}

cleanup() {
	kubectl delete namespace $ns --ignore-not-found --wait=false >/dev/null 2>&1 || true
}
trap cleanup EXIT

# workload <pvc> <uid> creates a PVC and a Deployment mounting it. uid 0
# means the image default (root).
workload() {
	local pvc=$1 uid=$2 sec=""
	if [[ $uid != 0 ]]; then
		sec="securityContext: {runAsUser: $uid, runAsGroup: $uid, fsGroup: $uid}"
	fi
	kubectl apply -n $ns -f - <<EOF
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: $pvc
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
          image: alpine:3.20
          command: ["sleep", "infinity"]
          volumeMounts:
            - name: data
              mountPath: /data
      volumes:
        - name: data
          persistentVolumeClaim:
            claimName: $pvc
EOF
	kubectl -n $ns rollout status deploy/app-"$pvc" --timeout=180s
}

seed() {
	# shellcheck disable=SC2016  # $i expands inside the pod's shell
	kubectl -n $ns exec deploy/app-"$1" -- sh -c '
		mkdir -p /data/files
		i=1
		while [ $i -le 3 ]; do
			dd if=/dev/urandom of=/data/files/blob$i bs=1M count=8 2>/dev/null
			i=$((i + 1))
		done
		cat /data/files/blob* | md5sum | cut -d" " -f1'
}

checksum() {
	kubectl -n $ns exec deploy/app-"$1" -- sh -c 'cat /data/files/blob* | md5sum | cut -d" " -f1'
}

pvc_size() {
	kubectl -n $ns get pvc "$1" -o jsonpath='{.spec.resources.requests.storage}'
}

kubectl delete namespace $ns --ignore-not-found --wait=true
kubectl create namespace $ns

echo "=== dry run makes no changes"
workload data 0
want=$(seed data)
"$bin" data --size 512Mi -n $ns --dry-run
[[ $(pvc_size data) == 1Gi ]] || fail "dry run resized the PVC"
kubectl -n $ns get pvc data-shrink-tmp >/dev/null 2>&1 && fail "dry run created a temp PVC"

echo "=== copy failure preserves source and restores workload"
workload failcopy 0
fail_want=$(seed failcopy)
if out=$("$bin" failcopy --size 512Mi -n $ns --yes --image alpine:3.20 2>&1); then
	fail "expected copy failure, got success"
fi
kubectl -n $ns rollout status deploy/app-failcopy --timeout=180s
[[ $(pvc_size failcopy) == 1Gi ]] || fail "copy failure changed source PVC size"
fail_got=$(checksum failcopy)
[[ $fail_got == "$fail_want" ]] || fail "copy failure changed source data: $fail_got != $fail_want"
kubectl -n $ns get configmap failcopy-shrink-state >/dev/null 2>&1 && fail "copy failure persisted destructive state"

echo "=== full replace as root"
"$bin" data --size 512Mi -n $ns --yes
kubectl -n $ns rollout status deploy/app-data --timeout=180s
[[ $(pvc_size data) == 512Mi ]] || fail "PVC was not resized, got $(pvc_size data)"
got=$(checksum data)
[[ $got == "$want" ]] || fail "checksum mismatch after root shrink: $got != $want"
kubectl -n $ns get pvc data-shrink-tmp >/dev/null 2>&1 && fail "temp PVC was not cleaned up"

echo "=== full replace as non-root"
workload data2 1000
want=$(seed data2)
"$bin" data2 --size 512Mi -n $ns --yes --run-as-user 1000
kubectl -n $ns rollout status deploy/app-data2 --timeout=180s
[[ $(pvc_size data2) == 512Mi ]] || fail "PVC was not resized, got $(pvc_size data2)"
got=$(checksum data2)
[[ $got == "$want" ]] || fail "checksum mismatch after non-root shrink: $got != $want"

echo "=== StatefulSet consumers are refused"
kubectl apply -n $ns -f - <<EOF
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
          image: alpine:3.20
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
kubectl -n $ns rollout status sts/sts --timeout=180s
if out=$("$bin" data-sts-0 --size 512Mi -n $ns --yes 2>&1); then
	fail "expected refusal for StatefulSet consumer, got success"
fi
echo "$out" | grep -q "unsupported" || fail "unexpected refusal message: $out"

echo "PASS"
