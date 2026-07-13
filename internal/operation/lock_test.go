package operation

import (
	"context"
	"strings"
	"testing"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestGeneratedLockNamesDoNotCollide(t *testing.T) {
	prefix := strings.Repeat("a", 58)
	if LockNameForPVC(prefix+"one") == LockNameForPVC(prefix+"two") {
		t.Fatal("distinct long PVC names generated the same lock name")
	}
}

func TestLeaseLockSerializesAndReleases(t *testing.T) {
	client := fake.NewSimpleClientset()
	first, err := AcquireLease(context.Background(), client, "ns", "data", "first")
	if err != nil {
		t.Fatal(err)
	}
	contenderCtx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := acquireLease(contenderCtx, client, "ns", "data", "second", 100*time.Millisecond, 20*time.Millisecond); err == nil || !strings.Contains(err.Error(), "is held") {
		t.Fatalf("expected concurrent acquisition refusal, got %v", err)
	}
	if err := first.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
	second, err := AcquireLease(context.Background(), client, "ns", "data", "second")
	if err != nil {
		t.Fatalf("acquire after release: %v", err)
	}
	if err := second.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestLeaseLockReleaseDoesNotDeleteNewOwner(t *testing.T) {
	client := fake.NewSimpleClientset()
	lock, err := AcquireLease(context.Background(), client, "ns", "data", "first")
	if err != nil {
		t.Fatal(err)
	}
	lease, err := client.CoordinationV1().Leases("ns").Get(context.Background(), LockNameForPVC("data"), metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	newHolder := "second"
	lease.Spec.HolderIdentity = &newHolder
	if _, err := client.CoordinationV1().Leases("ns").Update(context.Background(), lease, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := lock.Release(context.Background()); err == nil || !strings.Contains(err.Error(), "ownership changed") {
		t.Fatalf("expected ownership error, got %v", err)
	}
	got, err := client.CoordinationV1().Leases("ns").Get(context.Background(), LockNameForPVC("data"), metav1.GetOptions{})
	if err != nil {
		t.Fatalf("new owner's Lease was deleted: %v", err)
	}
	if got.Spec.HolderIdentity == nil || *got.Spec.HolderIdentity != newHolder {
		t.Fatalf("unexpected Lease holder: %#v", got.Spec.HolderIdentity)
	}
}

func TestLeaseLockTakesOverUnchangedLeaseRegardlessOfClockSkew(t *testing.T) {
	for _, remoteTime := range []time.Time{time.Now().Add(-24 * time.Hour), time.Now().Add(24 * time.Hour)} {
		client := fake.NewSimpleClientset()
		holder := "dead-process"
		duration := int32(30)
		remote := metav1.NewMicroTime(remoteTime)
		_, err := client.CoordinationV1().Leases("ns").Create(context.Background(), &coordinationv1.Lease{
			ObjectMeta: metav1.ObjectMeta{Name: LockNameForPVC("data"), Namespace: "ns"},
			Spec:       coordinationv1.LeaseSpec{HolderIdentity: &holder, LeaseDurationSeconds: &duration, RenewTime: &remote},
		}, metav1.CreateOptions{})
		if err != nil {
			t.Fatal(err)
		}
		started := time.Now()
		lock, err := acquireLease(context.Background(), client, "ns", "data", "replacement", 25*time.Millisecond, 5*time.Millisecond)
		if err != nil {
			t.Fatalf("take over unchanged Lease: %v", err)
		}
		if time.Since(started) < 20*time.Millisecond {
			t.Fatal("Lease was taken over before it was locally observed unchanged")
		}
		if err := lock.Release(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
}

func TestLeaseLockDoesNotTakeOverRenewingHolder(t *testing.T) {
	client := fake.NewSimpleClientset()
	first, err := acquireLease(context.Background(), client, "ns", "data", "first", 40*time.Millisecond, 5*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if _, err := acquireLease(ctx, client, "ns", "data", "second", 40*time.Millisecond, 5*time.Millisecond); err == nil {
		t.Fatal("renewing holder was taken over")
	}
	if err := first.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
}
