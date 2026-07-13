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

func TestLeaseLockSerializesAndReleases(t *testing.T) {
	client := fake.NewSimpleClientset()
	first, err := AcquireLease(context.Background(), client, "ns", "data", "first")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := AcquireLease(context.Background(), client, "ns", "data", "second"); err == nil || !strings.Contains(err.Error(), "is held") {
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

func TestLeaseLockTakesOverExpiredLease(t *testing.T) {
	client := fake.NewSimpleClientset()
	holder := "dead-process"
	duration := int32(1)
	old := metav1.NewMicroTime(time.Now().Add(-time.Minute))
	_, err := client.CoordinationV1().Leases("ns").Create(context.Background(), &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{Name: LockNameForPVC("data"), Namespace: "ns"},
		Spec:       coordinationv1.LeaseSpec{HolderIdentity: &holder, LeaseDurationSeconds: &duration, RenewTime: &old},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	lock, err := AcquireLease(context.Background(), client, "ns", "data", "replacement")
	if err != nil {
		t.Fatalf("take over expired Lease: %v", err)
	}
	if err := lock.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
}
