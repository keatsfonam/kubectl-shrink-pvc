package operation

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
)

func TestStoreRoundTrip(t *testing.T) {
	client := fake.NewSimpleClientset()
	store := Store{Client: client, Namespace: "ns", Name: NameForPVC("data")}
	state := &State{Version: 1, OperationID: "op", Namespace: "ns", SourceName: "data", OriginalSourceUID: "source", TempName: "tmp", TempUID: "temp", TargetSize: "1Gi", Phase: PhaseCopiedToTemp}
	if _, err := store.Create(context.Background(), state); err != nil {
		t.Fatalf("Create returned error: %v", err)
	}
	got, cm, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	if got.OperationID != state.OperationID || got.Phase != state.Phase {
		t.Fatalf("unexpected state: %#v", got)
	}
	got.Phase = PhaseSourceDeleted
	if _, err := store.Update(context.Background(), got, cm.ResourceVersion); err != nil {
		t.Fatalf("Update returned error: %v", err)
	}
}

func TestStoreRejectsExistingOperation(t *testing.T) {
	client := fake.NewSimpleClientset(&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: NameForPVC("data"), Namespace: "ns"}})
	store := Store{Client: client, Namespace: "ns", Name: NameForPVC("data")}
	state := &State{Version: 1, OperationID: "op", Namespace: "ns", SourceName: "data"}
	if _, err := store.Create(context.Background(), state); err == nil {
		t.Fatal("expected existing operation error")
	}
}

func TestStampAndValidateRecreatedPVC(t *testing.T) {
	pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "data", Namespace: "ns", UID: types.UID("uid")}}
	StampRecreatedPVC(pvc, "op")
	if err := ValidateRecreatedPVC(pvc, "op"); err != nil {
		t.Fatalf("ValidateRecreatedPVC returned error: %v", err)
	}
	if err := ValidateRecreatedPVC(pvc, "other"); err == nil {
		t.Fatal("expected ownership error")
	}
}
