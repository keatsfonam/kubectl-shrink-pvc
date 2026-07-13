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

func TestStoreUpdatePreservesConfigMapMetadata(t *testing.T) {
	client := fake.NewSimpleClientset()
	store := Store{Client: client, Namespace: "ns", Name: NameForPVC("data")}
	state := &State{Version: 1, OperationID: "op", Namespace: "ns", SourceName: "data", Phase: PhaseCopiedToTemp}
	cm, err := store.Create(context.Background(), state)
	if err != nil {
		t.Fatal(err)
	}
	cm.Labels["custom"] = "keep"
	cm.Annotations = map[string]string{"custom": "keep"}
	cm.Data["custom"] = "keep"
	cm.Finalizers = []string{"example.test/finalizer"}
	cm, err = client.CoreV1().ConfigMaps("ns").Update(context.Background(), cm, metav1.UpdateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	state.Phase = PhaseSourceDeleted
	if _, err := store.Update(context.Background(), state, cm.ResourceVersion); err != nil {
		t.Fatal(err)
	}
	got, err := client.CoreV1().ConfigMaps("ns").Get(context.Background(), store.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if got.Labels["custom"] != "keep" || got.Annotations["custom"] != "keep" || len(got.Finalizers) != 1 {
		t.Fatalf("metadata was not preserved: %#v", got.ObjectMeta)
	}
	if got.Data["custom"] != "keep" {
		t.Fatalf("unrelated ConfigMap data was not preserved: %#v", got.Data)
	}
}

func TestStoreEnsureAbsentRejectsExistingOperation(t *testing.T) {
	client := fake.NewSimpleClientset(&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: NameForPVC("data"), Namespace: "ns"}})
	store := Store{Client: client, Namespace: "ns", Name: NameForPVC("data")}
	if err := store.EnsureAbsent(context.Background()); err == nil {
		t.Fatal("expected existing operation error")
	}
	missing := Store{Client: client, Namespace: "ns", Name: NameForPVC("other")}
	if err := missing.EnsureAbsent(context.Background()); err != nil {
		t.Fatalf("unexpected missing-state error: %v", err)
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
