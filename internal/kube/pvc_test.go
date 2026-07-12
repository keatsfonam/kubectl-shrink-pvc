package kube

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
)

func TestDeletePVCUsesUIDPrecondition(t *testing.T) {
	client := fake.NewSimpleClientset(&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "data", Namespace: "ns", UID: "uid-1"}})
	var gotUID *types.UID
	client.PrependReactor("delete", "persistentvolumeclaims", func(action clienttesting.Action) (bool, runtime.Object, error) {
		gotUID = action.(clienttesting.DeleteAction).GetDeleteOptions().Preconditions.UID
		return false, nil, nil
	})

	if err := DeletePVC(context.Background(), client, "ns", "data", "uid-1"); err != nil {
		t.Fatalf("DeletePVC returned error: %v", err)
	}
	if gotUID == nil || *gotUID != "uid-1" {
		t.Fatalf("unexpected UID precondition: %v", gotUID)
	}
}

func TestDeletePVCRejectsMissingUID(t *testing.T) {
	client := fake.NewSimpleClientset()
	if err := DeletePVC(context.Background(), client, "ns", "data", ""); err == nil {
		t.Fatal("expected missing UID error")
	}
}
