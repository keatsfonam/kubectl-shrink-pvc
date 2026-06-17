package pvcmanifest

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestBuildSanitizesPVC(t *testing.T) {
	storage := resource.MustParse("10Gi")
	target := resource.MustParse("5Gi")
	source := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "data",
			Namespace:       "app",
			UID:             "uid",
			ResourceVersion: "123",
			Finalizers:      []string{"kubernetes.io/pvc-protection"},
			Labels:          map[string]string{"app": "demo"},
			Annotations: map[string]string{
				"keep":                            "true",
				"pv.kubernetes.io/bind-completed": "yes",
				"volume.kubernetes.io/storage-provisioner": "driver.example",
			},
			OwnerReferences: []metav1.OwnerReference{{Name: "owner"}},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			VolumeName: "pvc-old",
			Resources:  corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: storage}},
		},
		Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
	}

	got, err := Build(source, "data-shrink", target)
	if err != nil {
		t.Fatalf("Build returned error: %v", err)
	}
	if got.Name != "data-shrink" || got.Namespace != "app" {
		t.Fatalf("unexpected identity: %s/%s", got.Namespace, got.Name)
	}
	if got.UID != "" || got.ResourceVersion != "" || len(got.Finalizers) != 0 || len(got.OwnerReferences) != 0 {
		t.Fatalf("server-managed metadata was not sanitized: %#v", got.ObjectMeta)
	}
	if got.Spec.VolumeName != "" {
		t.Fatalf("volumeName should be cleared, got %q", got.Spec.VolumeName)
	}
	if got.Status.Phase != "" {
		t.Fatalf("status should be cleared, got %#v", got.Status)
	}
	if got.Annotations["keep"] != "true" {
		t.Fatalf("expected custom annotation to be preserved")
	}
	if _, ok := got.Annotations["pv.kubernetes.io/bind-completed"]; ok {
		t.Fatalf("binding annotation was not removed")
	}
	gotStorage := got.Spec.Resources.Requests[corev1.ResourceStorage]
	if gotStorage.Cmp(target) != 0 {
		t.Fatalf("target storage not set: %s", gotStorage.String())
	}
}

func TestTempNameTruncates(t *testing.T) {
	got := TempName("abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz", "shrink-tmp")
	if len(got) > 63 {
		t.Fatalf("name too long: %d", len(got))
	}
	if got == "" {
		t.Fatal("expected non-empty name")
	}
}
