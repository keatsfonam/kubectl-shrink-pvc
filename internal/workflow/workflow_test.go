package workflow

import (
	"context"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestRequiredBytesWithMargin(t *testing.T) {
	tests := []struct {
		name   string
		used   int64
		margin int
		want   int64
	}{
		{name: "no margin", used: 1000, margin: 0, want: 1000},
		{name: "ten percent", used: 1000, margin: 10, want: 1100},
		{name: "rounds down like integer percentage arithmetic", used: 1001, margin: 10, want: 1101},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := requiredBytesWithMargin(tt.used, tt.margin)
			if err != nil {
				t.Fatalf("requiredBytesWithMargin returned error: %v", err)
			}
			if got != tt.want {
				t.Fatalf("got %d, want %d", got, tt.want)
			}
		})
	}
}

func TestEnsureTemporaryPVCRejectsExistingWrongSize(t *testing.T) {
	client := fake.NewSimpleClientset(pvc("data-shrink", "ns", "1Gi"))
	target := resource.MustParse("2Gi")
	tempPVC := pvc("data-shrink", "ns", "2Gi")

	_, err := ensureTemporaryPVC(context.Background(), client, "ns", tempPVC, target)
	if err == nil {
		t.Fatal("expected size mismatch error")
	}
	if !strings.Contains(err.Error(), "delete it or pass a different --temp-name") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestEnsureTemporaryPVCReusesExistingMatchingSize(t *testing.T) {
	client := fake.NewSimpleClientset(pvc("data-shrink", "ns", "2Gi"))
	target := resource.MustParse("2Gi")
	tempPVC := pvc("data-shrink", "ns", "2Gi")

	reused, err := ensureTemporaryPVC(context.Background(), client, "ns", tempPVC, target)
	if err != nil {
		t.Fatalf("ensureTemporaryPVC returned error: %v", err)
	}
	if !reused {
		t.Fatal("expected existing matching temp PVC to be reused")
	}
}

func pvc(name, namespace, size string) *corev1.PersistentVolumeClaim {
	storage := resource.MustParse(size)
	return &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec: corev1.PersistentVolumeClaimSpec{
			Resources: corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: storage}},
		},
	}
}
