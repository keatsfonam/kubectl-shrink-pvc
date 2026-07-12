package kube

import (
	"context"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
)

func TestDiscoverConsumersResolvesDeployment(t *testing.T) {
	controller := true
	replicas := int32(3)
	client := fake.NewSimpleClientset([]runtime.Object{
		&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "web", Namespace: "ns"}, Spec: appsv1.DeploymentSpec{Replicas: &replicas}},
		&appsv1.ReplicaSet{ObjectMeta: metav1.ObjectMeta{Name: "web-abc", Namespace: "ns", OwnerReferences: []metav1.OwnerReference{{Kind: "Deployment", Name: "web", Controller: &controller}}}},
		pod("web-abc-1", "ns", "data", []metav1.OwnerReference{{Kind: "ReplicaSet", Name: "web-abc", Controller: &controller}}),
	}...)

	plan, err := DiscoverConsumers(context.Background(), client, "ns", "data")
	if err != nil {
		t.Fatalf("DiscoverConsumers returned error: %v", err)
	}
	if len(plan.Deployments) != 1 || plan.Deployments[0].Name != "web" || plan.Deployments[0].Replicas != 3 {
		t.Fatalf("unexpected deployments: %#v", plan.Deployments)
	}
	if len(plan.Unsupported) != 0 {
		t.Fatalf("unexpected unsupported consumers: %#v", plan.Unsupported)
	}
}

func TestDiscoverConsumersFlagsStatefulSet(t *testing.T) {
	controller := true
	client := fake.NewSimpleClientset(pod("db-0", "ns", "data", []metav1.OwnerReference{{Kind: "StatefulSet", Name: "db", Controller: &controller}}))

	plan, err := DiscoverConsumers(context.Background(), client, "ns", "data")
	if err != nil {
		t.Fatalf("DiscoverConsumers returned error: %v", err)
	}
	if len(plan.Unsupported) != 1 || plan.Unsupported[0].Kind != "StatefulSet" || plan.Unsupported[0].Name != "db" {
		t.Fatalf("expected StatefulSet unsupported consumer, got %#v", plan.Unsupported)
	}
}

func TestDiscoverDeploymentHPAsFindsTargets(t *testing.T) {
	client := fake.NewSimpleClientset(
		&autoscalingv2.HorizontalPodAutoscaler{
			ObjectMeta: metav1.ObjectMeta{Name: "web-hpa", Namespace: "ns"},
			Spec: autoscalingv2.HorizontalPodAutoscalerSpec{ScaleTargetRef: autoscalingv2.CrossVersionObjectReference{
				APIVersion: "apps/v1", Kind: "Deployment", Name: "web",
			}},
		},
		&autoscalingv2.HorizontalPodAutoscaler{
			ObjectMeta: metav1.ObjectMeta{Name: "other-hpa", Namespace: "ns"},
			Spec: autoscalingv2.HorizontalPodAutoscalerSpec{ScaleTargetRef: autoscalingv2.CrossVersionObjectReference{
				APIVersion: "apps/v1", Kind: "Deployment", Name: "other",
			}},
		},
	)

	refs, err := DiscoverDeploymentHPAs(context.Background(), client, []DeploymentRef{{Namespace: "ns", Name: "web"}})
	if err != nil {
		t.Fatalf("DiscoverDeploymentHPAs returned error: %v", err)
	}
	if len(refs) != 1 || refs[0].Name != "web-hpa" || refs[0].DeploymentName != "web" {
		t.Fatalf("unexpected HPA refs: %#v", refs)
	}
}

func TestActivePodsUsingPVCDoesNotResolveOwners(t *testing.T) {
	controller := true
	client := fake.NewSimpleClientset(pod("web-abc-1", "ns", "data", []metav1.OwnerReference{{Kind: "ReplicaSet", Name: "missing", Controller: &controller}}))

	pods, err := ActivePodsUsingPVC(context.Background(), client, "ns", "data")
	if err != nil {
		t.Fatalf("ActivePodsUsingPVC returned error: %v", err)
	}
	if len(pods) != 1 || pods[0] != "web-abc-1" {
		t.Fatalf("unexpected pods: %#v", pods)
	}
}

func pod(name, namespace, pvc string, owners []metav1.OwnerReference) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace, OwnerReferences: owners},
		Spec:       corev1.PodSpec{Volumes: []corev1.Volume{{Name: "data", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: pvc}}}}},
		Status:     corev1.PodStatus{Phase: corev1.PodRunning},
	}
}
