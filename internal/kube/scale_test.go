package kube

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	autoscalingv1 "k8s.io/api/autoscaling/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
)

func TestWaitForPVCDeletedPropagatesAPIErrors(t *testing.T) {
	client := fake.NewSimpleClientset()
	client.PrependReactor("get", "persistentvolumeclaims", func(clienttesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("API unavailable")
	})

	err := WaitForPVCDeleted(context.Background(), client, "ns", "data", "uid", time.Second, time.Millisecond)
	if err == nil || !strings.Contains(err.Error(), "API unavailable") {
		t.Fatalf("expected API error, got %v", err)
	}
}

func TestWaitForPVCDeletedAcceptsNotFound(t *testing.T) {
	client := fake.NewSimpleClientset()

	if err := WaitForPVCDeleted(context.Background(), client, "ns", "missing", "uid", time.Second, time.Millisecond); err != nil {
		t.Fatalf("expected missing PVC to count as deleted: %v", err)
	}
}

func TestRestoreDeploymentsAttemptsEveryDeployment(t *testing.T) {
	client := fake.NewSimpleClientset()
	var updates []string
	client.PrependReactor("get", "deployments", func(action clienttesting.Action) (bool, runtime.Object, error) {
		get := action.(clienttesting.GetAction)
		return true, &autoscalingv1.Scale{ObjectMeta: metav1.ObjectMeta{Name: get.GetName(), Namespace: get.GetNamespace()}}, nil
	})
	client.PrependReactor("update", "deployments", func(action clienttesting.Action) (bool, runtime.Object, error) {
		scale := action.(clienttesting.UpdateAction).GetObject().(*autoscalingv1.Scale)
		updates = append(updates, scale.Name)
		if scale.Name == "first" {
			return true, nil, errors.New("first restore failed")
		}
		return true, scale, nil
	})

	err := RestoreDeployments(context.Background(), client, []DeploymentRef{
		{Namespace: "ns", Name: "first", Replicas: 3},
		{Namespace: "ns", Name: "second", Replicas: 2},
	})
	if err == nil || !strings.Contains(err.Error(), "first restore failed") {
		t.Fatalf("expected restore error, got %v", err)
	}
	if want := []string{"first", "second"}; !reflect.DeepEqual(updates, want) {
		t.Fatalf("unexpected restore attempts: got %v, want %v", updates, want)
	}
}

func TestWaitForPVCDeletedRejectsReplacement(t *testing.T) {
	client := fake.NewSimpleClientset(&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "data", Namespace: "ns", UID: "replacement"}})

	err := WaitForPVCDeleted(context.Background(), client, "ns", "data", "original", time.Second, time.Millisecond)
	if err == nil || !strings.Contains(err.Error(), "was replaced") {
		t.Fatalf("expected replacement error, got %v", err)
	}
}

func TestScaleDeploymentsRollsBackPartialFailure(t *testing.T) {
	client := fake.NewSimpleClientset()
	replicas := map[string]int32{"first": 3, "second": 2}
	var updates []string

	client.PrependReactor("get", "deployments", func(action clienttesting.Action) (bool, runtime.Object, error) {
		get := action.(clienttesting.GetAction)
		return true, &autoscalingv1.Scale{
			ObjectMeta: metav1.ObjectMeta{Name: get.GetName(), Namespace: get.GetNamespace()},
			Spec:       autoscalingv1.ScaleSpec{Replicas: replicas[get.GetName()]},
		}, nil
	})
	client.PrependReactor("update", "deployments", func(action clienttesting.Action) (bool, runtime.Object, error) {
		update := action.(clienttesting.UpdateAction)
		scale := update.GetObject().(*autoscalingv1.Scale)
		updates = append(updates, scale.Name+"="+fmt.Sprint(scale.Spec.Replicas))
		if scale.Name == "second" && scale.Spec.Replicas == 0 {
			// Simulate a server-side write followed by an ambiguous transport error.
			replicas[scale.Name] = scale.Spec.Replicas
			return true, nil, errors.New("scale response lost")
		}
		replicas[scale.Name] = scale.Spec.Replicas
		return true, scale, nil
	})

	err := ScaleDeployments(context.Background(), client, []DeploymentRef{
		{Namespace: "ns", Name: "first", Replicas: 3},
		{Namespace: "ns", Name: "second", Replicas: 2},
	}, 0)
	if err == nil {
		t.Fatal("expected scale failure")
	}
	if replicas["first"] != 3 || replicas["second"] != 2 {
		t.Fatalf("Deployments were not restored: %#v", replicas)
	}
	wantUpdates := []string{"first=0", "second=0", "first=3", "second=2"}
	if !reflect.DeepEqual(updates, wantUpdates) {
		t.Fatalf("unexpected updates: got %v, want %v", updates, wantUpdates)
	}
}
