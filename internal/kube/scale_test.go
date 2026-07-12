package kube

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	autoscalingv1 "k8s.io/api/autoscaling/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
)

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
			return true, nil, errors.New("scale API failed")
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
	if replicas["first"] != 3 {
		t.Fatalf("first Deployment was not restored: replicas=%d", replicas["first"])
	}
	wantUpdates := []string{"first=0", "second=0", "first=3"}
	if !reflect.DeepEqual(updates, wantUpdates) {
		t.Fatalf("unexpected updates: got %v, want %v", updates, wantUpdates)
	}
}
