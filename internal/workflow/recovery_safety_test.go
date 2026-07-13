package workflow

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv1 "k8s.io/api/autoscaling/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/utils/ptr"

	"github.com/keatsfonam/kubectl-shrink-pvc/internal/kube"
	"github.com/keatsfonam/kubectl-shrink-pvc/internal/operation"
)

func recoveryState(t *testing.T, phase operation.Phase) *operation.State {
	t.Helper()
	finalPVC := pvc("data", "ns", "1Gi")
	operation.StampRecreatedPVC(finalPVC, "op")
	finalJSON, err := json.Marshal(finalPVC)
	if err != nil {
		t.Fatal(err)
	}
	return &operation.State{
		Version: 1, OperationID: "op", Namespace: "ns", SourceName: "data",
		OriginalSourceUID: "source-uid", TempName: "data-temp", TempUID: "temp-uid",
		TargetSize: "1Gi", Image: "image", RunAsUser: -1, FSGroup: -1,
		Deployments:  []kube.DeploymentRef{{Namespace: "ns", Name: "consumer", UID: "deployment-uid", Replicas: 3}},
		FinalPVCJSON: finalJSON, Phase: phase,
	}
}

func TestResumeLoadsLegacyStateName(t *testing.T) {
	pvcName := strings.Repeat("a", 60)
	finalPVC := pvc(pvcName, "ns", "1Gi")
	operation.StampRecreatedPVC(finalPVC, "op")
	finalPVC.UID = "recreated-uid"
	finalJSON, err := json.Marshal(finalPVC)
	if err != nil {
		t.Fatal(err)
	}
	client := fake.NewSimpleClientset(finalPVC)
	legacy := operation.Store{Client: client, Namespace: "ns", Name: operation.LegacyNameForPVC(pvcName)}
	state := &operation.State{
		Version: 1, OperationID: "op", Namespace: "ns", SourceName: pvcName,
		OriginalSourceUID: "original", RecreatedSourceUID: "recreated-uid", TempName: "temp",
		TargetSize: "1Gi", Image: "image", RunAsUser: -1, FSGroup: -1, KeepTemp: true,
		FinalPVCJSON: finalJSON, Phase: operation.PhaseCopiedBack,
	}
	cm, err := legacy.Create(context.Background(), state)
	if err != nil {
		t.Fatal(err)
	}
	cm.UID = "legacy-state-uid"
	if _, err := client.CoreV1().ConfigMaps("ns").Update(context.Background(), cm, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	cfg := Config{PVCName: pvcName, Image: "image", RunAsUser: -1, FSGroup: -1, Timeout: time.Second}
	cfg.IOStreams.Out, cfg.IOStreams.ErrOut = io.Discard, io.Discard
	if err := resume(context.Background(), cfg, client, "ns", resource.MustParse("1Gi")); err != nil {
		t.Fatalf("resume legacy state: %v", err)
	}
}

func TestResumePreparedRestoresOriginalReplicasAndCleansOwnedTemp(t *testing.T) {
	state := recoveryState(t, operation.PhasePrepared)
	source := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "data", Namespace: "ns", UID: "source-uid"}}
	temp := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "data-temp", Namespace: "ns", UID: "temp-uid", Annotations: map[string]string{
		operation.AnnotationOperationID: "op", tempSourceUIDAnnotation: "source-uid", tempSourceNameAnnotation: "data",
	}}}
	deployment := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "consumer", Namespace: "ns", UID: "deployment-uid"}, Spec: appsv1.DeploymentSpec{Replicas: ptr.To[int32](0)}}
	client := fake.NewSimpleClientset(source, temp, deployment)
	restored := int32(-1)
	client.PrependReactor("get", "deployments", func(action k8stesting.Action) (bool, runtime.Object, error) {
		if action.GetSubresource() == "scale" {
			return true, &autoscalingv1.Scale{ObjectMeta: metav1.ObjectMeta{Name: "consumer", Namespace: "ns", UID: "deployment-uid"}, Spec: autoscalingv1.ScaleSpec{Replicas: 0}}, nil
		}
		return false, nil, nil
	})
	client.PrependReactor("update", "deployments", func(action k8stesting.Action) (bool, runtime.Object, error) {
		scale := action.(k8stesting.UpdateAction).GetObject().(*autoscalingv1.Scale)
		restored = scale.Spec.Replicas
		return true, scale, nil
	})
	store := operation.StoreForPVC(client, "ns", "data")
	cm, err := store.Create(context.Background(), state)
	if err != nil {
		t.Fatal(err)
	}
	cm.UID = "state-uid"
	if _, err := client.CoreV1().ConfigMaps("ns").Update(context.Background(), cm, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}

	cfg := Config{PVCName: "data", Image: "image", RunAsUser: -1, FSGroup: -1, Timeout: time.Second}
	cfg.IOStreams.Out, cfg.IOStreams.ErrOut = io.Discard, io.Discard
	if err := resume(context.Background(), cfg, client, "ns", resource.MustParse("1Gi")); err != nil {
		t.Fatalf("resume prepared state: %v", err)
	}
	if restored != 3 {
		t.Fatalf("replicas restored to %d, want 3", restored)
	}
	if _, err := client.CoreV1().PersistentVolumeClaims("ns").Get(context.Background(), "data-temp", metav1.GetOptions{}); err == nil {
		t.Fatal("owned temporary PVC was retained")
	}
}

func TestResumeDeleteRequestTransportErrorNeverRestoresConsumers(t *testing.T) {
	state := recoveryState(t, operation.PhaseSourceDeleteRequested)
	source := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "data", Namespace: "ns", UID: "source-uid"}}
	temp := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "data-temp", Namespace: "ns", UID: "temp-uid", Annotations: map[string]string{
		operation.AnnotationOperationID: "op", tempSourceUIDAnnotation: "source-uid", tempSourceNameAnnotation: "data",
	}}}
	deployment := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "consumer", Namespace: "ns", UID: "deployment-uid"}, Spec: appsv1.DeploymentSpec{Replicas: ptr.To[int32](0)}}
	client := fake.NewSimpleClientset(source, temp, deployment)
	client.PrependReactor("get", "deployments", func(action k8stesting.Action) (bool, runtime.Object, error) {
		if action.GetSubresource() == "scale" {
			return true, &autoscalingv1.Scale{ObjectMeta: metav1.ObjectMeta{Name: "consumer", Namespace: "ns", UID: "deployment-uid"}, Spec: autoscalingv1.ScaleSpec{Replicas: 0}}, nil
		}
		return false, nil, nil
	})
	store := operation.StoreForPVC(client, "ns", "data")
	if _, err := store.Create(context.Background(), state); err != nil {
		t.Fatal(err)
	}
	client.PrependReactor("delete", "persistentvolumeclaims", func(action k8stesting.Action) (handled bool, ret runtime.Object, err error) {
		deleteAction := action.(k8stesting.DeleteAction)
		if err := client.Tracker().Delete(schema.GroupVersionResource{Version: "v1", Resource: "persistentvolumeclaims"}, "ns", deleteAction.GetName()); err != nil {
			return true, nil, err
		}
		return true, nil, fmt.Errorf("transport connection reset after server accepted delete")
	})

	cfg := Config{PVCName: "data", Image: "image", RunAsUser: -1, FSGroup: -1, Timeout: time.Second}
	cfg.IOStreams.Out, cfg.IOStreams.ErrOut = io.Discard, io.Discard
	err := resume(context.Background(), cfg, client, "ns", resource.MustParse("1Gi"))
	if err == nil {
		t.Fatal("expected ambiguous transport error")
	}
	for _, action := range client.Actions() {
		if action.Matches("update", "deployments") && action.GetSubresource() == "scale" {
			t.Fatalf("consumer restoration was attempted after ambiguous delete: %#v", action)
		}
	}
	got, getErr := client.AppsV1().Deployments("ns").Get(context.Background(), "consumer", metav1.GetOptions{})
	if getErr != nil || got.Spec.Replicas == nil || *got.Spec.Replicas != 0 {
		t.Fatalf("consumer was restored: deployment=%#v err=%v", got, getErr)
	}
}
