package workflow

import (
	"bytes"
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

	"github.com/keatsfonam/kubectl-shrink-pvc/internal/datamover"
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

func TestResumePreparedQuietAndDefaultPreserveActionsAndResult(t *testing.T) {
	defaultOut, defaultActions := runPreparedResumeOutput(t, false)
	quietOut, quietActions := runPreparedResumeOutput(t, true)

	result := "Recovered pre-copy operation state and restored Deployment replicas; rerun the shrink command to start a fresh verified copy.\n"
	if !strings.Contains(defaultOut, result) || !strings.Contains(quietOut, result) {
		t.Fatalf("resume result missing:\ndefault=%q\nquiet=%q", defaultOut, quietOut)
	}
	if !strings.Contains(defaultOut, "[progress]") || !strings.Contains(defaultOut, "cleanup/temp-pvc") || !strings.Contains(defaultOut, "total=0s phase-elapsed=0s") {
		t.Fatalf("default resume output lacks fresh invocation progress: %s", defaultOut)
	}
	if strings.Contains(quietOut, "[progress]") || strings.Contains(quietOut, "cleanup/") {
		t.Fatalf("quiet resume output contains progress: %s", quietOut)
	}
	if strings.Join(defaultActions, "\n") != strings.Join(quietActions, "\n") {
		t.Fatalf("quiet changed Kubernetes actions:\ndefault=%v\nquiet=%v", defaultActions, quietActions)
	}
}

func runPreparedResumeOutput(t *testing.T, quiet bool) (string, []string) {
	t.Helper()
	state := recoveryState(t, operation.PhasePrepared)
	state.Deployments = nil
	state.NoScale = true
	source := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "data", Namespace: "ns", UID: "source-uid"}}
	temp := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "data-temp", Namespace: "ns", UID: "temp-uid", Annotations: map[string]string{
		operation.AnnotationOperationID: "op", tempSourceUIDAnnotation: "source-uid", tempSourceNameAnnotation: "data",
	}}}
	client := fake.NewSimpleClientset(source, temp)
	store := operation.StoreForPVC(client, "ns", "data")
	cm, err := store.Create(context.Background(), state)
	if err != nil {
		t.Fatal(err)
	}
	cm.UID = "state-uid"
	if _, err := client.CoreV1().ConfigMaps("ns").Update(context.Background(), cm, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}

	var out bytes.Buffer
	cfg := Config{PVCName: "data", Image: "image", RunAsUser: -1, FSGroup: -1, NoScale: true, Quiet: quiet, Timeout: time.Second}
	cfg.IOStreams.Out, cfg.IOStreams.ErrOut = &out, &out
	if err := resume(context.Background(), cfg, client, "ns", resource.MustParse("1Gi")); err != nil {
		t.Fatalf("resume prepared state quiet=%t: %v", quiet, err)
	}
	actions := make([]string, 0, len(client.Actions()))
	for _, action := range client.Actions() {
		actions = append(actions, fmt.Sprintf("%s %s %s", action.GetVerb(), action.GetResource().Resource, action.GetSubresource()))
	}
	return out.String(), actions
}

func TestEveryDestructiveResumePhaseHasProgressAndQuietActionParity(t *testing.T) {
	tests := []struct {
		phase     operation.Phase
		wantPhase string
		wantMoves int
	}{
		{phase: operation.PhaseCopiedToTemp, wantPhase: "quiesce/waiting-for-unmount", wantMoves: 2},
		{phase: operation.PhaseSourceDeleteRequested, wantPhase: "replace-source/deleting", wantMoves: 2},
		{phase: operation.PhaseSourceDeleteAccepted, wantPhase: "replace-source/deleting", wantMoves: 2},
		{phase: operation.PhaseSourceDeleted, wantPhase: "replace-source/creating", wantMoves: 2},
		{phase: operation.PhaseSourceRecreated, wantPhase: "copy-back/scheduling", wantMoves: 2},
		{phase: operation.PhaseCopiedBack, wantPhase: "cleanup/temp-pvc", wantMoves: 0},
	}
	for _, tt := range tests {
		t.Run(string(tt.phase), func(t *testing.T) {
			defaultOut, defaultActions, defaultMoves := runDestructiveResumeOutput(t, tt.phase, false)
			quietOut, quietActions, quietMoves := runDestructiveResumeOutput(t, tt.phase, true)
			if !strings.Contains(defaultOut, "[progress]") || !strings.Contains(defaultOut, tt.wantPhase) || !strings.Contains(defaultOut, "total=") || !strings.Contains(defaultOut, "phase=") {
				t.Fatalf("default resume output for %s lacks canonical elapsed progress:\n%s", tt.phase, defaultOut)
			}
			if !strings.Contains(defaultOut, "PVC shrink workflow resumed and completed successfully.") || !strings.Contains(quietOut, "PVC shrink workflow resumed and completed successfully.") {
				t.Fatalf("resume result missing for %s:\ndefault=%s\nquiet=%s", tt.phase, defaultOut, quietOut)
			}
			if strings.Contains(quietOut, "[progress]") || strings.Contains(quietOut, "copy-back/") || strings.Contains(quietOut, "cleanup/") {
				t.Fatalf("quiet resume output for %s contains progress: %s", tt.phase, quietOut)
			}
			if strings.Join(defaultActions, "\n") != strings.Join(quietActions, "\n") {
				t.Fatalf("quiet changed Kubernetes actions for %s:\ndefault=%v\nquiet=%v", tt.phase, defaultActions, quietActions)
			}
			if defaultMoves != tt.wantMoves || quietMoves != tt.wantMoves {
				t.Fatalf("mover calls for %s = default %d quiet %d, want %d", tt.phase, defaultMoves, quietMoves, tt.wantMoves)
			}
		})
	}
}

func runDestructiveResumeOutput(t *testing.T, phase operation.Phase, quiet bool) (string, []string, int) {
	t.Helper()
	state := recoveryState(t, phase)
	state.Deployments = nil
	state.NoScale = true
	state.KeepTemp = false
	temp := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: state.TempName, Namespace: state.Namespace, UID: state.TempUID, Annotations: map[string]string{
		operation.AnnotationOperationID: state.OperationID,
		tempSourceUIDAnnotation:         string(state.OriginalSourceUID),
		tempSourceNameAnnotation:        state.SourceName,
	}}}
	objects := []runtime.Object{temp}
	switch phase {
	case operation.PhaseCopiedToTemp, operation.PhaseSourceDeleteRequested, operation.PhaseSourceDeleteAccepted:
		objects = append(objects, &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: state.SourceName, Namespace: state.Namespace, UID: state.OriginalSourceUID}})
	case operation.PhaseSourceRecreated, operation.PhaseCopiedBack:
		var recreated corev1.PersistentVolumeClaim
		if err := json.Unmarshal(state.FinalPVCJSON, &recreated); err != nil {
			t.Fatal(err)
		}
		recreated.UID = "recreated-uid"
		state.RecreatedSourceUID = recreated.UID
		objects = append(objects, &recreated)
	}
	client := fake.NewSimpleClientset(objects...)
	store := operation.StoreForPVC(client, state.Namespace, state.SourceName)
	cm, err := store.Create(context.Background(), state)
	if err != nil {
		t.Fatal(err)
	}
	cm.UID = "state-uid"
	if _, err := client.CoreV1().ConfigMaps(state.Namespace).Update(context.Background(), cm, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}

	mover := &observingFakeMover{}
	var out bytes.Buffer
	cfg := Config{
		PVCName: state.SourceName, Image: state.Image, RunAsUser: state.RunAsUser, FSGroup: state.FSGroup,
		NoScale: true, Quiet: quiet, Timeout: time.Second, mover: mover,
	}
	cfg.IOStreams.Out, cfg.IOStreams.ErrOut = &out, &out
	if err := resume(context.Background(), cfg, client, state.Namespace, resource.MustParse(state.TargetSize)); err != nil {
		t.Fatalf("resume phase %s quiet=%t: %v", phase, quiet, err)
	}
	actions := make([]string, 0, len(client.Actions()))
	for _, action := range client.Actions() {
		actions = append(actions, fmt.Sprintf("%s %s %s", action.GetVerb(), action.GetResource().Resource, action.GetSubresource()))
	}
	return out.String(), actions, len(mover.calls)
}

type observingFakeMover struct {
	calls []string
}

func (m *observingFakeMover) Move(_ context.Context, request datamover.Request) error {
	m.calls = append(m.calls, "move")
	if request.Observe != nil {
		request.Observe(datamover.Observation{JobName: request.JobName, PodName: "copy-pod", PodPhase: corev1.PodPending, WaitingReason: "ContainerCreating"})
		request.Observe(datamover.Observation{JobName: request.JobName, PodName: "copy-pod", PodPhase: corev1.PodRunning})
		request.Observe(datamover.Observation{JobName: request.JobName, PodName: "copy-pod", LogRecord: "1,024 100% 1.0MB/s", FinalRecord: true})
		request.Observe(datamover.Observation{JobName: request.JobName, Cleanup: true})
	}
	return nil
}

func (m *observingFakeMover) Verify(_ context.Context, request datamover.Request) error {
	m.calls = append(m.calls, "verify")
	if request.Observe != nil {
		request.Observe(datamover.Observation{JobName: request.JobName, PodName: "verify-pod", PodPhase: corev1.PodRunning})
		request.Observe(datamover.Observation{JobName: request.JobName, JobCondition: "Complete"})
		request.Observe(datamover.Observation{JobName: request.JobName, Cleanup: true})
	}
	return nil
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
