package workflow

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/keatsfonam/kubectl-shrink-pvc/internal/datamover"
	"github.com/keatsfonam/kubectl-shrink-pvc/internal/kube"
	"github.com/keatsfonam/kubectl-shrink-pvc/internal/operation"
	liveprogress "github.com/keatsfonam/kubectl-shrink-pvc/internal/progress"
)

func TestRunRejectsDryRunResumeBeforeClusterAccess(t *testing.T) {
	err := Run(context.Background(), Config{DryRun: true, Resume: true})
	if err == nil || !strings.Contains(err.Error(), "cannot be combined") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRunRejectsNonPositiveTargetBeforeClusterAccess(t *testing.T) {
	for _, size := range []string{"0", "-1Gi"} {
		err := Run(context.Background(), Config{TargetSize: size, Timeout: time.Second, RunAsUser: -1})
		if err == nil || !strings.Contains(err.Error(), "must be positive") {
			t.Errorf("size %s: unexpected error %v", size, err)
		}
	}
}

func TestValidateDestructiveBoundaryRejectsReplacement(t *testing.T) {
	client := fake.NewSimpleClientset(&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "data", Namespace: "ns", UID: "replacement"}})
	err := validateDestructiveBoundary(context.Background(), client, "ns", "data", "original", nil, true)
	if err == nil || !strings.Contains(err.Error(), "was replaced") {
		t.Fatalf("expected replacement error, got %v", err)
	}
}

func TestValidateDestructiveBoundaryRejectsNewPod(t *testing.T) {
	client := fake.NewSimpleClientset(
		&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "data", Namespace: "ns", UID: "uid"}},
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "late-pod", Namespace: "ns"},
			Spec:       corev1.PodSpec{Volumes: []corev1.Volume{{Name: "data", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "data"}}}}},
			Status:     corev1.PodStatus{Phase: corev1.PodRunning},
		},
	)
	err := validateDestructiveBoundary(context.Background(), client, "ns", "data", "uid", nil, true)
	if err == nil || !strings.Contains(err.Error(), "gained active consumers") {
		t.Fatalf("expected active consumer error, got %v", err)
	}
}

func TestRevalidateExecutionPlanRejectsChangedSourceUID(t *testing.T) {
	client := fake.NewSimpleClientset(&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "data", Namespace: "ns", UID: "new"}})
	approvedSource := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "data", Namespace: "ns", UID: "old"}}
	_, _, err := revalidateExecutionPlan(context.Background(), client, "ns", "data", approvedSource, &kube.ConsumerPlan{}, true)
	if err == nil || !strings.Contains(err.Error(), "was replaced after planning") {
		t.Fatalf("expected source replacement error, got %v", err)
	}
}

func TestResumeRequiresRecoveryPVCBeforeDeletingSource(t *testing.T) {
	finalPVC := pvc("data", "ns", "1Gi")
	operation.StampRecreatedPVC(finalPVC, "op")
	finalJSON, err := json.Marshal(finalPVC)
	if err != nil {
		t.Fatal(err)
	}
	source := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "data", Namespace: "ns", UID: "original"}}
	client := fake.NewSimpleClientset(source)
	store := operation.Store{Client: client, Namespace: "ns", Name: operation.NameForPVC("data")}
	state := &operation.State{
		Version: 1, OperationID: "op", Namespace: "ns", SourceName: "data",
		OriginalSourceUID: "original", TempName: "missing-temp", TempUID: "temp-uid",
		TargetSize: "1Gi", Image: "image", RunAsUser: -1, FSGroup: -1,
		FinalPVCJSON: finalJSON, Phase: operation.PhaseCopiedToTemp,
	}
	if _, err := store.Create(context.Background(), state); err != nil {
		t.Fatalf("create state: %v", err)
	}

	err = resume(context.Background(), Config{PVCName: "data", Image: "image", RunAsUser: -1, FSGroup: -1}, client, "ns", resource.MustParse("1Gi"))
	if err == nil || !strings.Contains(err.Error(), "temporary recovery PVC") {
		t.Fatalf("expected missing recovery PVC error, got %v", err)
	}
	got, getErr := client.CoreV1().PersistentVolumeClaims("ns").Get(context.Background(), "data", metav1.GetOptions{})
	if getErr != nil || got.UID != "original" {
		t.Fatalf("original source was changed: pvc=%#v err=%v", got, getErr)
	}
}

func TestResumeDeleteAcceptedRequiresRecoveryPVCBeforeRecreate(t *testing.T) {
	finalPVC := pvc("data", "ns", "1Gi")
	operation.StampRecreatedPVC(finalPVC, "op")
	finalJSON, err := json.Marshal(finalPVC)
	if err != nil {
		t.Fatal(err)
	}
	client := fake.NewSimpleClientset()
	store := operation.Store{Client: client, Namespace: "ns", Name: operation.NameForPVC("data")}
	state := &operation.State{
		Version: 1, OperationID: "op", Namespace: "ns", SourceName: "data",
		OriginalSourceUID: "original", TempName: "missing-temp", TempUID: "temp-uid",
		TargetSize: "1Gi", Image: "image", RunAsUser: -1, FSGroup: -1,
		FinalPVCJSON: finalJSON, Phase: operation.PhaseSourceDeleteAccepted,
	}
	if _, err := store.Create(context.Background(), state); err != nil {
		t.Fatal(err)
	}

	err = resume(context.Background(), Config{PVCName: "data", Image: "image", RunAsUser: -1, FSGroup: -1, Timeout: time.Second}, client, "ns", resource.MustParse("1Gi"))
	if err == nil || !strings.Contains(err.Error(), "temporary recovery PVC") {
		t.Fatalf("expected missing recovery PVC error, got %v", err)
	}
	if _, getErr := client.CoreV1().PersistentVolumeClaims("ns").Get(context.Background(), "data", metav1.GetOptions{}); getErr == nil {
		t.Fatal("source PVC was recreated without recovery data")
	}
}

func TestResumeRejectsChangedFSGroup(t *testing.T) {
	client := fake.NewSimpleClientset()
	store := operation.Store{Client: client, Namespace: "ns", Name: operation.NameForPVC("data")}
	state := &operation.State{
		Version: 1, OperationID: "op", Namespace: "ns", SourceName: "data",
		TargetSize: "1Gi", Image: "image", RunAsUser: 1000, FSGroup: 2000,
		Phase: operation.PhaseCopiedToTemp,
	}
	if _, err := store.Create(context.Background(), state); err != nil {
		t.Fatal(err)
	}
	err := resume(context.Background(), Config{PVCName: "data", Image: "image", RunAsUser: 1000, FSGroup: 3000}, client, "ns", resource.MustParse("1Gi"))
	if err == nil || !strings.Contains(err.Error(), "run-as settings") {
		t.Fatalf("expected security context mismatch, got %v", err)
	}
}

func TestResumeRejectsReplacedSourceBeforeRestoration(t *testing.T) {
	finalPVC := pvc("data", "ns", "1Gi")
	operation.StampRecreatedPVC(finalPVC, "op")
	finalJSON, err := json.Marshal(finalPVC)
	if err != nil {
		t.Fatal(err)
	}
	client := fake.NewSimpleClientset(&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "data", Namespace: "ns", UID: "replacement"}})
	store := operation.Store{Client: client, Namespace: "ns", Name: operation.NameForPVC("data")}
	state := &operation.State{
		Version: 1, OperationID: "op", Namespace: "ns", SourceName: "data",
		OriginalSourceUID: "original", RecreatedSourceUID: "expected", TempName: "temp", TempUID: "temp-uid",
		TargetSize: "1Gi", Image: "image", RunAsUser: -1, FSGroup: -1,
		FinalPVCJSON: finalJSON, Phase: operation.PhaseCopiedBack,
	}
	if _, err := store.Create(context.Background(), state); err != nil {
		t.Fatalf("create state: %v", err)
	}

	err = resume(context.Background(), Config{PVCName: "data", Image: "image", RunAsUser: -1, FSGroup: -1}, client, "ns", resource.MustParse("1Gi"))
	if err == nil || !strings.Contains(err.Error(), "replaced before restoration") {
		t.Fatalf("expected replacement error, got %v", err)
	}
}

func TestResumeRejectsChangedTarget(t *testing.T) {
	client := fake.NewSimpleClientset()
	store := operation.Store{Client: client, Namespace: "ns", Name: operation.NameForPVC("data")}
	state := &operation.State{
		Version: 1, OperationID: "op", Namespace: "ns", SourceName: "data",
		OriginalSourceUID: "source", TempName: "temp", TempUID: "temp-uid",
		TargetSize: "1Gi", Image: "image", RunAsUser: -1, FSGroup: -1,
		Phase: operation.PhaseCopiedToTemp,
	}
	if _, err := store.Create(context.Background(), state); err != nil {
		t.Fatalf("create state: %v", err)
	}

	err := resume(context.Background(), Config{PVCName: "data", Image: "image", RunAsUser: -1, FSGroup: -1}, client, "ns", resource.MustParse("2Gi"))
	if err == nil || !strings.Contains(err.Error(), "persisted operation target") {
		t.Fatalf("expected target mismatch, got %v", err)
	}
}

func TestNormalizeRsyncArgs(t *testing.T) {
	got, err := normalizeRsyncArgs([]string{"--exclude=path with spaces"}, "--partial --bwlimit=10m")
	if err != nil {
		t.Fatalf("normalizeRsyncArgs returned error: %v", err)
	}
	want := []string{"--partial", "--bwlimit=10m", "--exclude=path with spaces"}
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("got %v, want %v", got, want)
	}
	if _, err := normalizeRsyncArgs([]string{"unexpected-operand"}, ""); err == nil {
		t.Fatal("expected operand rejection")
	}
	for _, arg := range []string{"--archive", "-a", "--no-perms", "--chmod=Du=rwx", "-pog"} {
		if _, err := normalizeRsyncArgs([]string{arg}, ""); err == nil || !strings.Contains(err.Error(), "metadata preservation policy") {
			t.Fatalf("expected metadata policy rejection for %q, got %v", arg, err)
		}
	}
}

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
	client := fake.NewSimpleClientset(ownedTempPVC("data-shrink", "ns", "1Gi", "source-uid", "data"))
	target := resource.MustParse("2Gi")
	tempPVC := ownedTempPVC("data-shrink", "ns", "2Gi", "source-uid", "data")

	_, _, err := ensureTemporaryPVC(context.Background(), client, "ns", tempPVC, target)
	if err == nil {
		t.Fatal("expected size mismatch error")
	}
	if !strings.Contains(err.Error(), "delete it or pass a different --temp-name") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestEnsureTemporaryPVCRejectsExistingWithoutOwnership(t *testing.T) {
	client := fake.NewSimpleClientset(pvc("data-shrink", "ns", "2Gi"))
	target := resource.MustParse("2Gi")
	tempPVC := ownedTempPVC("data-shrink", "ns", "2Gi", "source-uid", "data")

	_, _, err := ensureTemporaryPVC(context.Background(), client, "ns", tempPVC, target)
	if err == nil || !strings.Contains(err.Error(), "not owned by this source PVC") {
		t.Fatalf("expected ownership error, got %v", err)
	}
}

func TestEnsureTemporaryPVCReusesExistingMatchingSize(t *testing.T) {
	client := fake.NewSimpleClientset(ownedTempPVC("data-shrink", "ns", "2Gi", "source-uid", "data"))
	target := resource.MustParse("2Gi")
	tempPVC := ownedTempPVC("data-shrink", "ns", "2Gi", "source-uid", "data")

	got, reused, err := ensureTemporaryPVC(context.Background(), client, "ns", tempPVC, target)
	if err != nil {
		t.Fatalf("ensureTemporaryPVC returned error: %v", err)
	}
	if !reused {
		t.Fatal("expected existing matching temp PVC to be reused")
	}
	if got.Name != tempPVC.Name {
		t.Fatalf("unexpected PVC returned: %s", got.Name)
	}
}

func TestCopyActivityUsesIndependentPerCopyLabels(t *testing.T) {
	first := copyActivity("copy 1 of 2: source to temporary", "1,024 25% 1.0MB/s")
	second := copyActivity("copy 2 of 2: temporary to source", "2,048 75% 2.0MB/s")
	for _, test := range []struct {
		got   string
		label string
		pct   string
	}{
		{got: first, label: "copy 1 of 2: source to temporary", pct: "per-copy progress=25%"},
		{got: second, label: "copy 2 of 2: temporary to source", pct: "per-copy progress=75%"},
	} {
		if !strings.Contains(test.got, test.label) || !strings.Contains(test.got, test.pct) {
			t.Fatalf("copy activity %q missing label or per-copy percentage", test.got)
		}
		if strings.Contains(strings.ToLower(test.got), "overall") || strings.Contains(strings.ToLower(test.got), "eta") {
			t.Fatalf("copy activity claims non-portable aggregate progress: %q", test.got)
		}
	}
}

func TestVerificationJobCompletionDoesNotPublishCompletedDisplayPhase(t *testing.T) {
	var out bytes.Buffer
	cfg := Config{}
	cfg.reporter = liveprogress.New(&out, false)
	observer := moverObserver(cfg, moverProgressSpec{
		scheduling: liveprogress.VerifyTempScheduling,
		mounting:   liveprogress.VerifyTempScheduling,
		running:    liveprogress.VerifyTempChecksumming,
	})
	observer(datamover.Observation{JobName: "verify", JobCondition: "Complete"})
	cfg.reporter.Close()

	got := out.String()
	if !strings.Contains(got, "verify-temp/checksumming") {
		t.Fatalf("Job completion did not retain checksumming phase: %s", got)
	}
	if strings.Contains(got, "verify-temp/completed") {
		t.Fatalf("Job condition alone incorrectly published verification completion: %s", got)
	}
}

func TestQuietRetainsDurableStdoutAndStderrContracts(t *testing.T) {
	for _, quiet := range []bool{false, true} {
		t.Run(map[bool]string{false: "default", true: "quiet"}[quiet], func(t *testing.T) {
			var out, errOut bytes.Buffer
			cfg := Config{PVCName: "data", TempName: "data-temp", SafetyMarginPercent: 10, Quiet: quiet}
			cfg.IOStreams.In = strings.NewReader("yes\n")
			cfg.IOStreams.Out = &out
			cfg.IOStreams.ErrOut = &errOut
			cfg.reporter = liveprogress.New(&out, quiet)

			source := pvc("data", "ns", "2Gi")
			printPlan(cfg, "ns", source, resource.MustParse("1Gi"), &kube.ConsumerPlan{})
			if err := confirm(cfg); err != nil {
				t.Fatalf("confirm returned error: %v", err)
			}
			setProgressPhase(cfg, liveprogress.InspectScanning, "inspection Pod running")
			durableOutput(cfg, cfg.IOStreams.Out, "Source usage: 100 bytes; required with 10% margin: 110 bytes; target: 1000 bytes.\n")
			durableOutput(cfg, cfg.IOStreams.Out, "\nDry-run only; no changes made.\n")
			setProgressPhase(cfg, liveprogress.CleanupCheckpoint, "checkpoint removed")
			durableOutput(cfg, cfg.IOStreams.Out, "PVC shrink workflow completed successfully.\n")
			durableOutput(cfg, cfg.IOStreams.ErrOut, "Warning: preserved warning text.\n")
			cfg.reporter.Close()

			got := out.String()
			for _, required := range []string{
				"PVC shrink plan", "Continue? Type 'yes' to proceed:", "Source usage: 100 bytes",
				"Dry-run only; no changes made.", "PVC shrink workflow completed successfully.",
			} {
				if !strings.Contains(got, required) {
					t.Fatalf("quiet=%t output missing %q:\n%s", quiet, required, got)
				}
			}
			if !strings.Contains(got, "Continue? Type 'yes' to proceed: \n") {
				t.Fatalf("quiet=%t confirmation prompt was joined to later output:\n%s", quiet, got)
			}
			if errOut.String() != "Warning: preserved warning text.\n" {
				t.Fatalf("quiet=%t stderr = %q", quiet, errOut.String())
			}
			if quiet && strings.Contains(got, "[progress]") {
				t.Fatalf("quiet output contains progress: %s", got)
			}
			if !quiet && !strings.Contains(got, "inspect/scanning") {
				t.Fatalf("default output missing progress: %s", got)
			}
		})
	}
}

func ownedTempPVC(name, namespace, size, sourceUID, sourceName string) *corev1.PersistentVolumeClaim {
	claim := pvc(name, namespace, size)
	claim.Annotations = map[string]string{
		tempSourceUIDAnnotation:         sourceUID,
		tempSourceNameAnnotation:        sourceName,
		operation.AnnotationOperationID: "test-operation",
	}
	return claim
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
