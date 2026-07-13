package datamover

import (
	"context"
	"errors"
	"io"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
)

func TestJobDisablesServiceAccountToken(t *testing.T) {
	job := buildJob(Request{Namespace: "ns", SourcePVC: "source", DestPVC: "dest", Image: "image", RunAsUser: -1, FSGroup: -1}, "job", []string{"/src/", "/dest/"}, false, 0, false)
	value := job.Spec.Template.Spec.AutomountServiceAccountToken
	if value == nil || *value {
		t.Fatal("rsync Job must disable service account token automount")
	}
}

func TestRsyncArgs(t *testing.T) {
	got := rsyncArgs([]string{"--partial", "--exclude=path with spaces"}, false)
	want := []string{"-aHAX", "--numeric-ids", "--exclude=lost+found", "--delete", "--info=progress2", "--partial", "--exclude=path with spaces", "/src/", "/dest/"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected args:\n got: %#v\nwant: %#v", got, want)
	}
}

func TestRsyncArgsNonRoot(t *testing.T) {
	got := rsyncArgs(nil, true)
	want := []string{"-rlHt", "-O", "--exclude=lost+found", "--delete", "--info=progress2", "/src/", "/dest/"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected args:\n got: %#v\nwant: %#v", got, want)
	}
}

func TestBuildJobUsesDirectRsyncArgv(t *testing.T) {
	args := []string{"--partial", "/src/", "/dest/"}
	job := buildJob(Request{Namespace: "ns", SourcePVC: "source", DestPVC: "dest", Image: "image"}, "job", args, false, 0, false)
	container := job.Spec.Template.Spec.Containers[0]
	if !reflect.DeepEqual(container.Command, []string{"rsync"}) {
		t.Fatalf("unexpected command: %#v", container.Command)
	}
	if !reflect.DeepEqual(container.Args, args) {
		t.Fatalf("unexpected args: %#v", container.Args)
	}
}

func TestVerificationJobMountsBothPVCsReadOnly(t *testing.T) {
	job := buildJob(Request{Namespace: "ns", SourcePVC: "source", DestPVC: "dest", Image: "image"}, "verify", verifyArgs(nil, false), false, 0, true)
	for _, mount := range job.Spec.Template.Spec.Containers[0].VolumeMounts {
		if !mount.ReadOnly {
			t.Fatalf("verification mount %s is writable", mount.Name)
		}
	}
	for _, volume := range job.Spec.Template.Spec.Volumes {
		if volume.PersistentVolumeClaim == nil || !volume.PersistentVolumeClaim.ReadOnly {
			t.Fatalf("verification volume %s is writable", volume.Name)
		}
	}
}

func TestVerificationDifferencesNoChanges(t *testing.T) {
	logs := "Number of files: 2\ntotal size is 42  speedup is 1.00 (DRY RUN)\n"
	got, err := verificationDifferences(logs)
	if err != nil || got != "" {
		t.Fatalf("verificationDifferences() = %q, %v", got, err)
	}
}

func TestVerificationDifferencesIgnoresUnchangedRecords(t *testing.T) {
	logs := verificationRecordPrefix + ".f          files/unchanged\n" + verificationRecordPrefix + ".d          files/dir\ntotal size is 42  speedup is 1.00 (DRY RUN)\n"
	got, err := verificationDifferences(logs)
	if err != nil || got != "" {
		t.Fatalf("verificationDifferences() = %q, %v", got, err)
	}
}

func TestVerificationDifferencesReportsChanges(t *testing.T) {
	logs := verificationRecordPrefix + ">fc........ files/changed\n" + verificationRecordPrefix + "*deleting   files/extra\ntotal size is 42  speedup is 1.00 (DRY RUN)\n"
	want := ">fc........ files/changed\n*deleting   files/extra"
	got, err := verificationDifferences(logs)
	if err != nil || got != want {
		t.Fatalf("verificationDifferences() = %q, %v; want %q", got, err, want)
	}
}

func TestVerificationDifferencesRequiresCompletionSentinel(t *testing.T) {
	if got, err := verificationDifferences(verificationRecordPrefix + ">fc........ files/changed\n"); err == nil || got != "" {
		t.Fatalf("verificationDifferences() = %q, %v; want missing-sentinel error", got, err)
	}
	if !isVerificationSentinel("total size is 25,174,016  speedup is 123,456.78 (DRY RUN)") {
		t.Fatal("rejected valid C-locale sentinel with grouped numbers")
	}
	for _, invalid := range []string{"total size is forged", "total size is 1 speedup is nope", "not total size is 1 speedup is 1"} {
		if isVerificationSentinel(invalid) {
			t.Fatalf("accepted invalid sentinel %q", invalid)
		}
	}
}

func TestVerifyArgs(t *testing.T) {
	got := verifyArgs([]string{"--exclude=cache", "--bwlimit=10m"}, false)
	if got[len(got)-2] != "/src/" || got[len(got)-1] != "/dest/" {
		t.Fatalf("unexpected verification operands: %#v", got)
	}
	for _, required := range []string{"-aHAXniO", "--numeric-ids", "--checksum", "--delete", "--itemize-changes", "--exclude=cache"} {
		found := false
		for _, arg := range got {
			if arg == required {
				found = true
			}
		}
		if !found {
			t.Fatalf("verification args missing %s: %#v", required, got)
		}
	}
	foundProtocolFormat := false
	for _, arg := range got {
		if arg == "--bwlimit=10m" {
			t.Fatal("non-selection copy option must not alter verification")
		}
		if arg == "--out-format="+verificationRecordPrefix+"%i %n%L" {
			foundProtocolFormat = true
		}
	}
	if !foundProtocolFormat {
		t.Fatal("verification args must use the reserved record prefix")
	}
}

func TestVerifyArgsNonRootOmitsPrivilegedMetadata(t *testing.T) {
	got := verifyArgs(nil, true)
	if got[0] != "-rlHtniO" {
		t.Fatalf("unexpected non-root verification policy: %#v", got)
	}
}

func TestUniqueNameFitsDNSLabel(t *testing.T) {
	got := uniqueName("abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz")
	if len(got) > 63 {
		t.Fatalf("uniqueName returned %d chars", len(got))
	}
	if got == "" {
		t.Fatal("uniqueName returned empty string")
	}
}

func TestVerifySuccess(t *testing.T) {
	mover, client := verifyTestMover(t, batchv1.JobComplete, true, "Number of files: 1\ntotal size is 5  speedup is 1.00 (DRY RUN)\n", nil)
	if err := mover.Verify(context.Background(), verifyTestRequest()); err != nil {
		t.Fatalf("Verify() error = %v", err)
	}
	assertNoJobsOrPods(t, client)
}

func TestVerifyDifferences(t *testing.T) {
	logs := verificationRecordPrefix + ">fc........ changed\nNumber of files: 1\ntotal size is 5  speedup is 1.00 (DRY RUN)\n"
	mover, client := verifyTestMover(t, batchv1.JobComplete, true, logs, nil)
	err := mover.Verify(context.Background(), verifyTestRequest())
	if err == nil || !strings.Contains(err.Error(), ">fc........ changed") {
		t.Fatalf("Verify() error = %v, want reported difference", err)
	}
	assertNoJobsOrPods(t, client)
}

func TestVerifyFailsClosedWithoutPods(t *testing.T) {
	mover, _ := verifyTestMover(t, batchv1.JobComplete, false, "", nil)
	if err := mover.Verify(context.Background(), verifyTestRequest()); err == nil || !strings.Contains(err.Error(), "no rsync job pods") {
		t.Fatalf("Verify() error = %v", err)
	}
}

func TestVerifyFailsClosedWithoutCompletionSentinel(t *testing.T) {
	mover, _ := verifyTestMover(t, batchv1.JobComplete, true, "", nil)
	if err := mover.Verify(context.Background(), verifyTestRequest()); err == nil || !strings.Contains(err.Error(), "completion sentinel not found") {
		t.Fatalf("Verify() error = %v", err)
	}
}

func TestVerifyFailsClosedOnPodListError(t *testing.T) {
	mover, client := verifyTestMover(t, batchv1.JobComplete, true, "", nil)
	client.PrependReactor("list", "pods", func(ktesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("pods forbidden")
	})
	if err := mover.Verify(context.Background(), verifyTestRequest()); err == nil || !strings.Contains(err.Error(), "pods forbidden") {
		t.Fatalf("Verify() error = %v", err)
	}
}

func TestVerifyFailsClosedOnLogOpenError(t *testing.T) {
	mover, _ := verifyTestMover(t, batchv1.JobComplete, true, "", errors.New("logs forbidden"))
	if err := mover.Verify(context.Background(), verifyTestRequest()); err == nil || !strings.Contains(err.Error(), "logs forbidden") {
		t.Fatalf("Verify() error = %v", err)
	}
}

func TestVerifyFailsClosedOnLogReadError(t *testing.T) {
	mover, _ := verifyTestMover(t, batchv1.JobComplete, true, "", nil)
	mover.readLogs = func(context.Context, string, string, *corev1.PodLogOptions) (io.ReadCloser, error) {
		return &errorReadCloser{readErr: errors.New("broken stream")}, nil
	}
	if err := mover.Verify(context.Background(), verifyTestRequest()); err == nil || !strings.Contains(err.Error(), "broken stream") {
		t.Fatalf("Verify() error = %v", err)
	}
}

func TestVerifyFailsClosedOnOversizedLogs(t *testing.T) {
	mover, _ := verifyTestMover(t, batchv1.JobComplete, true, strings.Repeat("x", maxJobLogBytes+1), nil)
	if err := mover.Verify(context.Background(), verifyTestRequest()); err == nil || !strings.Contains(err.Error(), "exceed") {
		t.Fatalf("Verify() error = %v", err)
	}
}

func TestVerifyJobFailureIncludesLogsAndCleansUp(t *testing.T) {
	mover, client := verifyTestMover(t, batchv1.JobFailed, true, "rsync: permission denied", nil)
	err := mover.Verify(context.Background(), verifyTestRequest())
	if err == nil || !strings.Contains(err.Error(), "permission denied") || !strings.Contains(err.Error(), "job failed") {
		t.Fatalf("Verify() error = %v", err)
	}
	assertNoJobsOrPods(t, client)
}

func TestVerifyCleanupFailure(t *testing.T) {
	mover, client := verifyTestMover(t, batchv1.JobComplete, true, "total size is 0  speedup is 1.00 (DRY RUN)", nil)
	client.PrependReactor("delete", "jobs", func(ktesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("delete forbidden")
	})
	if err := mover.Verify(context.Background(), verifyTestRequest()); err == nil || !strings.Contains(err.Error(), "delete forbidden") {
		t.Fatalf("Verify() error = %v", err)
	}
}

func verifyTestRequest() Request {
	return Request{
		Namespace: "ns", SourcePVC: "source", DestPVC: "dest", Image: "image", JobName: "test",
		RunAsUser: -1, FSGroup: -1, WaitTimeout: 100 * time.Millisecond, PollInterval: time.Millisecond,
	}
}

func verifyTestMover(t *testing.T, condition batchv1.JobConditionType, createPod bool, logs string, logErr error) (RsyncMover, *fake.Clientset) {
	t.Helper()
	client := fake.NewSimpleClientset()
	client.PrependReactor("delete-collection", "pods", func(action ktesting.Action) (bool, runtime.Object, error) {
		namespace := action.GetNamespace()
		objects, err := client.Tracker().List(schema.GroupVersionResource{Version: "v1", Resource: "pods"}, schema.GroupVersionKind{Version: "v1", Kind: "Pod"}, namespace)
		if err != nil {
			return true, nil, err
		}
		for _, pod := range objects.(*corev1.PodList).Items {
			if err := client.Tracker().Delete(schema.GroupVersionResource{Version: "v1", Resource: "pods"}, namespace, pod.Name); err != nil {
				return true, nil, err
			}
		}
		return true, nil, nil
	})
	client.PrependReactor("create", "jobs", func(action ktesting.Action) (bool, runtime.Object, error) {
		job := action.(ktesting.CreateAction).GetObject().(*batchv1.Job)
		if createPod {
			pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
				Name: "verify-pod", Namespace: job.Namespace,
				Labels: map[string]string{"shrink-pvc-job": job.Name},
			}}
			if err := client.Tracker().Create(schema.GroupVersionResource{Version: "v1", Resource: "pods"}, pod, job.Namespace); err != nil {
				return true, nil, err
			}
		}
		return false, nil, nil
	})
	client.PrependReactor("get", "jobs", func(action ktesting.Action) (bool, runtime.Object, error) {
		get := action.(ktesting.GetAction)
		obj, err := client.Tracker().Get(schema.GroupVersionResource{Group: "batch", Version: "v1", Resource: "jobs"}, get.GetNamespace(), get.GetName())
		if err != nil {
			return true, nil, err
		}
		job := obj.(*batchv1.Job).DeepCopy()
		job.Status.Conditions = []batchv1.JobCondition{{Type: condition, Status: corev1.ConditionTrue, Message: "test condition"}}
		return true, job, nil
	})
	mover := RsyncMover{Client: client, readLogs: func(context.Context, string, string, *corev1.PodLogOptions) (io.ReadCloser, error) {
		if logErr != nil {
			return nil, logErr
		}
		return io.NopCloser(strings.NewReader(logs)), nil
	}}
	return mover, client
}

func assertNoJobsOrPods(t *testing.T, client *fake.Clientset) {
	t.Helper()
	jobs, err := client.BatchV1().Jobs("ns").List(context.Background(), metav1.ListOptions{})
	if err != nil || len(jobs.Items) != 0 {
		t.Fatalf("jobs after Verify = %d, %v", len(jobs.Items), err)
	}
	pods, err := client.CoreV1().Pods("ns").List(context.Background(), metav1.ListOptions{})
	if err != nil || len(pods.Items) != 0 {
		t.Fatalf("pods after Verify = %d, %v", len(pods.Items), err)
	}
}

type errorReadCloser struct {
	readErr error
}

func (r *errorReadCloser) Read([]byte) (int, error) { return 0, r.readErr }
func (r *errorReadCloser) Close() error             { return nil }

func TestLogRecordParserHandlesEveryChunkBoundary(t *testing.T) {
	input := []byte("first\rprogress 10%\r\nthird\nfinal partial")
	want := []string{"first", "progress 10%", "third", "final partial"}
	for split := 0; split <= len(input); split++ {
		parser := &logRecordParser{}
		var got []string
		parser.Write(input[:split], func(record string) { got = append(got, record) })
		parser.Write(input[split:], func(record string) { got = append(got, record) })
		parser.Flush(func(record string) { got = append(got, record) })
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("split %d: records = %#v, want %#v", split, got, want)
		}
	}

	parser := &logRecordParser{}
	var bytewise []string
	for _, b := range input {
		parser.Write([]byte{b}, func(record string) { bytewise = append(bytewise, record) })
	}
	parser.Flush(func(record string) { bytewise = append(bytewise, record) })
	if !reflect.DeepEqual(bytewise, want) {
		t.Fatalf("bytewise records = %#v, want %#v", bytewise, want)
	}
}

func TestLogRecordParserBoundsOversizedPartialRecord(t *testing.T) {
	parser := &logRecordParser{}
	var got []string
	parser.Write([]byte(strings.Repeat("x", maxLiveLogRecordBytes+100)+"\n"), func(record string) { got = append(got, record) })
	if len(got) != 1 || !strings.HasSuffix(got[0], "…") {
		t.Fatalf("oversized record was not bounded and marked: %#v", got)
	}
	if len(got[0]) > maxLiveLogRecordBytes+len("…") {
		t.Fatalf("bounded record has %d bytes", len(got[0]))
	}
}

func TestMoveStreamFailuresDoNotChangeSuccessfulJobResult(t *testing.T) {
	tests := []struct {
		name       string
		stream     func(chan struct{}) podLogReader
		wantRecord string
	}{
		{
			name: "open failure",
			stream: func(gate chan struct{}) podLogReader {
				var once sync.Once
				return func(context.Context, string, string, *corev1.PodLogOptions) (io.ReadCloser, error) {
					once.Do(func() { close(gate) })
					return nil, errors.New("live logs forbidden")
				}
			},
		},
		{
			name: "read failure after data",
			stream: func(gate chan struct{}) podLogReader {
				return func(context.Context, string, string, *corev1.PodLogOptions) (io.ReadCloser, error) {
					return &oneReadCloser{data: []byte("progress 57%\rfinal partial"), readErr: errors.New("stream interrupted"), onRead: func() { close(gate) }}, nil
				}
			},
			wantRecord: "final partial",
		},
		{
			name: "close failure",
			stream: func(gate chan struct{}) podLogReader {
				return func(context.Context, string, string, *corev1.PodLogOptions) (io.ReadCloser, error) {
					return &oneReadCloser{readErr: io.EOF, closeErr: errors.New("close failed"), onRead: func() { close(gate) }}, nil
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gate := make(chan struct{})
			mover, client := moveTestMover(t, batchv1.JobComplete, gate, "diagnostic logs")
			mover.streamLogs = tt.stream(gate)
			var mu sync.Mutex
			var observations []Observation
			req := moveTestRequest()
			req.Observe = func(observation Observation) {
				mu.Lock()
				observations = append(observations, observation)
				mu.Unlock()
			}
			if err := mover.Move(context.Background(), req); err != nil {
				t.Fatalf("Move() changed successful Job result: %v", err)
			}
			assertNoJobsOrPods(t, client)
			mu.Lock()
			defer mu.Unlock()
			if tt.wantRecord != "" {
				found := false
				for _, observation := range observations {
					if observation.LogRecord == tt.wantRecord && observation.FinalRecord {
						found = true
					}
				}
				if !found {
					t.Fatalf("final partial record missing from observations: %#v", observations)
				}
			}
		})
	}
}

func TestMoveRetriesLiveStreamSetupWithoutChangingJobPolling(t *testing.T) {
	gate := make(chan struct{})
	mover, client := moveTestMover(t, batchv1.JobComplete, gate, "diagnostic logs")
	var mu sync.Mutex
	attempts := 0
	mover.streamLogs = func(context.Context, string, string, *corev1.PodLogOptions) (io.ReadCloser, error) {
		mu.Lock()
		defer mu.Unlock()
		attempts++
		if attempts == 1 {
			return nil, errors.New("container is waiting to start")
		}
		return &oneReadCloser{data: []byte("progress 80%\r"), readErr: io.EOF, onRead: func() { close(gate) }}, nil
	}
	var recordsMu sync.Mutex
	var records []string
	req := moveTestRequest()
	req.Observe = func(observation Observation) {
		if observation.LogRecord != "" {
			recordsMu.Lock()
			records = append(records, observation.LogRecord)
			recordsMu.Unlock()
		}
	}
	if err := mover.Move(context.Background(), req); err != nil {
		t.Fatalf("Move() error = %v", err)
	}
	mu.Lock()
	gotAttempts := attempts
	mu.Unlock()
	if gotAttempts < 2 {
		t.Fatalf("live stream setup attempts = %d, want retry", gotAttempts)
	}
	recordsMu.Lock()
	gotRecords := append([]string(nil), records...)
	recordsMu.Unlock()
	if !slices.Contains(gotRecords, "progress 80%") {
		t.Fatalf("retried stream records = %#v", gotRecords)
	}
	assertNoJobsOrPods(t, client)
}

func TestMoveFailureStillUsesSeparateBoundedDiagnosticLogs(t *testing.T) {
	gate := make(chan struct{})
	mover, client := moveTestMover(t, batchv1.JobFailed, gate, "bounded diagnostic logs")
	var gateOnce sync.Once
	mover.streamLogs = func(context.Context, string, string, *corev1.PodLogOptions) (io.ReadCloser, error) {
		gateOnce.Do(func() { close(gate) })
		return nil, errors.New("live stream unavailable")
	}
	req := moveTestRequest()
	req.Observe = func(Observation) {}
	err := mover.Move(context.Background(), req)
	if err == nil || !strings.Contains(err.Error(), "job failed") || !strings.Contains(err.Error(), "bounded diagnostic logs") {
		t.Fatalf("Move() error = %v, want Job failure plus diagnostic logs", err)
	}
	assertNoJobsOrPods(t, client)
}

func TestMoveCancellationClosesLiveStreamAndCleansUp(t *testing.T) {
	mover, client := moveTestMover(t, "", nil, "cancel diagnostics")
	opened := make(chan struct{})
	stream := newBlockingReadCloser()
	mover.streamLogs = func(context.Context, string, string, *corev1.PodLogOptions) (io.ReadCloser, error) {
		close(opened)
		return stream, nil
	}
	req := moveTestRequest()
	req.WaitTimeout = time.Second
	req.Observe = func(Observation) {}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- mover.Move(ctx, req) }()
	select {
	case <-opened:
	case <-time.After(time.Second):
		t.Fatal("live stream was not opened")
	}
	cancel()
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "context canceled") {
			t.Fatalf("Move() error after cancellation = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Move() did not stop after cancellation")
	}
	select {
	case <-stream.closed:
	default:
		t.Fatal("live stream was not closed")
	}
	assertNoJobsOrPods(t, client)
}

func TestVerifyNeverUsesLiveLogStream(t *testing.T) {
	mover, _ := verifyTestMover(t, batchv1.JobComplete, true, "total size is 5  speedup is 1.00 (DRY RUN)\n", nil)
	var mu sync.Mutex
	streamCalls := 0
	mover.streamLogs = func(context.Context, string, string, *corev1.PodLogOptions) (io.ReadCloser, error) {
		mu.Lock()
		streamCalls++
		mu.Unlock()
		return nil, errors.New("unexpected live stream")
	}
	req := verifyTestRequest()
	req.Observe = func(Observation) {}
	if err := mover.Verify(context.Background(), req); err != nil {
		t.Fatalf("Verify() error = %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if streamCalls != 0 {
		t.Fatalf("verification opened %d live log stream(s)", streamCalls)
	}
}

func moveTestRequest() Request {
	return Request{
		Namespace: "ns", SourcePVC: "source", DestPVC: "dest", Image: "image", JobName: "move-test",
		RunAsUser: -1, FSGroup: -1, WaitTimeout: 100 * time.Millisecond, PollInterval: time.Millisecond,
	}
}

func moveTestMover(t *testing.T, condition batchv1.JobConditionType, statusGate <-chan struct{}, diagnosticLogs string) (RsyncMover, *fake.Clientset) {
	t.Helper()
	client := fake.NewSimpleClientset()
	client.PrependReactor("delete-collection", "pods", func(action ktesting.Action) (bool, runtime.Object, error) {
		namespace := action.GetNamespace()
		objects, err := client.Tracker().List(schema.GroupVersionResource{Version: "v1", Resource: "pods"}, schema.GroupVersionKind{Version: "v1", Kind: "Pod"}, namespace)
		if err != nil {
			return true, nil, err
		}
		for _, pod := range objects.(*corev1.PodList).Items {
			if err := client.Tracker().Delete(schema.GroupVersionResource{Version: "v1", Resource: "pods"}, namespace, pod.Name); err != nil {
				return true, nil, err
			}
		}
		return true, nil, nil
	})
	client.PrependReactor("create", "jobs", func(action ktesting.Action) (bool, runtime.Object, error) {
		job := action.(ktesting.CreateAction).GetObject().(*batchv1.Job)
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "move-pod", Namespace: job.Namespace, Labels: map[string]string{"shrink-pvc-job": job.Name}},
			Status:     corev1.PodStatus{Phase: corev1.PodRunning},
		}
		if err := client.Tracker().Create(schema.GroupVersionResource{Version: "v1", Resource: "pods"}, pod, job.Namespace); err != nil {
			return true, nil, err
		}
		return false, nil, nil
	})
	client.PrependReactor("get", "jobs", func(action ktesting.Action) (bool, runtime.Object, error) {
		statusReady := false
		if statusGate != nil {
			select {
			case <-statusGate:
				statusReady = true
			default:
			}
		}
		get := action.(ktesting.GetAction)
		obj, err := client.Tracker().Get(schema.GroupVersionResource{Group: "batch", Version: "v1", Resource: "jobs"}, get.GetNamespace(), get.GetName())
		if err != nil {
			return true, nil, err
		}
		job := obj.(*batchv1.Job).DeepCopy()
		if condition != "" && statusReady {
			job.Status.Conditions = []batchv1.JobCondition{{Type: condition, Status: corev1.ConditionTrue, Message: "test condition"}}
		}
		return true, job, nil
	})
	mover := RsyncMover{
		Client: client,
		readLogs: func(context.Context, string, string, *corev1.PodLogOptions) (io.ReadCloser, error) {
			return io.NopCloser(strings.NewReader(diagnosticLogs)), nil
		},
	}
	return mover, client
}

type oneReadCloser struct {
	data     []byte
	readErr  error
	closeErr error
	onRead   func()
	read     bool
}

func (r *oneReadCloser) Read(buffer []byte) (int, error) {
	if r.read {
		return 0, io.EOF
	}
	r.read = true
	if r.onRead != nil {
		r.onRead()
	}
	return copy(buffer, r.data), r.readErr
}

func (r *oneReadCloser) Close() error { return r.closeErr }

type blockingReadCloser struct {
	closed chan struct{}
	once   sync.Once
}

func newBlockingReadCloser() *blockingReadCloser {
	return &blockingReadCloser{closed: make(chan struct{})}
}

func (r *blockingReadCloser) Read([]byte) (int, error) {
	<-r.closed
	return 0, errors.New("stream closed")
}

func (r *blockingReadCloser) Close() error {
	r.once.Do(func() { close(r.closed) })
	return nil
}
