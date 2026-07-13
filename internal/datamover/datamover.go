package datamover

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/keatsfonam/kubectl-shrink-pvc/internal/naming"
	"github.com/keatsfonam/kubectl-shrink-pvc/internal/podsec"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/utils/ptr"
)

const DefaultImage = "instrumentisto/rsync-ssh:alpine3.23-r3@sha256:6cbad37c2fbdca4ac7ad9d1c1bb8990af9efd4dc76321b349935876cbb1e9e4a"

const (
	maxJobLogBytes             = 1 << 20
	maxLiveLogRecordBytes      = 16 << 10
	verificationRecordPrefix   = "KSP_VERIFY_RECORD "
	verificationSentinelPrefix = "total size is "
)

type Observation struct {
	JobName       string
	JobCondition  string
	JobMessage    string
	Active        int32
	Succeeded     int32
	Failed        int32
	PodName       string
	PodPhase      corev1.PodPhase
	WaitingReason string
	PodCount      int
	Cleanup       bool
	LogRecord     string
	FinalRecord   bool
	StreamError   string
}

type Observer func(Observation)

type Request struct {
	Namespace    string
	SourcePVC    string
	DestPVC      string
	Image        string
	JobName      string
	Args         []string
	RunAsUser    int64
	FSGroup      int64
	WaitTimeout  time.Duration
	PollInterval time.Duration
	Observe      Observer
}

type RsyncMover struct {
	Client     kubernetes.Interface
	readLogs   podLogReader
	streamLogs podLogReader
}

type podLogReader func(context.Context, string, string, *corev1.PodLogOptions) (io.ReadCloser, error)

func (m RsyncMover) Move(ctx context.Context, req Request) error {
	runName := uniqueName(req.JobName)
	backoff := int32(0)
	nonRoot := req.RunAsUser >= 0
	args := rsyncArgs(req.Args, nonRoot)

	job := buildJob(req, runName, args, nonRoot, backoff, false)

	if _, err := m.Client.BatchV1().Jobs(req.Namespace).Create(ctx, job, metav1.CreateOptions{}); err != nil {
		return fmt.Errorf("create rsync job: %w", err)
	}
	notifyObserver(req.Observe, Observation{JobName: runName})

	liveLogs := newCopyLogStream(ctx, m.Client, m.streamLogs, req.Namespace, runName, req.Observe)
	observePod := func(observation Observation) {
		notifyObserver(req.Observe, observation)
		if observation.PodName != "" {
			liveLogs.Start(observation.PodName)
		}
	}
	podObserver := startJobPodObserver(ctx, m.Client, req.Namespace, runName, req.PollInterval, observePod)
	waitErr := waitForJob(ctx, m.Client, req.Namespace, runName, req.WaitTimeout, req.PollInterval, req.Observe)
	podObserver.Stop()
	liveLogs.Stop()
	if waitErr != nil {
		// Fetch logs and clean up on a fresh context so a cancelled caller
		// (Ctrl-C) does not leave the failed job behind, but bound it so a
		// broken API server cannot hang the exit path.
		cleanupCtx, cancel := context.WithTimeout(context.Background(), req.WaitTimeout)
		defer cancel()
		logs, _ := jobLogs(cleanupCtx, m.Client, m.readLogs, req.Namespace, runName)
		_ = cleanupJob(cleanupCtx, m.Client, req.Namespace, runName, req.WaitTimeout, req.PollInterval, req.Observe)
		if logs != "" {
			return fmt.Errorf("%w; logs: %s", waitErr, logs)
		}
		return waitErr
	}
	if err := cleanupJob(ctx, m.Client, req.Namespace, runName, req.WaitTimeout, req.PollInterval, req.Observe); err != nil {
		return err
	}
	return nil
}

func buildJob(req Request, runName string, args []string, nonRoot bool, backoff int32, destReadOnly bool) *batchv1.Job {
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: runName, Namespace: req.Namespace},
		Spec: batchv1.JobSpec{
			BackoffLimit: &backoff,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app.kubernetes.io/name": "kubectl-shrink-pvc", "shrink-pvc-job": runName}},
				Spec: corev1.PodSpec{
					RestartPolicy:                corev1.RestartPolicyNever,
					AutomountServiceAccountToken: ptr.To(false),
					SecurityContext:              podsec.Pod(req.RunAsUser, req.FSGroup),
					Containers: []corev1.Container{{
						Name:            "rsync",
						Image:           req.Image,
						Command:         []string{"rsync"},
						Args:            args,
						SecurityContext: podsec.Container(nonRoot, "CHOWN", "DAC_OVERRIDE", "FOWNER", "FSETID", "SETFCAP", "MKNOD"),
						VolumeMounts: []corev1.VolumeMount{
							{Name: "source", MountPath: "/src", ReadOnly: true},
							{Name: "dest", MountPath: "/dest", ReadOnly: destReadOnly},
						},
					}},
					Volumes: []corev1.Volume{
						{Name: "source", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: req.SourcePVC, ReadOnly: true}}},
						{Name: "dest", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: req.DestPVC, ReadOnly: destReadOnly}}},
					},
				},
			},
		},
	}
}

func (m RsyncMover) Verify(ctx context.Context, req Request) error {
	runName := uniqueName(req.JobName + "-verify")
	backoff := int32(0)
	nonRoot := req.RunAsUser >= 0
	job := buildJob(req, runName, verifyArgs(req.Args, nonRoot), nonRoot, backoff, true)
	job.Spec.Template.Spec.Containers[0].Env = append(job.Spec.Template.Spec.Containers[0].Env, corev1.EnvVar{Name: "LC_ALL", Value: "C"})
	if _, err := m.Client.BatchV1().Jobs(req.Namespace).Create(ctx, job, metav1.CreateOptions{}); err != nil {
		return fmt.Errorf("create rsync verification job: %w", err)
	}
	notifyObserver(req.Observe, Observation{JobName: runName})
	podObserver := startJobPodObserver(ctx, m.Client, req.Namespace, runName, req.PollInterval, req.Observe)
	cleaned := false
	defer func() {
		if cleaned {
			return
		}
		cleanupCtx, cancel := context.WithTimeout(context.Background(), req.WaitTimeout)
		defer cancel()
		_ = cleanupJob(cleanupCtx, m.Client, req.Namespace, runName, req.WaitTimeout, req.PollInterval, req.Observe)
	}()
	waitErr := waitForJob(ctx, m.Client, req.Namespace, runName, req.WaitTimeout, req.PollInterval, req.Observe)
	podObserver.Stop()
	if waitErr != nil {
		failureCtx, cancel := context.WithTimeout(context.Background(), req.WaitTimeout)
		defer cancel()
		logs, logErr := jobLogs(failureCtx, m.Client, m.readLogs, req.Namespace, runName)
		if logErr == nil && logs != "" {
			return fmt.Errorf("verify copied data: %w; logs: %s", waitErr, logs)
		}
		if logErr != nil {
			return fmt.Errorf("verify copied data: %w; read verification logs: %v", waitErr, logErr)
		}
		return fmt.Errorf("verify copied data: %w", waitErr)
	}
	logs, err := jobLogs(ctx, m.Client, m.readLogs, req.Namespace, runName)
	if err != nil {
		return fmt.Errorf("read verification logs: %w", err)
	}
	if err := cleanupJob(ctx, m.Client, req.Namespace, runName, req.WaitTimeout, req.PollInterval, req.Observe); err != nil {
		return err
	}
	cleaned = true
	differences, err := verificationDifferences(logs)
	if err != nil {
		return fmt.Errorf("parse verification logs: %w", err)
	}
	if differences != "" {
		return fmt.Errorf("copy verification found differences:\n%s", differences)
	}
	return nil
}

// verificationDifferences parses rsync's machine-prefixed item records and
// requires its final C-locale summary line. The summary is emitted only after
// rsync has completed its file-list comparison, so an empty log or truncated
// stream cannot be mistaken for a clean verification.
func verificationDifferences(logs string) (string, error) {
	var differences []string
	completed := false
	for _, line := range strings.Split(logs, "\n") {
		line = strings.TrimRight(line, "\r")
		if strings.HasPrefix(line, verificationRecordPrefix) {
			record := strings.TrimPrefix(line, verificationRecordPrefix)
			if record != "" && !isUnchangedVerificationRecord(record) {
				differences = append(differences, record)
			}
			continue
		}
		if isVerificationSentinel(line) {
			completed = true
		}
	}
	if !completed {
		return "", fmt.Errorf("completion sentinel not found")
	}
	return strings.Join(differences, "\n"), nil
}

func isUnchangedVerificationRecord(record string) bool {
	// The itemized code is 11 bytes. A leading '.', followed by the file type
	// and nine spaces, describes an unchanged entry that some rsync versions
	// still emit with --itemize-changes.
	if len(record) < 11 || record[0] != '.' {
		return false
	}
	for i := 2; i < 11; i++ {
		if record[i] != ' ' {
			return false
		}
	}
	return true
}

func isVerificationSentinel(line string) bool {
	if !strings.HasPrefix(line, verificationSentinelPrefix) {
		return false
	}
	parts := strings.Fields(strings.TrimPrefix(line, verificationSentinelPrefix))
	if len(parts) < 4 || parts[1] != "speedup" || parts[2] != "is" {
		return false
	}
	_, sizeErr := strconv.ParseUint(strings.ReplaceAll(parts[0], ",", ""), 10, 64)
	_, speedErr := strconv.ParseFloat(strings.ReplaceAll(parts[3], ",", ""), 64)
	return sizeErr == nil && speedErr == nil
}

func verifyArgs(copyArgs []string, nonRoot bool) []string {
	args := []string{"-aHAXniO", "--numeric-ids", "--checksum"}
	if nonRoot {
		args = []string{"-rlHtniO", "--checksum"}
	}
	args = append(args, "--exclude=lost+found", "--delete", "--itemize-changes", "--stats", "--out-format="+verificationRecordPrefix+"%i %n%L")
	for _, arg := range copyArgs {
		if isRsyncSelectionArg(arg) {
			args = append(args, arg)
		}
	}
	return append(args, "/src/", "/dest/")
}

func isRsyncSelectionArg(arg string) bool {
	for _, prefix := range []string{"--exclude=", "--include=", "--filter="} {
		if strings.HasPrefix(arg, prefix) {
			return true
		}
	}
	return arg == "--delete-excluded" || arg == "--prune-empty-dirs"
}

func rsyncArgs(extraArgs []string, nonRoot bool) []string {
	// Without root there is no way to preserve arbitrary owners, groups,
	// exact modes, devices, or privileged xattrs, so copy content, links,
	// and file times only. -p and dir times (-O) must stay off because
	// chmod/utimes fail on the volume root a non-root user does not own.
	// lost+found is fsck scratch space and root-only on ext4; never copy it.
	base := []string{"-aHAX", "--numeric-ids"}
	if nonRoot {
		base = []string{"-rlHt", "-O"}
	}
	args := append([]string(nil), base...)
	args = append(args, "--exclude=lost+found", "--delete", "--info=progress2")
	args = append(args, extraArgs...)
	return append(args, "/src/", "/dest/")
}

func uniqueName(base string) string {
	suffix := fmt.Sprintf("-%x", time.Now().UnixNano())
	base = naming.SafeDNSLabel(base)
	maxBase := 63 - len(suffix)
	if maxBase < 1 {
		maxBase = 1
	}
	if len(base) > maxBase {
		base = strings.Trim(base[:maxBase], "-")
	}
	return base + suffix
}

func cleanupJob(ctx context.Context, client kubernetes.Interface, namespace, name string, timeout, poll time.Duration, observers ...Observer) error {
	selector := "shrink-pvc-job=" + name
	if err := client.BatchV1().Jobs(namespace).Delete(ctx, name, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete rsync job: %w", err)
	}
	if err := client.CoreV1().Pods(namespace).DeleteCollection(ctx, metav1.DeleteOptions{}, metav1.ListOptions{LabelSelector: selector}); err != nil {
		return fmt.Errorf("delete rsync job pods: %w", err)
	}
	err := wait.PollUntilContextTimeout(ctx, poll, timeout, true, func(ctx context.Context) (bool, error) {
		pods, err := client.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{LabelSelector: selector})
		if err != nil {
			return false, fmt.Errorf("list rsync job pods during cleanup: %w", err)
		}
		notifyObservers(observers, Observation{JobName: name, PodCount: len(pods.Items), Cleanup: true})
		return len(pods.Items) == 0, nil
	})
	if wait.Interrupted(err) && ctx.Err() == nil {
		return fmt.Errorf("timed out waiting for rsync job pods to terminate for %s/%s", namespace, name)
	}
	return err
}

func waitForJob(ctx context.Context, client kubernetes.Interface, namespace, name string, timeout, poll time.Duration, observers ...Observer) error {
	err := wait.PollUntilContextTimeout(ctx, poll, timeout, true, func(ctx context.Context) (bool, error) {
		job, err := client.BatchV1().Jobs(namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return false, fmt.Errorf("get rsync job: %w", err)
		}
		observation := Observation{JobName: name, Active: job.Status.Active, Succeeded: job.Status.Succeeded, Failed: job.Status.Failed}
		for _, cond := range job.Status.Conditions {
			if cond.Status != corev1.ConditionTrue {
				continue
			}
			observation.JobCondition = string(cond.Type)
			observation.JobMessage = cond.Message
			notifyObservers(observers, observation)
			if cond.Type == batchv1.JobComplete {
				return true, nil
			}
			if cond.Type == batchv1.JobFailed {
				return false, fmt.Errorf("rsync job failed: %s", cond.Message)
			}
		}
		notifyObservers(observers, observation)
		return false, nil
	})
	if wait.Interrupted(err) && ctx.Err() == nil {
		return fmt.Errorf("timed out waiting for rsync job %s/%s", namespace, name)
	}
	return err
}

type jobPodObserver struct {
	cancel context.CancelFunc
	done   chan struct{}
	once   sync.Once
}

func startJobPodObserver(ctx context.Context, client kubernetes.Interface, namespace, jobName string, poll time.Duration, observer Observer) *jobPodObserver {
	observationCtx, cancel := context.WithCancel(ctx)
	result := &jobPodObserver{cancel: cancel, done: make(chan struct{})}
	if observer == nil {
		close(result.done)
		return result
	}
	if poll <= 0 {
		poll = time.Second
	}
	go func() {
		defer close(result.done)
		observe := func() {
			pods, err := client.CoreV1().Pods(namespace).List(observationCtx, metav1.ListOptions{LabelSelector: "shrink-pvc-job=" + jobName})
			if err != nil {
				return
			}
			if len(pods.Items) == 0 {
				notifyObserver(observer, Observation{JobName: jobName})
				return
			}
			for i := range pods.Items {
				pod := &pods.Items[i]
				notifyObserver(observer, Observation{
					JobName: jobName, PodName: pod.Name, PodPhase: pod.Status.Phase,
					WaitingReason: podWaitingReason(pod), PodCount: len(pods.Items),
				})
			}
		}
		observe()
		ticker := time.NewTicker(poll)
		defer ticker.Stop()
		for {
			select {
			case <-observationCtx.Done():
				return
			case <-ticker.C:
				observe()
			}
		}
	}()
	return result
}

func (o *jobPodObserver) Stop() {
	o.once.Do(func() {
		o.cancel()
		<-o.done
	})
}

type copyLogStream struct {
	ctx       context.Context
	cancel    context.CancelFunc
	client    kubernetes.Interface
	reader    podLogReader
	namespace string
	jobName   string
	observer  Observer

	mu        sync.Mutex
	running   bool
	completed bool
	stopped   bool
	stream    *onceReadCloser
	wg        sync.WaitGroup
}

func newCopyLogStream(ctx context.Context, client kubernetes.Interface, reader podLogReader, namespace, jobName string, observer Observer) *copyLogStream {
	streamCtx, cancel := context.WithCancel(ctx)
	return &copyLogStream{ctx: streamCtx, cancel: cancel, client: client, reader: reader, namespace: namespace, jobName: jobName, observer: observer}
}

func (s *copyLogStream) Start(podName string) {
	s.mu.Lock()
	if s.running || s.completed || s.stopped || s.observer == nil {
		s.mu.Unlock()
		return
	}
	s.running = true
	s.wg.Add(1)
	s.mu.Unlock()
	go s.run(podName)
}

func (s *copyLogStream) Stop() {
	s.mu.Lock()
	if !s.stopped {
		s.stopped = true
		s.cancel()
	}
	stream := s.stream
	s.mu.Unlock()
	if stream != nil {
		_ = stream.Close()
	}
	s.wg.Wait()
}

func (s *copyLogStream) run(podName string) {
	successfullyOpened := false
	var stream *onceReadCloser
	defer func() {
		s.mu.Lock()
		s.running = false
		if successfullyOpened {
			s.completed = true
		}
		if s.stream == stream {
			s.stream = nil
		}
		s.mu.Unlock()
		s.wg.Done()
	}()
	reader := s.reader
	if reader == nil {
		reader = func(ctx context.Context, namespace, pod string, options *corev1.PodLogOptions) (io.ReadCloser, error) {
			return s.client.CoreV1().Pods(namespace).GetLogs(pod, options).Stream(ctx)
		}
	}
	opened, err := reader(s.ctx, s.namespace, podName, &corev1.PodLogOptions{Container: "rsync", Follow: true})
	if err != nil {
		if s.ctx.Err() == nil {
			notifyObserver(s.observer, Observation{JobName: s.jobName, PodName: podName, StreamError: err.Error()})
		}
		return
	}
	stream = &onceReadCloser{ReadCloser: opened}
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		_ = stream.Close()
		return
	}
	s.stream = stream
	successfullyOpened = true
	s.mu.Unlock()

	parser := &logRecordParser{}
	buffer := make([]byte, 32<<10)
	lastRecord := ""
	emit := func(record string, final bool) {
		if record == "" {
			return
		}
		lastRecord = record
		notifyObserver(s.observer, Observation{JobName: s.jobName, PodName: podName, LogRecord: record, FinalRecord: final})
	}
	for {
		n, readErr := stream.Read(buffer)
		if n > 0 {
			parser.Write(buffer[:n], func(record string) { emit(record, false) })
		}
		if readErr == nil {
			continue
		}
		emittedPartial := false
		parser.Flush(func(record string) {
			emittedPartial = true
			emit(record, true)
		})
		if !emittedPartial && lastRecord != "" {
			emit(lastRecord, true)
		}
		if readErr != io.EOF && s.ctx.Err() == nil {
			notifyObserver(s.observer, Observation{JobName: s.jobName, PodName: podName, StreamError: readErr.Error()})
		}
		break
	}
	if closeErr := stream.Close(); closeErr != nil && s.ctx.Err() == nil {
		notifyObserver(s.observer, Observation{JobName: s.jobName, PodName: podName, StreamError: closeErr.Error()})
	}
}

type onceReadCloser struct {
	io.ReadCloser
	once sync.Once
	err  error
}

func (r *onceReadCloser) Close() error {
	r.once.Do(func() { r.err = r.ReadCloser.Close() })
	return r.err
}

type logRecordParser struct {
	partial    []byte
	previousCR bool
	truncated  bool
}

func (p *logRecordParser) Write(chunk []byte, emit func(string)) {
	for _, b := range chunk {
		switch b {
		case '\r':
			p.emit(emit)
			p.previousCR = true
		case '\n':
			if p.previousCR {
				p.previousCR = false
				continue
			}
			p.emit(emit)
		default:
			p.previousCR = false
			if len(p.partial) < maxLiveLogRecordBytes {
				p.partial = append(p.partial, b)
			} else {
				p.truncated = true
			}
		}
	}
}

func (p *logRecordParser) Flush(emit func(string)) {
	p.emit(emit)
	p.previousCR = false
}

func (p *logRecordParser) emit(emit func(string)) {
	if len(p.partial) == 0 && !p.truncated {
		return
	}
	record := string(p.partial)
	if p.truncated {
		record += "…"
	}
	p.partial = p.partial[:0]
	p.truncated = false
	emit(record)
}

func podWaitingReason(pod *corev1.Pod) string {
	statuses := append([]corev1.ContainerStatus(nil), pod.Status.InitContainerStatuses...)
	statuses = append(statuses, pod.Status.ContainerStatuses...)
	for _, status := range statuses {
		if status.State.Waiting != nil && status.State.Waiting.Reason != "" {
			return status.State.Waiting.Reason
		}
	}
	return ""
}

func notifyObserver(observer Observer, observation Observation) {
	if observer == nil {
		return
	}
	func() {
		defer func() { _ = recover() }()
		observer(observation)
	}()
}

func notifyObservers(observers []Observer, observation Observation) {
	for _, observer := range observers {
		notifyObserver(observer, observation)
	}
}

func jobLogs(ctx context.Context, client kubernetes.Interface, reader podLogReader, namespace, jobName string) (string, error) {
	pods, err := client.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{LabelSelector: "shrink-pvc-job=" + jobName})
	if err != nil {
		return "", fmt.Errorf("list rsync job pods: %w", err)
	}
	if len(pods.Items) == 0 {
		return "", fmt.Errorf("no rsync job pods found")
	}
	if reader == nil {
		reader = func(ctx context.Context, namespace, pod string, options *corev1.PodLogOptions) (io.ReadCloser, error) {
			return client.CoreV1().Pods(namespace).GetLogs(pod, options).Stream(ctx)
		}
	}
	var logs strings.Builder
	for _, pod := range pods.Items {
		stream, err := reader(ctx, namespace, pod.Name, &corev1.PodLogOptions{Container: "rsync"})
		if err != nil {
			return "", fmt.Errorf("open logs for pod %s: %w", pod.Name, err)
		}
		remaining := int64(maxJobLogBytes - logs.Len())
		if remaining <= 0 {
			_ = stream.Close()
			return "", fmt.Errorf("rsync job logs exceed %d bytes", maxJobLogBytes)
		}
		b, readErr := io.ReadAll(io.LimitReader(stream, remaining+1))
		closeErr := stream.Close()
		if readErr != nil {
			return "", fmt.Errorf("read logs for pod %s: %w", pod.Name, readErr)
		}
		if closeErr != nil {
			return "", fmt.Errorf("close logs for pod %s: %w", pod.Name, closeErr)
		}
		if int64(len(b)) > remaining {
			return "", fmt.Errorf("rsync job logs exceed %d bytes", maxJobLogBytes)
		}
		if logs.Len() > 0 && len(b) > 0 {
			logs.WriteByte('\n')
		}
		logs.Write(b)
	}
	return strings.TrimSpace(logs.String()), nil
}
