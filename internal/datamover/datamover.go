package datamover

import (
	"context"
	"fmt"
	"io"
	"strings"
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
}

type RsyncMover struct {
	Client kubernetes.Interface
}

func (m RsyncMover) Move(ctx context.Context, req Request) error {
	runName := uniqueName(req.JobName)
	backoff := int32(0)
	nonRoot := req.RunAsUser >= 0
	args := rsyncArgs(req.Args, nonRoot)

	job := buildJob(req, runName, args, nonRoot, backoff, false)

	if _, err := m.Client.BatchV1().Jobs(req.Namespace).Create(ctx, job, metav1.CreateOptions{}); err != nil {
		return fmt.Errorf("create rsync job: %w", err)
	}

	if err := waitForJob(ctx, m.Client, req.Namespace, runName, req.WaitTimeout, req.PollInterval); err != nil {
		// Fetch logs and clean up on a fresh context so a cancelled caller
		// (Ctrl-C) does not leave the failed job behind, but bound it so a
		// broken API server cannot hang the exit path.
		cleanupCtx, cancel := context.WithTimeout(context.Background(), req.WaitTimeout)
		defer cancel()
		logs, _ := jobLogs(cleanupCtx, m.Client, req.Namespace, runName)
		_ = cleanupJob(cleanupCtx, m.Client, req.Namespace, runName, req.WaitTimeout, req.PollInterval)
		if logs != "" {
			return fmt.Errorf("%w; logs: %s", err, logs)
		}
		return err
	}
	if err := cleanupJob(ctx, m.Client, req.Namespace, runName, req.WaitTimeout, req.PollInterval); err != nil {
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
	if _, err := m.Client.BatchV1().Jobs(req.Namespace).Create(ctx, job, metav1.CreateOptions{}); err != nil {
		return fmt.Errorf("create rsync verification job: %w", err)
	}
	cleaned := false
	defer func() {
		if cleaned {
			return
		}
		cleanupCtx, cancel := context.WithTimeout(context.Background(), req.WaitTimeout)
		defer cancel()
		_ = cleanupJob(cleanupCtx, m.Client, req.Namespace, runName, req.WaitTimeout, req.PollInterval)
	}()
	if err := waitForJob(ctx, m.Client, req.Namespace, runName, req.WaitTimeout, req.PollInterval); err != nil {
		return fmt.Errorf("verify copied data: %w", err)
	}
	logs, err := jobLogs(ctx, m.Client, req.Namespace, runName)
	if err != nil {
		return fmt.Errorf("read verification logs: %w", err)
	}
	if err := cleanupJob(ctx, m.Client, req.Namespace, runName, req.WaitTimeout, req.PollInterval); err != nil {
		return err
	}
	cleaned = true
	if differences := verificationDifferences(logs); differences != "" {
		return fmt.Errorf("copy verification found differences:\n%s", differences)
	}
	return nil
}

func verificationDifferences(logs string) string {
	var differences []string
	for _, line := range strings.Split(logs, "\n") {
		line = strings.TrimRight(line, "\r")
		if strings.TrimSpace(line) == "" {
			continue
		}
		// Rsync can emit unchanged entries such as ".f          path" when
		// checksum/itemize mode is active. The itemized code is 11 bytes: a
		// leading '.', a file-type byte, and nine unchanged attribute slots.
		if len(line) >= 11 && line[0] == '.' {
			unchanged := true
			for i := 2; i < 11; i++ {
				if line[i] != ' ' {
					unchanged = false
					break
				}
			}
			if unchanged {
				continue
			}
		}
		differences = append(differences, line)
	}
	return strings.Join(differences, "\n")
}

func verifyArgs(copyArgs []string, nonRoot bool) []string {
	args := []string{"-aHAXniO", "--numeric-ids", "--checksum"}
	if nonRoot {
		args = []string{"-rlHtniO", "--checksum"}
	}
	args = append(args, "--exclude=lost+found", "--delete", "--itemize-changes")
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

func cleanupJob(ctx context.Context, client kubernetes.Interface, namespace, name string, timeout, poll time.Duration) error {
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
		return len(pods.Items) == 0, nil
	})
	if wait.Interrupted(err) && ctx.Err() == nil {
		return fmt.Errorf("timed out waiting for rsync job pods to terminate for %s/%s", namespace, name)
	}
	return err
}

func waitForJob(ctx context.Context, client kubernetes.Interface, namespace, name string, timeout, poll time.Duration) error {
	err := wait.PollUntilContextTimeout(ctx, poll, timeout, true, func(ctx context.Context) (bool, error) {
		job, err := client.BatchV1().Jobs(namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return false, fmt.Errorf("get rsync job: %w", err)
		}
		for _, cond := range job.Status.Conditions {
			if cond.Type == batchv1.JobComplete && cond.Status == corev1.ConditionTrue {
				return true, nil
			}
			if cond.Type == batchv1.JobFailed && cond.Status == corev1.ConditionTrue {
				return false, fmt.Errorf("rsync job failed: %s", cond.Message)
			}
		}
		return false, nil
	})
	if wait.Interrupted(err) && ctx.Err() == nil {
		return fmt.Errorf("timed out waiting for rsync job %s/%s", namespace, name)
	}
	return err
}

func jobLogs(ctx context.Context, client kubernetes.Interface, namespace, jobName string) (string, error) {
	pods, err := client.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{LabelSelector: "shrink-pvc-job=" + jobName})
	if err != nil {
		return "", err
	}
	var parts []string
	for _, pod := range pods.Items {
		stream, err := client.CoreV1().Pods(namespace).GetLogs(pod.Name, &corev1.PodLogOptions{}).Stream(ctx)
		if err != nil {
			continue
		}
		b, _ := io.ReadAll(stream)
		_ = stream.Close()
		if len(b) > 0 {
			parts = append(parts, string(b))
		}
	}
	return strings.TrimSpace(strings.Join(parts, "\n")), nil
}
