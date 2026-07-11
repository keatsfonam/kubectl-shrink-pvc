package datamover

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/keatsfonam/kubectl-shrink-pvc/internal/naming"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

const DefaultImage = "instrumentisto/rsync-ssh:alpine3.23-r3@sha256:6cbad37c2fbdca4ac7ad9d1c1bb8990af9efd4dc76321b349935876cbb1e9e4a"

type Request struct {
	Namespace    string
	SourcePVC    string
	DestPVC      string
	Image        string
	JobName      string
	ExtraArgs    string
	WaitTimeout  time.Duration
	PollInterval time.Duration
}

type DataMover interface {
	Move(ctx context.Context, req Request) error
}

type RsyncMover struct {
	Client kubernetes.Interface
}

func (m RsyncMover) Move(ctx context.Context, req Request) error {
	if req.Image == "" {
		req.Image = DefaultImage
	}
	if req.JobName == "" {
		req.JobName = "shrink-copy-" + req.SourcePVC + "-to-" + req.DestPVC
	}
	runName := uniqueName(req.JobName)
	if req.PollInterval == 0 {
		req.PollInterval = 2 * time.Second
	}
	if req.WaitTimeout == 0 {
		req.WaitTimeout = 10 * time.Minute
	}

	backoff := int32(0)
	cmd := rsyncCommand(req.ExtraArgs)

	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: runName, Namespace: req.Namespace},
		Spec: batchv1.JobSpec{
			BackoffLimit: &backoff,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app.kubernetes.io/name": "kubectl-shrink-pvc", "shrink-pvc-job": runName}},
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					Containers: []corev1.Container{{
						Name:    "rsync",
						Image:   req.Image,
						Command: []string{"/bin/sh", "-c", cmd},
						VolumeMounts: []corev1.VolumeMount{
							{Name: "source", MountPath: "/src", ReadOnly: true},
							{Name: "dest", MountPath: "/dest"},
						},
					}},
					Volumes: []corev1.Volume{
						{Name: "source", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: req.SourcePVC, ReadOnly: true}}},
						{Name: "dest", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: req.DestPVC}}},
					},
				},
			},
		},
	}

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

func rsyncCommand(extraArgs string) string {
	cmd := "rsync -aHAX --numeric-ids --delete --info=progress2 " + extraArgs + " /src/ /dest/"
	return strings.Join(strings.Fields(cmd), " ")
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
	deadline := time.Now().Add(timeout)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		pods, err := client.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{LabelSelector: selector})
		if err != nil {
			return fmt.Errorf("list rsync job pods during cleanup: %w", err)
		}
		if len(pods.Items) == 0 {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timed out waiting for rsync job pods to terminate for %s/%s", namespace, name)
		}
		time.Sleep(poll)
	}
}

func waitForJob(ctx context.Context, client kubernetes.Interface, namespace, name string, timeout, poll time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		job, err := client.BatchV1().Jobs(namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return fmt.Errorf("get rsync job: %w", err)
		}
		for _, cond := range job.Status.Conditions {
			if cond.Type == batchv1.JobComplete && cond.Status == corev1.ConditionTrue {
				return nil
			}
			if cond.Type == batchv1.JobFailed && cond.Status == corev1.ConditionTrue {
				return fmt.Errorf("rsync job failed: %s", cond.Message)
			}
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timed out waiting for rsync job %s/%s", namespace, name)
		}
		time.Sleep(poll)
	}
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
