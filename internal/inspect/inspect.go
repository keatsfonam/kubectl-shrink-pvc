package inspect

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/keatsfonam/kubectl-shrink-pvc/internal/naming"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

type Options struct {
	Namespace    string
	PVCName      string
	Image        string
	PodName      string
	WaitTimeout  time.Duration
	PollInterval time.Duration
}

func UsageBytes(ctx context.Context, client kubernetes.Interface, opts Options) (int64, error) {
	if opts.Image == "" {
		opts.Image = "alpine:3.20"
	}
	generateName := ""
	if opts.PodName == "" {
		generateName = naming.SafeDNSLabel(opts.PVCName+"-shrink-inspect") + "-"
	} else {
		opts.PodName = naming.SafeDNSLabel(opts.PodName)
	}
	if opts.PollInterval == 0 {
		opts.PollInterval = 2 * time.Second
	}
	if opts.WaitTimeout == 0 {
		opts.WaitTimeout = 10 * time.Minute
	}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: opts.PodName, GenerateName: generateName, Namespace: opts.Namespace},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyNever,
			Containers: []corev1.Container{{
				Name:    "inspect",
				Image:   opts.Image,
				Command: []string{"/bin/sh", "-c", "du -sk /data | awk '{print $1 * 1024}'"},
				VolumeMounts: []corev1.VolumeMount{{
					Name:      "source",
					MountPath: "/data",
					ReadOnly:  true,
				}},
			}},
			Volumes: []corev1.Volume{{
				Name: "source",
				VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
					ClaimName: opts.PVCName,
					ReadOnly:  true,
				}},
			}},
		},
	}

	created, err := client.CoreV1().Pods(opts.Namespace).Create(ctx, pod, metav1.CreateOptions{})
	if err != nil {
		return 0, fmt.Errorf("create inspection pod: %w", err)
	}
	defer func() {
		_ = cleanupPod(context.Background(), client, opts.Namespace, created.Name, opts.WaitTimeout, opts.PollInterval)
	}()

	if err := waitForPodCompletion(ctx, client, opts.Namespace, created.Name, opts.WaitTimeout, opts.PollInterval); err != nil {
		logs, _ := podLogs(context.Background(), client, opts.Namespace, created.Name)
		if logs != "" {
			return 0, fmt.Errorf("%w; logs: %s", err, logs)
		}
		return 0, err
	}

	logs, err := podLogs(ctx, client, opts.Namespace, created.Name)
	if err != nil {
		return 0, err
	}
	return ParseUsageBytes(logs)
}

func ParseUsageBytes(logs string) (int64, error) {
	fields := strings.Fields(strings.TrimSpace(logs))
	if len(fields) == 0 {
		return 0, fmt.Errorf("inspection produced no usage output")
	}
	value, err := strconv.ParseInt(fields[len(fields)-1], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse inspection usage %q: %w", fields[len(fields)-1], err)
	}
	if value < 0 {
		return 0, fmt.Errorf("inspection usage must not be negative: %d", value)
	}
	return value, nil
}

func cleanupPod(ctx context.Context, client kubernetes.Interface, namespace, name string, timeout, poll time.Duration) error {
	if err := client.CoreV1().Pods(namespace).Delete(ctx, name, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete inspection pod: %w", err)
	}
	deadline := time.Now().Add(timeout)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		_, err := client.CoreV1().Pods(namespace).Get(ctx, name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("get inspection pod during cleanup: %w", err)
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timed out waiting for inspection pod %s/%s cleanup", namespace, name)
		}
		time.Sleep(poll)
	}
}

func waitForPodCompletion(ctx context.Context, client kubernetes.Interface, namespace, name string, timeout, poll time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		pod, err := client.CoreV1().Pods(namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return fmt.Errorf("get inspection pod: %w", err)
		}
		switch pod.Status.Phase {
		case corev1.PodSucceeded:
			return nil
		case corev1.PodFailed:
			return fmt.Errorf("inspection pod failed")
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timed out waiting for inspection pod %s/%s", namespace, name)
		}
		time.Sleep(poll)
	}
}

func podLogs(ctx context.Context, client kubernetes.Interface, namespace, name string) (string, error) {
	stream, err := client.CoreV1().Pods(namespace).GetLogs(name, &corev1.PodLogOptions{}).Stream(ctx)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return "", fmt.Errorf("inspection pod logs not found: %w", err)
		}
		return "", fmt.Errorf("get inspection pod logs: %w", err)
	}
	defer stream.Close()
	b, err := io.ReadAll(stream)
	if err != nil {
		return "", fmt.Errorf("read inspection pod logs: %w", err)
	}
	return strings.TrimSpace(string(b)), nil
}
