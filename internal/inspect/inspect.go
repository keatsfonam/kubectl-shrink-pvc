package inspect

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/keatsfonam/kubectl-shrink-pvc/internal/naming"
	"github.com/keatsfonam/kubectl-shrink-pvc/internal/podsec"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/utils/ptr"
)

type Observation struct {
	PodName       string
	PodPhase      corev1.PodPhase
	WaitingReason string
	Cleanup       bool
	Exists        bool
}

type Observer func(Observation)

type Options struct {
	Namespace    string
	PVCName      string
	Image        string
	PodName      string
	RunAsUser    int64
	FSGroup      int64
	WaitTimeout  time.Duration
	PollInterval time.Duration
	Observe      Observer
}

func UsageBytes(ctx context.Context, client kubernetes.Interface, opts Options) (int64, error) {
	pod := buildInspectionPod(opts)

	created, err := client.CoreV1().Pods(opts.Namespace).Create(ctx, pod, metav1.CreateOptions{})
	if err != nil {
		return 0, fmt.Errorf("create inspection pod: %w", err)
	}
	notifyObserver(opts.Observe, Observation{PodName: created.Name, PodPhase: created.Status.Phase, Exists: true})
	defer func() {
		_ = cleanupPod(context.Background(), client, opts.Namespace, created.Name, opts.WaitTimeout, opts.PollInterval, opts.Observe)
	}()

	if err := waitForPodCompletion(ctx, client, opts.Namespace, created.Name, opts.WaitTimeout, opts.PollInterval, opts.Observe); err != nil {
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

func buildInspectionPod(opts Options) *corev1.Pod {
	generateName := ""
	if opts.PodName == "" {
		generateName = naming.SafeDNSLabel(opts.PVCName+"-shrink-inspect") + "-"
	} else {
		opts.PodName = naming.SafeDNSLabel(opts.PodName)
	}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: opts.PodName, GenerateName: generateName, Namespace: opts.Namespace},
		Spec: corev1.PodSpec{
			RestartPolicy:                corev1.RestartPolicyNever,
			AutomountServiceAccountToken: ptr.To(false),
			SecurityContext:              podsec.Pod(opts.RunAsUser, opts.FSGroup),
			Containers: []corev1.Container{{
				Name:    "inspect",
				Image:   opts.Image,
				Command: []string{"/bin/sh", "-c", "du -sk /data | awk '{print $1 * 1024}'"},
				// du has to stat every file regardless of owner. DAC_OVERRIDE
				// rather than DAC_READ_SEARCH: only the former is on the
				// baseline PodSecurity allowlist, and the volume is mounted
				// read-only here anyway.
				SecurityContext: podsec.Container(opts.RunAsUser >= 0, "DAC_OVERRIDE"),
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

func cleanupPod(ctx context.Context, client kubernetes.Interface, namespace, name string, timeout, poll time.Duration, observers ...Observer) error {
	if err := client.CoreV1().Pods(namespace).Delete(ctx, name, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete inspection pod: %w", err)
	}
	err := wait.PollUntilContextTimeout(ctx, poll, timeout, true, func(ctx context.Context) (bool, error) {
		pod, err := client.CoreV1().Pods(namespace).Get(ctx, name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			notifyObservers(observers, Observation{PodName: name, Cleanup: true})
			return true, nil
		}
		if err != nil {
			return false, fmt.Errorf("get inspection pod during cleanup: %w", err)
		}
		notifyObservers(observers, Observation{PodName: name, PodPhase: pod.Status.Phase, Cleanup: true, Exists: true})
		return false, nil
	})
	if wait.Interrupted(err) && ctx.Err() == nil {
		return fmt.Errorf("timed out waiting for inspection pod %s/%s cleanup", namespace, name)
	}
	return err
}

func waitForPodCompletion(ctx context.Context, client kubernetes.Interface, namespace, name string, timeout, poll time.Duration, observers ...Observer) error {
	err := wait.PollUntilContextTimeout(ctx, poll, timeout, true, func(ctx context.Context) (bool, error) {
		pod, err := client.CoreV1().Pods(namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return false, fmt.Errorf("get inspection pod: %w", err)
		}
		notifyObservers(observers, Observation{PodName: name, PodPhase: pod.Status.Phase, WaitingReason: podWaitingReason(pod), Exists: true})
		switch pod.Status.Phase {
		case corev1.PodSucceeded:
			return true, nil
		case corev1.PodFailed:
			return false, fmt.Errorf("inspection pod failed")
		}
		return false, nil
	})
	if wait.Interrupted(err) && ctx.Err() == nil {
		return fmt.Errorf("timed out waiting for inspection pod %s/%s", namespace, name)
	}
	return err
}

func podWaitingReason(pod *corev1.Pod) string {
	for _, status := range append(append([]corev1.ContainerStatus(nil), pod.Status.InitContainerStatuses...), pod.Status.ContainerStatuses...) {
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

func podLogs(ctx context.Context, client kubernetes.Interface, namespace, name string) (string, error) {
	stream, err := client.CoreV1().Pods(namespace).GetLogs(name, &corev1.PodLogOptions{}).Stream(ctx)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return "", fmt.Errorf("inspection pod logs not found: %w", err)
		}
		return "", fmt.Errorf("get inspection pod logs: %w", err)
	}
	defer func() { _ = stream.Close() }()
	b, err := io.ReadAll(stream)
	if err != nil {
		return "", fmt.Errorf("read inspection pod logs: %w", err)
	}
	return strings.TrimSpace(string(b)), nil
}
