package workflow

import (
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"

	corev1 "k8s.io/api/core/v1"

	"github.com/keatsfonam/kubectl-shrink-pvc/internal/datamover"
	"github.com/keatsfonam/kubectl-shrink-pvc/internal/inspect"
	"github.com/keatsfonam/kubectl-shrink-pvc/internal/kube"
	liveprogress "github.com/keatsfonam/kubectl-shrink-pvc/internal/progress"
)

func setProgressPhase(cfg Config, phase liveprogress.Phase, activity string) {
	if cfg.reporter != nil {
		cfg.reporter.SetPhase(phase, activity)
	}
}

func progressActivity(cfg Config, activity string) {
	if cfg.reporter != nil {
		cfg.reporter.Activity(activity)
	}
}

func durableOutput(cfg Config, writer io.Writer, text string) {
	if cfg.reporter != nil {
		cfg.reporter.Output(writer, text)
		return
	}
	if writer != nil {
		_, _ = io.WriteString(writer, text)
	}
}

func durableOutputf(cfg Config, writer io.Writer, format string, args ...any) {
	durableOutput(cfg, writer, fmt.Sprintf(format, args...))
}

func pvcUnmountObserver(cfg Config, namespace, pvcName string) kube.PVCUnmountObserver {
	return func(observation kube.PVCUnmountObservation) {
		if len(observation.ActivePods) == 0 {
			setProgressPhase(cfg, liveprogress.QuiesceWaitingForUnmount, fmt.Sprintf("PVC %s/%s has no active consumer Pods", namespace, pvcName))
			return
		}
		setProgressPhase(cfg, liveprogress.QuiesceWaitingForUnmount, fmt.Sprintf("PVC %s/%s is still used by %d Pod(s): %s", namespace, pvcName, len(observation.ActivePods), strings.Join(observation.ActivePods, ", ")))
	}
}

func pvcDeletionObserver(cfg Config, namespace, pvcName string) kube.PVCDeletionObserver {
	return func(observation kube.PVCDeletionObservation) {
		if !observation.Exists {
			setProgressPhase(cfg, liveprogress.ReplaceSourceDeleting, fmt.Sprintf("PVC %s/%s no longer exists", namespace, pvcName))
			return
		}
		phase := string(observation.Phase)
		if phase == "" {
			phase = "unknown"
		}
		setProgressPhase(cfg, liveprogress.ReplaceSourceDeleting, fmt.Sprintf("PVC %s/%s still exists uid=%s phase=%s", namespace, pvcName, observation.UID, phase))
	}
}

func inspectionObserver(cfg Config) inspect.Observer {
	return func(observation inspect.Observation) {
		if observation.Cleanup {
			if observation.Exists {
				setProgressPhase(cfg, liveprogress.CleanupJobsPods, fmt.Sprintf("inspection Pod %s still exists phase=%s", observation.PodName, observedPodPhase(observation.PodPhase)))
			} else {
				setProgressPhase(cfg, liveprogress.CleanupJobsPods, fmt.Sprintf("inspection Pod %s removed", observation.PodName))
			}
			return
		}
		activity := fmt.Sprintf("inspection Pod %s phase=%s", observation.PodName, observedPodPhase(observation.PodPhase))
		if observation.WaitingReason != "" {
			activity += " waiting=" + observation.WaitingReason
			setProgressPhase(cfg, liveprogress.InspectMounting, activity)
			return
		}
		switch observation.PodPhase {
		case corev1.PodRunning, corev1.PodSucceeded, corev1.PodFailed:
			setProgressPhase(cfg, liveprogress.InspectScanning, activity)
		default:
			setProgressPhase(cfg, liveprogress.InspectScheduling, activity)
		}
	}
}

type moverProgressSpec struct {
	scheduling        liveprogress.Phase
	mounting          liveprogress.Phase
	running           liveprogress.Phase
	preparationMount  liveprogress.Phase
	copyLabel         string
	verificationLabel string
}

func moverObserver(cfg Config, spec moverProgressSpec) datamover.Observer {
	var activityMu sync.Mutex
	transferring := false
	return func(observation datamover.Observation) {
		if observation.Cleanup {
			setProgressPhase(cfg, liveprogress.CleanupJobsPods, fmt.Sprintf("rsync Job %s cleanup has %d Pod(s) remaining", observation.JobName, observation.PodCount))
			return
		}
		if observation.LogRecord != "" {
			activity := copyActivity(spec.copyLabel, observation.LogRecord)
			activityMu.Lock()
			firstRecord := !transferring
			transferring = true
			activityMu.Unlock()
			if firstRecord {
				setProgressPhase(cfg, spec.running, activity)
				if cfg.reporter != nil {
					cfg.reporter.HighFrequencyActivity(activity)
				}
			} else if cfg.reporter != nil {
				if observation.FinalRecord {
					cfg.reporter.FinalActivity(activity)
				} else {
					cfg.reporter.HighFrequencyActivity(activity)
				}
			}
			return
		}
		if observation.StreamError != "" {
			progressActivity(cfg, fmt.Sprintf("%s; live rsync log unavailable for Pod %s; continuing with Kubernetes Job and Pod activity", spec.copyLabel, observation.PodName))
			return
		}
		if observation.PodName != "" {
			label := spec.copyLabel
			if label == "" {
				label = spec.verificationLabel
			}
			activity := fmt.Sprintf("%s; rsync Pod %s phase=%s", label, observation.PodName, observedPodPhase(observation.PodPhase))
			if observation.WaitingReason != "" {
				activity += " waiting=" + observation.WaitingReason
				if spec.preparationMount != "" {
					setProgressPhase(cfg, spec.preparationMount, activity)
				}
				setProgressPhase(cfg, spec.mounting, activity)
				return
			}
			switch observation.PodPhase {
			case corev1.PodRunning, corev1.PodSucceeded, corev1.PodFailed:
				setProgressPhase(cfg, spec.running, activity)
			default:
				setProgressPhase(cfg, spec.scheduling, activity)
			}
			return
		}
		if observation.JobCondition != "" {
			label := spec.copyLabel
			if label == "" {
				label = spec.verificationLabel
			}
			activity := fmt.Sprintf("%s; rsync Job %s condition=%s", label, observation.JobName, observation.JobCondition)
			if observation.JobMessage != "" {
				activity += " message=" + observation.JobMessage
			}
			setProgressPhase(cfg, spec.running, activity)
			return
		}
		if observation.JobName != "" {
			label := spec.copyLabel
			if label == "" {
				label = spec.verificationLabel
			}
			setProgressPhase(cfg, spec.scheduling, fmt.Sprintf("rsync Job %s scheduled for %s; active=%d succeeded=%d failed=%d", observation.JobName, label, observation.Active, observation.Succeeded, observation.Failed))
		}
	}
}

func copyActivity(label, record string) string {
	record = strings.TrimSpace(record)
	if percentage := rsyncPercentage(record); percentage != "" {
		return fmt.Sprintf("%s; per-copy progress=%s; rsync: %s", label, percentage, record)
	}
	return fmt.Sprintf("%s; rsync: %s", label, record)
}

func rsyncPercentage(record string) string {
	for _, field := range strings.Fields(record) {
		candidate := strings.Trim(field, "(),[]")
		if !strings.HasSuffix(candidate, "%") {
			continue
		}
		number := strings.TrimSuffix(candidate, "%")
		if number == "" {
			continue
		}
		if _, err := strconv.ParseFloat(number, 64); err == nil {
			return candidate
		}
	}
	return ""
}

func observedPodPhase(phase corev1.PodPhase) string {
	if phase == "" {
		return "unknown"
	}
	return string(phase)
}
