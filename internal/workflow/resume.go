package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/keatsfonam/kubectl-shrink-pvc/internal/datamover"
	"github.com/keatsfonam/kubectl-shrink-pvc/internal/kube"
	"github.com/keatsfonam/kubectl-shrink-pvc/internal/naming"
	"github.com/keatsfonam/kubectl-shrink-pvc/internal/operation"
	liveprogress "github.com/keatsfonam/kubectl-shrink-pvc/internal/progress"
)

func resume(ctx context.Context, cfg Config, client kubernetes.Interface, namespace string, requestedTarget resource.Quantity) (retErr error) {
	if cfg.reporter == nil {
		cfg.reporter = liveprogress.New(cfg.IOStreams.Out, cfg.Quiet)
		defer cfg.reporter.Close()
	}
	store, err := operation.StoreForPVC(client, namespace, cfg.PVCName).Resolve(ctx)
	if err != nil {
		return err
	}
	state, stateCM, err := store.Load(ctx)
	if err != nil {
		return err
	}
	if state.SourceName != cfg.PVCName {
		return fmt.Errorf("operation state belongs to PVC %s, not %s", state.SourceName, cfg.PVCName)
	}
	persistedTarget, err := resource.ParseQuantity(state.TargetSize)
	if err != nil || persistedTarget.Cmp(requestedTarget) != 0 {
		return fmt.Errorf("--size must match persisted operation target %s", state.TargetSize)
	}
	if cfg.TempName != "" && cfg.TempName != state.TempName {
		return fmt.Errorf("--temp-name must match persisted operation value %s", state.TempName)
	}
	if cfg.Image != state.Image || !slices.Equal(cfg.RsyncArgs, state.RsyncArgs) || cfg.RunAsUser != state.RunAsUser || cfg.FSGroup != state.FSGroup {
		return fmt.Errorf("image, rsync arguments, and run-as settings must match the persisted operation")
	}

	cfg.TempName = state.TempName
	cfg.KeepTemp = state.KeepTemp
	cfg.NoScale = state.NoScale
	cfg.FSGroup = state.FSGroup
	var finalPVC corev1.PersistentVolumeClaim
	if err := json.Unmarshal(state.FinalPVCJSON, &finalPVC); err != nil {
		return fmt.Errorf("decode persisted replacement PVC: %w", err)
	}
	if err := operation.ValidateRecreatedPVC(&finalPVC, state.OperationID); err != nil {
		return fmt.Errorf("validate persisted replacement PVC: %w", err)
	}

	resourceVersion := stateCM.ResourceVersion
	updatePhase := func(phase operation.Phase) error {
		state.Phase = phase
		var updateErr error
		resourceVersion, updateErr = store.Update(ctx, state, resourceVersion)
		return updateErr
	}
	mover := cfg.mover
	if mover == nil {
		mover = datamover.RsyncMover{Client: client}
	}
	scaled := false
	restoreOnExit := true
	defer func() {
		if !scaled || !restoreOnExit {
			return
		}
		setProgressPhase(cfg, liveprogress.RestoreControllers, fmt.Sprintf("restoring %d Deployment replica target(s) while resuming", len(state.Deployments)))
		restoreCtx, cancel := context.WithTimeout(context.Background(), cfg.Timeout)
		defer cancel()
		if err := kube.RestoreDeployments(restoreCtx, client, state.Deployments); err != nil {
			retErr = errors.Join(retErr, fmt.Errorf("restore Deployment replicas while resuming: %w", err))
			return
		}
		setProgressPhase(cfg, liveprogress.RestoreObservingConsumers, "Deployment replica targets restored; application readiness is not used as a workflow gate")
	}()

	if state.Phase == operation.PhasePrepared {
		// No destructive source action is possible in Prepared. Restore the
		// original UID-bound replica counts and remove only temp data demonstrably
		// owned by this operation, then retire the checkpoint so a fresh run can
		// repeat inspection and copy verification.
		source, getErr := client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, state.SourceName, metav1.GetOptions{})
		if getErr != nil {
			return fmt.Errorf("inspect intact source while recovering prepared operation: %w", getErr)
		}
		if source.UID != state.OriginalSourceUID {
			return fmt.Errorf("source PVC %s/%s was replaced; refusing prepared-state recovery", namespace, state.SourceName)
		}
		if !state.NoScale && len(state.Deployments) > 0 {
			setProgressPhase(cfg, liveprogress.RestoreControllers, fmt.Sprintf("restoring %d Deployment replica target(s) from prepared recovery state", len(state.Deployments)))
			if err := kube.RestoreDeployments(ctx, client, state.Deployments); err != nil {
				return fmt.Errorf("restore Deployment replicas from prepared state: %w", err)
			}
			setProgressPhase(cfg, liveprogress.RestoreObservingConsumers, "Deployment replica targets restored; application readiness is not used as a recovery gate")
		}
		temp, getErr := client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, state.TempName, metav1.GetOptions{})
		if getErr == nil {
			if temp.Annotations[operation.AnnotationOperationID] != state.OperationID ||
				temp.Annotations[tempSourceUIDAnnotation] != string(state.OriginalSourceUID) ||
				temp.Annotations[tempSourceNameAnnotation] != state.SourceName {
				return fmt.Errorf("temporary PVC %s/%s is not owned by prepared operation", namespace, state.TempName)
			}
			setProgressPhase(cfg, liveprogress.CleanupTempPVC, fmt.Sprintf("deleting prepared-state temporary PVC %s/%s with expected uid=%s", namespace, state.TempName, temp.UID))
			if err := kube.DeletePVC(ctx, client, namespace, state.TempName, temp.UID); err != nil {
				return err
			}
			progressActivity(cfg, fmt.Sprintf("temporary PVC %s/%s delete request accepted", namespace, state.TempName))
		} else if !apierrors.IsNotFound(getErr) {
			return fmt.Errorf("inspect temporary PVC during prepared-state recovery: %w", getErr)
		}
		setProgressPhase(cfg, liveprogress.CleanupCheckpoint, fmt.Sprintf("deleting prepared recovery checkpoint for PVC %s/%s", namespace, state.SourceName))
		if err := store.Delete(ctx, stateCM.UID); err != nil {
			return err
		}
		progressActivity(cfg, fmt.Sprintf("prepared recovery checkpoint for PVC %s/%s removed", namespace, state.SourceName))
		durableOutput(cfg, cfg.IOStreams.Out, "Recovered pre-copy operation state and restored Deployment replicas; rerun the shrink command to start a fresh verified copy.\n")
		return nil
	}

	if state.Phase == operation.PhaseCopiedToTemp {
		// Recovery data must be present and owned before touching an intact source.
		if _, err := loadOwnedTempPVC(ctx, client, state); err != nil {
			return err
		}
		source, getErr := client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, state.SourceName, metav1.GetOptions{})
		switch {
		case apierrors.IsNotFound(getErr):
			// The process may have exited after deletion but before checkpointing.
			setProgressPhase(cfg, liveprogress.ReplaceSourceDeleting, fmt.Sprintf("original PVC %s/%s is already absent", namespace, state.SourceName))
			restoreOnExit = false
			if err := updatePhase(operation.PhaseSourceDeleted); err != nil {
				return err
			}
		case getErr != nil:
			return fmt.Errorf("inspect source PVC while resuming: %w", getErr)
		case source.UID != state.OriginalSourceUID:
			return fmt.Errorf("source PVC %s/%s was replaced by an unowned object; refusing to resume", namespace, state.SourceName)
		default:
			if !state.NoScale && len(state.Deployments) > 0 {
				setProgressPhase(cfg, liveprogress.QuiesceScalingConsumers, fmt.Sprintf("requesting zero replicas for %d Deployment(s) while resuming", len(state.Deployments)))
				scaled = true // ensure a final restoration attempt after any partial scale failure
				if err := kube.ScaleDeployments(ctx, client, state.Deployments, 0); err != nil {
					return err
				}
				progressActivity(cfg, fmt.Sprintf("zero-replica scale requests accepted for %d Deployment(s)", len(state.Deployments)))
			}
			setProgressPhase(cfg, liveprogress.QuiesceWaitingForUnmount, fmt.Sprintf("observing active Pods that reference PVC %s/%s", namespace, state.SourceName))
			if err := kube.WaitForPVCUnmounted(ctx, client, namespace, state.SourceName, cfg.Timeout, pollInterval, pvcUnmountObserver(cfg, namespace, state.SourceName)); err != nil {
				return err
			}
			if err := validateDestructiveBoundary(ctx, client, namespace, state.SourceName, state.OriginalSourceUID, state.Deployments, state.NoScale); err != nil {
				return err
			}
			restoreOnExit = false
			if err := updatePhase(operation.PhaseSourceDeleteRequested); err != nil {
				return err
			}
			setProgressPhase(cfg, liveprogress.ReplaceSourceDeleting, fmt.Sprintf("deleting original PVC %s/%s with expected uid=%s while resuming", namespace, state.SourceName, state.OriginalSourceUID))
			if err := kube.DeletePVC(ctx, client, namespace, state.SourceName, state.OriginalSourceUID); err != nil {
				return err
			}
		}
	}

	if state.Phase == operation.PhaseSourceDeleteRequested || state.Phase == operation.PhaseSourceDeleteAccepted {
		if _, err := loadOwnedTempPVC(ctx, client, state); err != nil {
			return err
		}
		restoreOnExit = false
		setProgressPhase(cfg, liveprogress.ReplaceSourceDeleting, fmt.Sprintf("observing deletion of original PVC %s/%s with expected uid=%s", namespace, state.SourceName, state.OriginalSourceUID))
		if err := validateWorkloadsQuiesced(ctx, client, namespace, state.SourceName, state.Deployments, state.NoScale); err != nil {
			return fmt.Errorf("revalidate workloads after delete request: %w", err)
		}
		source, getErr := client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, state.SourceName, metav1.GetOptions{})
		switch {
		case apierrors.IsNotFound(getErr):
			// The prior request succeeded even if its response was lost.
			progressActivity(cfg, fmt.Sprintf("original PVC %s/%s is already absent", namespace, state.SourceName))
		case getErr != nil:
			return fmt.Errorf("inspect source after delete request: %w", getErr)
		case source.UID != state.OriginalSourceUID:
			return fmt.Errorf("source PVC %s/%s was replaced after delete request; refusing to resume", namespace, state.SourceName)
		default:
			// The request was not accepted or deletion is still pending. Retrying
			// with the original UID precondition is idempotent and ownership-safe.
			if err := kube.DeletePVC(ctx, client, namespace, state.SourceName, state.OriginalSourceUID); err != nil {
				return err
			}
		}
		if err := kube.WaitForPVCDeleted(ctx, client, namespace, state.SourceName, state.OriginalSourceUID, cfg.Timeout, pollInterval, pvcDeletionObserver(cfg, namespace, state.SourceName)); err != nil {
			return err
		}
		if err := updatePhase(operation.PhaseSourceDeleted); err != nil {
			return err
		}
	}

	if state.Phase == operation.PhaseSourceDeleted {
		if _, err := loadOwnedTempPVC(ctx, client, state); err != nil {
			return err
		}
		if err := validateWorkloadsQuiesced(ctx, client, namespace, state.SourceName, state.Deployments, state.NoScale); err != nil {
			return fmt.Errorf("revalidate workloads before recreating source: %w", err)
		}
		setProgressPhase(cfg, liveprogress.ReplaceSourceCreating, fmt.Sprintf("ensuring replacement PVC %s/%s exists while resuming", namespace, state.SourceName))
		recreated, getErr := client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, state.SourceName, metav1.GetOptions{})
		if apierrors.IsNotFound(getErr) {
			recreated, err = client.CoreV1().PersistentVolumeClaims(namespace).Create(ctx, &finalPVC, metav1.CreateOptions{})
			if err != nil {
				return fmt.Errorf("recreate original PVC while resuming: %w", err)
			}
		} else if getErr != nil {
			return fmt.Errorf("inspect recreated PVC while resuming: %w", getErr)
		} else if err := operation.ValidateRecreatedPVC(recreated, state.OperationID); err != nil {
			return fmt.Errorf("same-name PVC appeared during recovery: %w", err)
		}
		recreatedPhase := string(recreated.Status.Phase)
		if recreatedPhase == "" {
			recreatedPhase = "unknown"
		}
		setProgressPhase(cfg, liveprogress.ReplaceSourceProvisioning, fmt.Sprintf("replacement PVC %s/%s observed uid=%s phase=%s", namespace, state.SourceName, recreated.UID, recreatedPhase))
		state.RecreatedSourceUID = recreated.UID
		if err := updatePhase(operation.PhaseSourceRecreated); err != nil {
			return err
		}
	}

	if state.Phase == operation.PhaseSourceRecreated {
		if err := validateWorkloadsQuiesced(ctx, client, namespace, state.SourceName, state.Deployments, state.NoScale); err != nil {
			return fmt.Errorf("revalidate workloads before copy-back: %w", err)
		}
		if _, err := loadOwnedTempPVC(ctx, client, state); err != nil {
			return err
		}
		recreated, err := client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, state.SourceName, metav1.GetOptions{})
		if err != nil {
			return fmt.Errorf("get recreated source PVC while resuming: %w", err)
		}
		if recreated.UID != state.RecreatedSourceUID {
			return fmt.Errorf("recreated source PVC %s/%s was replaced; refusing to resume", namespace, state.SourceName)
		}
		if err := operation.ValidateRecreatedPVC(recreated, state.OperationID); err != nil {
			return err
		}
		recreatedPhase := string(recreated.Status.Phase)
		if recreatedPhase == "" {
			recreatedPhase = "unknown"
		}
		setProgressPhase(cfg, liveprogress.ReplaceSourceProvisioning, fmt.Sprintf("replacement PVC %s/%s observed uid=%s phase=%s", namespace, state.SourceName, recreated.UID, recreatedPhase))
		copyBackLabel := "copy 2 of 2: temporary to source"
		setProgressPhase(cfg, liveprogress.CopyBackScheduling, fmt.Sprintf("creating resumed rsync Job for %s", copyBackLabel))
		copyBack := datamover.Request{
			Namespace: namespace, SourcePVC: state.TempName, DestPVC: state.SourceName, Image: state.Image,
			JobName: naming.SafeDNSLabel("shrink-copy-back-" + state.SourceName), Args: state.RsyncArgs,
			RunAsUser: state.RunAsUser, FSGroup: state.FSGroup,
			WaitTimeout: cfg.Timeout, PollInterval: pollInterval,
			Observe: moverObserver(cfg, moverProgressSpec{
				scheduling: liveprogress.CopyBackScheduling, mounting: liveprogress.CopyBackMounting,
				running: liveprogress.CopyBackTransferring, preparationMount: liveprogress.ReplaceSourceMounting,
				copyLabel: copyBackLabel,
			}),
		}
		if err := mover.Move(ctx, copyBack); err != nil {
			return err
		}
		setProgressPhase(cfg, liveprogress.VerifySourceScheduling, "creating checksum verification Job for replacement source while resuming")
		verifySource := copyBack
		verifySource.Observe = moverObserver(cfg, moverProgressSpec{
			scheduling: liveprogress.VerifySourceScheduling, mounting: liveprogress.VerifySourceScheduling,
			running: liveprogress.VerifySourceChecksumming, verificationLabel: "replacement source verification",
		})
		if err := mover.Verify(ctx, verifySource); err != nil {
			return err
		}
		setProgressPhase(cfg, liveprogress.VerifySourceCompleted, "replacement source checksum verification completed without differences")
		if err := updatePhase(operation.PhaseCopiedBack); err != nil {
			return err
		}
	}

	if state.Phase != operation.PhaseCopiedBack {
		return fmt.Errorf("unsupported persisted operation phase %q", state.Phase)
	}
	if err := validateRecreatedSource(ctx, client, state); err != nil {
		return err
	}
	if len(state.Deployments) > 0 && !state.NoScale {
		// A failed restore may have updated only some Deployments. Keep the deferred
		// final attempt armed until the full set is restored successfully.
		restoreOnExit = true
		scaled = true
		setProgressPhase(cfg, liveprogress.RestoreControllers, fmt.Sprintf("restoring %d Deployment replica target(s) while resuming", len(state.Deployments)))
		restoreCtx, cancel := context.WithTimeout(context.Background(), cfg.Timeout)
		err := kube.RestoreDeployments(restoreCtx, client, state.Deployments)
		cancel()
		if err != nil {
			return fmt.Errorf("restore Deployment replicas while resuming: %w", err)
		}
		scaled = false
		setProgressPhase(cfg, liveprogress.RestoreObservingConsumers, "Deployment replica targets restored; application readiness is not used as a recovery gate")
	}
	if !state.KeepTemp {
		setProgressPhase(cfg, liveprogress.CleanupTempPVC, fmt.Sprintf("deleting temporary recovery PVC %s/%s with expected uid=%s", namespace, state.TempName, state.TempUID))
		if err := kube.DeletePVC(ctx, client, namespace, state.TempName, state.TempUID); err != nil {
			return err
		}
		progressActivity(cfg, fmt.Sprintf("temporary recovery PVC %s/%s delete request accepted", namespace, state.TempName))
	}
	setProgressPhase(cfg, liveprogress.CleanupCheckpoint, fmt.Sprintf("deleting recovery checkpoint for PVC %s/%s", namespace, state.SourceName))
	if err := store.Delete(ctx, stateCM.UID); err != nil {
		return err
	}
	progressActivity(cfg, fmt.Sprintf("recovery checkpoint for PVC %s/%s removed", namespace, state.SourceName))
	durableOutput(cfg, cfg.IOStreams.Out, "PVC shrink workflow resumed and completed successfully.\n")
	return nil
}

func loadOwnedTempPVC(ctx context.Context, client kubernetes.Interface, state *operation.State) (*corev1.PersistentVolumeClaim, error) {
	temp, err := client.CoreV1().PersistentVolumeClaims(state.Namespace).Get(ctx, state.TempName, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("get temporary recovery PVC before resuming: %w", err)
	}
	if temp.UID != state.TempUID || temp.Annotations[operation.AnnotationOperationID] != state.OperationID {
		return nil, fmt.Errorf("temporary PVC %s/%s was replaced or is not owned by this operation", state.Namespace, state.TempName)
	}
	return temp, nil
}
