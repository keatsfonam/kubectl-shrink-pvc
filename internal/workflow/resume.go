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
)

func resume(ctx context.Context, cfg Config, client kubernetes.Interface, namespace string, requestedTarget resource.Quantity) (retErr error) {
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
	mover := datamover.RsyncMover{Client: client}
	scaled := false
	restoreOnExit := true
	defer func() {
		if !scaled || !restoreOnExit {
			return
		}
		restoreCtx, cancel := context.WithTimeout(context.Background(), cfg.Timeout)
		defer cancel()
		if err := kube.RestoreDeployments(restoreCtx, client, state.Deployments); err != nil {
			retErr = errors.Join(retErr, fmt.Errorf("restore Deployment replicas while resuming: %w", err))
		}
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
			if err := kube.RestoreDeployments(ctx, client, state.Deployments); err != nil {
				return fmt.Errorf("restore Deployment replicas from prepared state: %w", err)
			}
		}
		temp, getErr := client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, state.TempName, metav1.GetOptions{})
		if getErr == nil {
			if temp.Annotations[operation.AnnotationOperationID] != state.OperationID ||
				temp.Annotations[tempSourceUIDAnnotation] != string(state.OriginalSourceUID) ||
				temp.Annotations[tempSourceNameAnnotation] != state.SourceName {
				return fmt.Errorf("temporary PVC %s/%s is not owned by prepared operation", namespace, state.TempName)
			}
			if err := kube.DeletePVC(ctx, client, namespace, state.TempName, temp.UID); err != nil {
				return err
			}
		} else if !apierrors.IsNotFound(getErr) {
			return fmt.Errorf("inspect temporary PVC during prepared-state recovery: %w", getErr)
		}
		if err := store.Delete(ctx, stateCM.UID); err != nil {
			return err
		}
		fmt.Fprintln(cfg.IOStreams.Out, "Recovered pre-copy operation state and restored Deployment replicas; rerun the shrink command to start a fresh verified copy.")
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
				scaled = true // ensure a final restoration attempt after any partial scale failure
				if err := kube.ScaleDeployments(ctx, client, state.Deployments, 0); err != nil {
					return err
				}
			}
			if err := kube.WaitForPVCUnmounted(ctx, client, namespace, state.SourceName, cfg.Timeout, pollInterval); err != nil {
				return err
			}
			if err := validateDestructiveBoundary(ctx, client, namespace, state.SourceName, state.OriginalSourceUID, state.Deployments, state.NoScale); err != nil {
				return err
			}
			restoreOnExit = false
			if err := updatePhase(operation.PhaseSourceDeleteRequested); err != nil {
				return err
			}
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
		if err := validateWorkloadsQuiesced(ctx, client, namespace, state.SourceName, state.Deployments, state.NoScale); err != nil {
			return fmt.Errorf("revalidate workloads after delete request: %w", err)
		}
		source, getErr := client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, state.SourceName, metav1.GetOptions{})
		switch {
		case apierrors.IsNotFound(getErr):
			// The prior request succeeded even if its response was lost.
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
		if err := kube.WaitForPVCDeleted(ctx, client, namespace, state.SourceName, state.OriginalSourceUID, cfg.Timeout, pollInterval); err != nil {
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
		fmt.Fprintf(cfg.IOStreams.Out, "Resuming copy %s -> %s...\n", state.TempName, state.SourceName)
		copyBack := datamover.Request{
			Namespace: namespace, SourcePVC: state.TempName, DestPVC: state.SourceName, Image: state.Image,
			JobName: naming.SafeDNSLabel("shrink-copy-back-" + state.SourceName), Args: state.RsyncArgs,
			RunAsUser: state.RunAsUser, FSGroup: state.FSGroup,
			WaitTimeout: cfg.Timeout, PollInterval: pollInterval,
		}
		if err := mover.Move(ctx, copyBack); err != nil {
			return err
		}
		if err := mover.Verify(ctx, copyBack); err != nil {
			return err
		}
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
		restoreCtx, cancel := context.WithTimeout(context.Background(), cfg.Timeout)
		err := kube.RestoreDeployments(restoreCtx, client, state.Deployments)
		cancel()
		if err != nil {
			return fmt.Errorf("restore Deployment replicas while resuming: %w", err)
		}
		scaled = false
	}
	if !state.KeepTemp {
		if err := kube.DeletePVC(ctx, client, namespace, state.TempName, state.TempUID); err != nil {
			return err
		}
	}
	if err := store.Delete(ctx, stateCM.UID); err != nil {
		return err
	}
	fmt.Fprintln(cfg.IOStreams.Out, "PVC shrink workflow resumed and completed successfully.")
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
