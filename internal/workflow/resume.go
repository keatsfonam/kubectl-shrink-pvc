package workflow

import (
	"context"
	"encoding/json"
	"fmt"

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

func resume(ctx context.Context, cfg Config, client kubernetes.Interface, namespace string, requestedTarget resource.Quantity) error {
	store := operation.Store{Client: client, Namespace: namespace, Name: operation.NameForPVC(cfg.PVCName)}
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
	if cfg.Image != state.Image || cfg.RsyncExtraArgs != state.RsyncExtraArgs || cfg.RunAsUser != state.RunAsUser {
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

	if state.Phase == operation.PhaseCopiedToTemp {
		source, getErr := client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, state.SourceName, metav1.GetOptions{})
		switch {
		case apierrors.IsNotFound(getErr):
			// The process may have exited after deletion but before checkpointing.
		case getErr != nil:
			return fmt.Errorf("inspect source PVC while resuming: %w", getErr)
		case source.UID != state.OriginalSourceUID:
			return fmt.Errorf("source PVC %s/%s was replaced by an unowned object; refusing to resume", namespace, state.SourceName)
		default:
			if err := validateDestructiveBoundary(ctx, client, namespace, state.SourceName, state.OriginalSourceUID, state.Deployments, state.NoScale); err != nil {
				return err
			}
			if err := kube.DeletePVC(ctx, client, namespace, state.SourceName, state.OriginalSourceUID); err != nil {
				return err
			}
			if err := kube.WaitForPVCDeleted(ctx, client, namespace, state.SourceName, state.OriginalSourceUID, cfg.Timeout, pollInterval); err != nil {
				return err
			}
		}
		if err := updatePhase(operation.PhaseSourceDeleted); err != nil {
			return err
		}
	}

	if state.Phase == operation.PhaseSourceDeleted {
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
		temp, err := client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, state.TempName, metav1.GetOptions{})
		if err != nil {
			return fmt.Errorf("get temporary PVC while resuming: %w", err)
		}
		if temp.UID != state.TempUID || temp.Annotations[operation.AnnotationOperationID] != state.OperationID {
			return fmt.Errorf("temporary PVC %s/%s was replaced or is not owned by this operation", namespace, state.TempName)
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
		if err := mover.Move(ctx, datamover.Request{
			Namespace: namespace, SourcePVC: state.TempName, DestPVC: state.SourceName, Image: state.Image,
			JobName: naming.SafeDNSLabel("shrink-copy-back-" + state.SourceName), ExtraArgs: state.RsyncExtraArgs,
			RunAsUser: state.RunAsUser, FSGroup: state.FSGroup,
			WaitTimeout: cfg.Timeout, PollInterval: pollInterval,
		}); err != nil {
			return err
		}
		if err := updatePhase(operation.PhaseCopiedBack); err != nil {
			return err
		}
	}

	if state.Phase != operation.PhaseCopiedBack {
		return fmt.Errorf("unsupported persisted operation phase %q", state.Phase)
	}
	if len(state.Deployments) > 0 && !state.NoScale {
		restoreCtx, cancel := context.WithTimeout(context.Background(), cfg.Timeout)
		err := kube.RestoreDeployments(restoreCtx, client, state.Deployments)
		cancel()
		if err != nil {
			return fmt.Errorf("restore Deployment replicas while resuming: %w", err)
		}
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
