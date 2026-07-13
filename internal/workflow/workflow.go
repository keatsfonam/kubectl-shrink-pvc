package workflow

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/client-go/kubernetes"

	"github.com/keatsfonam/kubectl-shrink-pvc/internal/datamover"
	"github.com/keatsfonam/kubectl-shrink-pvc/internal/inspect"
	"github.com/keatsfonam/kubectl-shrink-pvc/internal/kube"
	"github.com/keatsfonam/kubectl-shrink-pvc/internal/naming"
	"github.com/keatsfonam/kubectl-shrink-pvc/internal/operation"
	"github.com/keatsfonam/kubectl-shrink-pvc/internal/pvcmanifest"
)

type Config struct {
	PVCName             string
	TargetSize          string
	Yes                 bool
	DryRun              bool
	KeepTemp            bool
	NoScale             bool
	Resume              bool
	TempName            string
	Image               string
	RsyncArgs           []string
	RsyncExtraArgs      string
	RunAsUser           int64
	FSGroup             int64
	SafetyMarginPercent int
	Timeout             time.Duration
	IOStreams           genericclioptions.IOStreams
	ConfigFlags         *genericclioptions.ConfigFlags
}

const (
	// Status checks do not need to be configurable.
	pollInterval = 2 * time.Second

	tempSourceUIDAnnotation  = "shrink-pvc.keats.dev/source-uid"
	tempSourceNameAnnotation = "shrink-pvc.keats.dev/source-name"
)

func Run(ctx context.Context, cfg Config) (retErr error) {
	if cfg.DryRun && cfg.Resume {
		return fmt.Errorf("--dry-run cannot be combined with --resume")
	}
	if cfg.SafetyMarginPercent < 0 {
		return fmt.Errorf("--safety-margin must be non-negative")
	}
	if cfg.Timeout <= 0 {
		return fmt.Errorf("--timeout must be positive")
	}
	if cfg.RunAsUser == 0 {
		return fmt.Errorf("--run-as-user must be a non-zero UID; omit it to run as root")
	}
	if cfg.FSGroup < 0 && cfg.RunAsUser > 0 {
		cfg.FSGroup = cfg.RunAsUser
	}
	normalizedArgs, err := normalizeRsyncArgs(cfg.RsyncArgs, cfg.RsyncExtraArgs)
	if err != nil {
		return err
	}
	cfg.RsyncArgs = normalizedArgs
	if cfg.RsyncExtraArgs != "" {
		fmt.Fprintln(cfg.IOStreams.ErrOut, "Warning: --rsync-extra-args is deprecated; use repeatable --rsync-arg=--option=value instead.")
	}

	target, err := resource.ParseQuantity(cfg.TargetSize)
	if err != nil {
		return fmt.Errorf("parse --size %q: %w", cfg.TargetSize, err)
	}
	if target.Sign() <= 0 {
		return fmt.Errorf("--size must be positive")
	}
	client, namespace, err := kube.Clientset(cfg.ConfigFlags)
	if err != nil {
		return err
	}
	if cfg.Resume {
		return withOperationLease(ctx, cfg, client, namespace, func(lockedCtx context.Context) error {
			return resume(lockedCtx, cfg, client, namespace, target)
		})
	}

	source, err := client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, cfg.PVCName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("get source PVC %s/%s: %w", namespace, cfg.PVCName, err)
	}
	if source.Spec.VolumeMode != nil && *source.Spec.VolumeMode == corev1.PersistentVolumeBlock {
		return fmt.Errorf("PVC %s/%s uses volumeMode=Block; v1 supports Filesystem PVCs only", namespace, cfg.PVCName)
	}
	current := pvcmanifest.CurrentSize(source)
	if current.IsZero() {
		return fmt.Errorf("could not determine current size for PVC %s/%s", namespace, cfg.PVCName)
	}
	if target.Cmp(current) >= 0 {
		return fmt.Errorf("target size %s must be smaller than current PVC size %s", target.String(), current.String())
	}

	plan, err := kube.DiscoverConsumers(ctx, client, namespace, cfg.PVCName)
	if err != nil {
		return err
	}
	if cfg.TempName == "" {
		cfg.TempName = pvcmanifest.TempName(cfg.PVCName)
	}
	if cfg.TempName == cfg.PVCName {
		return fmt.Errorf("temporary PVC name must differ from source PVC name")
	}

	printPlan(cfg, namespace, source, target, plan)
	if cfg.DryRun {
		fmt.Fprintln(cfg.IOStreams.Out, "\nDry-run only; no changes made.")
		return nil
	}
	if err := validateConsumers(plan, cfg.NoScale); err != nil {
		return err
	}
	if !cfg.Yes {
		if err := confirm(cfg); err != nil {
			return err
		}
	}

	return withOperationLease(ctx, cfg, client, namespace, func(ctx context.Context) (retErr error) {
		source, plan, err = revalidateExecutionPlan(ctx, client, namespace, cfg.PVCName, source, plan, cfg.NoScale)
		if err != nil {
			return err
		}
		store := operation.StoreForPVC(client, namespace, cfg.PVCName)
		if err := store.EnsureAbsent(ctx); err != nil {
			return err
		}
		operationID, err := operation.NewID()
		if err != nil {
			return err
		}
		finalPVC, err := pvcmanifest.Build(source, cfg.PVCName, target)
		if err != nil {
			return err
		}
		operation.StampRecreatedPVC(finalPVC, operationID)
		finalPVCJSON, err := json.Marshal(finalPVC)
		if err != nil {
			return fmt.Errorf("encode replacement PVC: %w", err)
		}
		// Persist the approved UID-bound replica counts before the first scale
		// write. A crash from this point can be recovered with --resume.
		state := &operation.State{
			Version: 1, OperationID: operationID, Namespace: namespace, SourceName: cfg.PVCName,
			OriginalSourceUID: source.UID, TempName: cfg.TempName, TargetSize: target.String(),
			Image: cfg.Image, RsyncArgs: cfg.RsyncArgs, RunAsUser: cfg.RunAsUser, FSGroup: cfg.FSGroup,
			KeepTemp: cfg.KeepTemp, NoScale: cfg.NoScale, Deployments: plan.Deployments,
			FinalPVCJSON: finalPVCJSON, Phase: operation.PhasePrepared,
		}
		stateCM, err := store.Create(ctx, state)
		if err != nil {
			return err
		}
		stateResourceVersion := stateCM.ResourceVersion
		updatePhase := func(phase operation.Phase) error {
			state.Phase = phase
			var updateErr error
			stateResourceVersion, updateErr = store.Update(ctx, state, stateResourceVersion)
			return updateErr
		}

		scaled := false
		restoreOnExit := true
		defer func() {
			if !scaled {
				return
			}
			if !restoreOnExit {
				fmt.Fprintln(cfg.IOStreams.ErrOut, "Warning: not restoring Deployment replicas because the original PVC replacement did not complete. Restore manually after recovery.")
				return
			}
			fmt.Fprintln(cfg.IOStreams.Out, "Restoring Deployment replica counts...")
			restoreCtx, cancel := context.WithTimeout(context.Background(), cfg.Timeout)
			defer cancel()
			if err := kube.RestoreDeployments(restoreCtx, client, plan.Deployments); err != nil {
				restoreErr := fmt.Errorf("restore Deployment replicas: %w", err)
				fmt.Fprintf(cfg.IOStreams.ErrOut, "Warning: %v\n", restoreErr)
				retErr = errors.Join(retErr, restoreErr)
			}
		}()

		if !cfg.NoScale && len(plan.Deployments) > 0 {
			fmt.Fprintln(cfg.IOStreams.Out, "Scaling Deployments to zero...")
			scaled = true // any attempted write must receive a final restoration attempt
			if err := kube.ScaleDeployments(ctx, client, plan.Deployments, 0); err != nil {
				return err
			}
		}

		fmt.Fprintln(cfg.IOStreams.Out, "Waiting for source PVC to be unmounted...")
		if err := kube.WaitForPVCUnmounted(ctx, client, namespace, cfg.PVCName, cfg.Timeout, pollInterval); err != nil {
			return err
		}

		fmt.Fprintln(cfg.IOStreams.Out, "Inspecting source PVC usage...")
		usedBytes, err := inspect.UsageBytes(ctx, client, inspect.Options{
			Namespace: namespace, PVCName: cfg.PVCName, Image: cfg.Image,
			RunAsUser: cfg.RunAsUser, FSGroup: cfg.FSGroup,
			WaitTimeout: cfg.Timeout, PollInterval: pollInterval,
		})
		if err != nil {
			return err
		}
		requiredBytes, err := requiredBytesWithMargin(usedBytes, cfg.SafetyMarginPercent)
		if err != nil {
			return err
		}
		if requiredBytes > target.Value() {
			return fmt.Errorf("source PVC contains about %d bytes; with %d%% safety margin it requires %d bytes, which exceeds target size %d bytes", usedBytes, cfg.SafetyMarginPercent, requiredBytes, target.Value())
		}
		fmt.Fprintf(cfg.IOStreams.Out, "Source usage: %d bytes; required with %d%% margin: %d bytes; target: %d bytes.\n", usedBytes, cfg.SafetyMarginPercent, requiredBytes, target.Value())

		tempPVC, err := pvcmanifest.Build(source, cfg.TempName, target)
		if err != nil {
			return err
		}
		if tempPVC.Annotations == nil {
			tempPVC.Annotations = map[string]string{}
		}
		tempPVC.Annotations[tempSourceUIDAnnotation] = string(source.UID)
		tempPVC.Annotations[tempSourceNameAnnotation] = source.Name
		tempPVC.Annotations[operation.AnnotationOperationID] = operationID
		fmt.Fprintf(cfg.IOStreams.Out, "Creating temporary PVC %s/%s...\n", namespace, cfg.TempName)
		tempPVC, reused, err := ensureTemporaryPVC(ctx, client, namespace, tempPVC, target)
		if err != nil {
			return err
		}
		if reused {
			fmt.Fprintf(cfg.IOStreams.Out, "Temporary PVC %s/%s already exists at the requested size; reusing it.\n", namespace, cfg.TempName)
		}

		mover := datamover.RsyncMover{Client: client}
		fmt.Fprintf(cfg.IOStreams.Out, "Copying %s -> %s...\n", cfg.PVCName, cfg.TempName)
		copyToTemp := datamover.Request{
			Namespace: namespace, SourcePVC: cfg.PVCName, DestPVC: cfg.TempName, Image: cfg.Image,
			JobName: naming.SafeDNSLabel("shrink-copy-to-temp-" + cfg.PVCName), Args: cfg.RsyncArgs,
			RunAsUser: cfg.RunAsUser, FSGroup: cfg.FSGroup,
			WaitTimeout: cfg.Timeout, PollInterval: pollInterval,
		}
		if err := mover.Move(ctx, copyToTemp); err != nil {
			return err
		}
		fmt.Fprintln(cfg.IOStreams.Out, "Verifying temporary copy...")
		if err := mover.Verify(ctx, copyToTemp); err != nil {
			return err
		}

		state.TempUID = tempPVC.UID
		if err := updatePhase(operation.PhaseCopiedToTemp); err != nil {
			return err
		}

		if err := validateDestructiveBoundary(ctx, client, namespace, cfg.PVCName, source.UID, plan.Deployments, cfg.NoScale); err != nil {
			return err
		}
		// Checkpoint and disarm restoration before the ambiguous Delete call: the
		// API server can accept deletion even when the client receives an error.
		if err := updatePhase(operation.PhaseSourceDeleteRequested); err != nil {
			return err
		}
		restoreOnExit = false
		fmt.Fprintf(cfg.IOStreams.Out, "Deleting original PVC %s/%s...\n", namespace, cfg.PVCName)
		if err := kube.DeletePVC(ctx, client, namespace, cfg.PVCName, source.UID); err != nil {
			return err
		}
		if err := kube.WaitForPVCDeleted(ctx, client, namespace, cfg.PVCName, source.UID, cfg.Timeout, pollInterval); err != nil {
			return err
		}
		if err := updatePhase(operation.PhaseSourceDeleted); err != nil {
			return err
		}
		if err := validateWorkloadsQuiesced(ctx, client, namespace, cfg.PVCName, plan.Deployments, cfg.NoScale); err != nil {
			return fmt.Errorf("revalidate workloads before recreating source: %w", err)
		}

		fmt.Fprintf(cfg.IOStreams.Out, "Recreating original PVC %s/%s at %s...\n", namespace, cfg.PVCName, target.String())
		recreated, err := client.CoreV1().PersistentVolumeClaims(namespace).Create(ctx, finalPVC, metav1.CreateOptions{})
		if err != nil {
			return fmt.Errorf("recreate original PVC: %w", err)
		}
		state.RecreatedSourceUID = recreated.UID
		if err := updatePhase(operation.PhaseSourceRecreated); err != nil {
			return err
		}
		if err := validateWorkloadsQuiesced(ctx, client, namespace, cfg.PVCName, plan.Deployments, cfg.NoScale); err != nil {
			return fmt.Errorf("revalidate workloads before copy-back: %w", err)
		}

		fmt.Fprintf(cfg.IOStreams.Out, "Copying %s -> %s...\n", cfg.TempName, cfg.PVCName)
		copyBack := datamover.Request{
			Namespace: namespace, SourcePVC: cfg.TempName, DestPVC: cfg.PVCName, Image: cfg.Image,
			JobName: naming.SafeDNSLabel("shrink-copy-back-" + cfg.PVCName), Args: cfg.RsyncArgs,
			RunAsUser: cfg.RunAsUser, FSGroup: cfg.FSGroup,
			WaitTimeout: cfg.Timeout, PollInterval: pollInterval,
		}
		if err := mover.Move(ctx, copyBack); err != nil {
			return err
		}
		fmt.Fprintln(cfg.IOStreams.Out, "Verifying restored copy...")
		if err := mover.Verify(ctx, copyBack); err != nil {
			return err
		}
		if err := updatePhase(operation.PhaseCopiedBack); err != nil {
			return err
		}
		if err := validateRecreatedSource(ctx, client, state); err != nil {
			return err
		}
		// Consumers may only be restored after copy-back is durably checkpointed;
		// otherwise resume could repeat rsync while applications are writing.
		restoreOnExit = true
		if scaled {
			restoreCtx, cancel := context.WithTimeout(context.Background(), cfg.Timeout)
			err := kube.RestoreDeployments(restoreCtx, client, plan.Deployments)
			cancel()
			if err != nil {
				return fmt.Errorf("restore Deployment replicas: %w", err)
			}
			scaled = false
		}

		if !cfg.KeepTemp {
			fmt.Fprintf(cfg.IOStreams.Out, "Deleting temporary PVC %s/%s...\n", namespace, cfg.TempName)
			if err := kube.DeletePVC(ctx, client, namespace, cfg.TempName, tempPVC.UID); err != nil {
				return err
			}
		}
		if err := store.Delete(ctx, stateCM.UID); err != nil {
			return err
		}

		fmt.Fprintln(cfg.IOStreams.Out, "PVC shrink workflow completed successfully.")
		return nil
	})
}

func withOperationLease(ctx context.Context, cfg Config, client kubernetes.Interface, namespace string, fn func(context.Context) error) (retErr error) {
	holder, err := operation.NewID()
	if err != nil {
		return err
	}
	lock, err := operation.AcquireLease(ctx, client, namespace, cfg.PVCName, holder)
	if err != nil {
		return err
	}
	defer func() {
		releaseCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		retErr = errors.Join(retErr, lock.Release(releaseCtx))
	}()
	return fn(lock.Context())
}

func revalidateExecutionPlan(ctx context.Context, client kubernetes.Interface, namespace, pvcName string, source *corev1.PersistentVolumeClaim, approved *kube.ConsumerPlan, noScale bool) (*corev1.PersistentVolumeClaim, *kube.ConsumerPlan, error) {
	currentSource, err := client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvcName, metav1.GetOptions{})
	if err != nil {
		return nil, nil, fmt.Errorf("revalidate source PVC %s/%s: %w", namespace, pvcName, err)
	}
	if currentSource.UID != source.UID {
		return nil, nil, fmt.Errorf("source PVC %s/%s was replaced after planning; rerun the command", namespace, pvcName)
	}

	fresh, err := kube.DiscoverConsumers(ctx, client, namespace, pvcName)
	if err != nil {
		return nil, nil, fmt.Errorf("revalidate PVC consumers: %w", err)
	}
	if err := validateConsumers(fresh, noScale); err != nil {
		return nil, nil, err
	}
	approvedDeployments := map[string]kube.DeploymentRef{}
	for _, dep := range approved.Deployments {
		approvedDeployments[dep.Namespace+"/"+dep.Name] = dep
	}
	for _, dep := range fresh.Deployments {
		approvedDep, ok := approvedDeployments[dep.Namespace+"/"+dep.Name]
		if !ok {
			return nil, nil, fmt.Errorf("new Deployment consumer %s/%s appeared after confirmation; rerun the command", dep.Namespace, dep.Name)
		}
		if approvedDep.UID == "" || dep.UID != approvedDep.UID {
			return nil, nil, fmt.Errorf("deployment consumer %s/%s was replaced after confirmation; rerun the command", dep.Namespace, dep.Name)
		}
	}

	refreshed := &kube.ConsumerPlan{Pods: fresh.Pods, Deployments: append([]kube.DeploymentRef(nil), approved.Deployments...)}
	for i := range refreshed.Deployments {
		dep := &refreshed.Deployments[i]
		current, err := client.AppsV1().Deployments(dep.Namespace).Get(ctx, dep.Name, metav1.GetOptions{})
		if err != nil {
			return nil, nil, fmt.Errorf("refresh Deployment %s/%s: %w", dep.Namespace, dep.Name, err)
		}
		if dep.UID == "" || current.UID != dep.UID {
			return nil, nil, fmt.Errorf("deployment consumer %s/%s was replaced after confirmation", dep.Namespace, dep.Name)
		}
		scale, err := client.AppsV1().Deployments(dep.Namespace).GetScale(ctx, dep.Name, metav1.GetOptions{})
		if err != nil {
			return nil, nil, fmt.Errorf("refresh scale for Deployment %s/%s: %w", dep.Namespace, dep.Name, err)
		}
		dep.Replicas = scale.Spec.Replicas
	}
	hpas, err := kube.DiscoverDeploymentHPAs(ctx, client, refreshed.Deployments)
	if err != nil {
		return nil, nil, err
	}
	if len(hpas) > 0 {
		items := make([]string, 0, len(hpas))
		for _, hpa := range hpas {
			items = append(items, fmt.Sprintf("%s/%s -> Deployment %s/%s", hpa.Namespace, hpa.Name, hpa.Namespace, hpa.DeploymentName))
		}
		return nil, nil, fmt.Errorf("HorizontalPodAutoscalers target PVC consumers; suspend them and rerun: %s", strings.Join(items, ", "))
	}
	return currentSource, refreshed, nil
}

func validateRecreatedSource(ctx context.Context, client kubernetes.Interface, state *operation.State) error {
	recreated, err := client.CoreV1().PersistentVolumeClaims(state.Namespace).Get(ctx, state.SourceName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("validate recreated source before restoration: %w", err)
	}
	if recreated.UID != state.RecreatedSourceUID {
		return fmt.Errorf("recreated source PVC %s/%s was replaced before restoration", state.Namespace, state.SourceName)
	}
	if err := operation.ValidateRecreatedPVC(recreated, state.OperationID); err != nil {
		return fmt.Errorf("validate recreated source before restoration: %w", err)
	}
	return nil
}

func validateDestructiveBoundary(ctx context.Context, client kubernetes.Interface, namespace, pvcName string, expectedUID types.UID, deployments []kube.DeploymentRef, noScale bool) error {
	current, err := client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvcName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("revalidate source PVC before deletion: %w", err)
	}
	if current.UID != expectedUID {
		return fmt.Errorf("source PVC %s/%s was replaced before deletion; refusing to continue", namespace, pvcName)
	}
	return validateWorkloadsQuiesced(ctx, client, namespace, pvcName, deployments, noScale)
}

func validateWorkloadsQuiesced(ctx context.Context, client kubernetes.Interface, namespace, pvcName string, deployments []kube.DeploymentRef, noScale bool) error {
	fresh, err := kube.DiscoverConsumers(ctx, client, namespace, pvcName)
	if err != nil {
		return err
	}
	if len(fresh.Pods) > 0 {
		return fmt.Errorf("PVC %s/%s gained active consumers: %v", namespace, pvcName, fresh.Pods)
	}
	if len(fresh.Unsupported) > 0 {
		return validateConsumers(fresh, noScale)
	}
	expected := make(map[string]kube.DeploymentRef, len(deployments))
	for _, dep := range deployments {
		expected[dep.Namespace+"/"+dep.Name] = dep
	}
	for _, dep := range fresh.Deployments {
		approved, ok := expected[dep.Namespace+"/"+dep.Name]
		if !ok {
			return fmt.Errorf("new Deployment consumer %s/%s appeared; refusing to continue", dep.Namespace, dep.Name)
		}
		if approved.UID == "" || dep.UID != approved.UID {
			return fmt.Errorf("deployment consumer %s/%s was replaced; refusing to continue", dep.Namespace, dep.Name)
		}
	}
	for _, dep := range deployments {
		current, err := client.AppsV1().Deployments(dep.Namespace).Get(ctx, dep.Name, metav1.GetOptions{})
		if err != nil {
			return fmt.Errorf("verify Deployment %s/%s: %w", dep.Namespace, dep.Name, err)
		}
		if dep.UID == "" || current.UID != dep.UID {
			return fmt.Errorf("deployment %s/%s was replaced; refusing to continue", dep.Namespace, dep.Name)
		}
		scale, err := client.AppsV1().Deployments(dep.Namespace).GetScale(ctx, dep.Name, metav1.GetOptions{})
		if err != nil {
			return fmt.Errorf("verify scale for Deployment %s/%s: %w", dep.Namespace, dep.Name, err)
		}
		if scale.Spec.Replicas != 0 {
			return fmt.Errorf("deployment %s/%s is not quiesced (replicas=%d)", dep.Namespace, dep.Name, scale.Spec.Replicas)
		}
	}
	hpas, err := kube.DiscoverDeploymentHPAs(ctx, client, deployments)
	if err != nil {
		return err
	}
	if len(hpas) > 0 {
		return fmt.Errorf("a HorizontalPodAutoscaler appeared before deletion; refusing to continue")
	}
	return nil
}

func validateConsumers(plan *kube.ConsumerPlan, manual bool) error {
	if len(plan.Unsupported) > 0 {
		var items []string
		for _, c := range plan.Unsupported {
			item := fmt.Sprintf("%s/%s", c.Kind, c.Name)
			if c.Pod != "" {
				item += " via pod " + c.Pod
			}
			items = append(items, item)
		}
		if manual {
			return fmt.Errorf("unsupported consumers still have pods using the PVC; scale them down first: %s", strings.Join(items, ", "))
		}
		return fmt.Errorf("unsupported PVC consumers in v1: %s", strings.Join(items, ", "))
	}
	if manual && len(plan.Pods) > 0 {
		return fmt.Errorf("--no-scale requires the PVC to already be unmounted; active pods: %v", plan.Pods)
	}
	if manual {
		for _, dep := range plan.Deployments {
			if dep.Replicas != 0 {
				return fmt.Errorf("--no-scale requires Deployment %s/%s to already be scaled to zero", dep.Namespace, dep.Name)
			}
		}
	}
	return nil
}

func printPlan(cfg Config, namespace string, source *corev1.PersistentVolumeClaim, target resource.Quantity, plan *kube.ConsumerPlan) {
	out := cfg.IOStreams.Out
	current := pvcmanifest.CurrentSize(source)
	fmt.Fprintln(out, "PVC shrink plan")
	fmt.Fprintf(out, "  Source:           %s/%s\n", namespace, cfg.PVCName)
	fmt.Fprintf(out, "  Current size:     %s\n", current.String())
	fmt.Fprintf(out, "  Target size:      %s\n", target.String())
	fmt.Fprintf(out, "  Temporary PVC:    %s/%s\n", namespace, cfg.TempName)
	fmt.Fprintf(out, "  Scale consumers:  %t\n", !cfg.NoScale)
	fmt.Fprintf(out, "  Safety margin:    %d%%\n", cfg.SafetyMarginPercent)
	if len(plan.Pods) > 0 {
		fmt.Fprintf(out, "  Active pods:      %s\n", strings.Join(plan.Pods, ", "))
	}
	for _, dep := range plan.Deployments {
		fmt.Fprintf(out, "  Deployment:       %s/%s replicas=%d\n", dep.Namespace, dep.Name, dep.Replicas)
	}
	for _, c := range plan.Unsupported {
		fmt.Fprintf(out, "  Unsupported:      %s/%s via pod %s\n", c.Kind, c.Name, c.Pod)
	}
}

func confirm(cfg Config) error {
	fmt.Fprint(cfg.IOStreams.Out, "\nContinue? Type 'yes' to proceed: ")
	scanner := bufio.NewScanner(cfg.IOStreams.In)
	if scanner.Scan() && strings.EqualFold(strings.TrimSpace(scanner.Text()), "yes") {
		return nil
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	return fmt.Errorf("confirmation declined")
}

func normalizeRsyncArgs(args []string, legacy string) ([]string, error) {
	result := append([]string(nil), strings.Fields(legacy)...)
	result = append(result, args...)
	for _, arg := range result {
		if arg == "" || !strings.HasPrefix(arg, "-") {
			return nil, fmt.Errorf("rsync arguments must be options beginning with '-' and values must use --option=value: %q", arg)
		}
		if changesRsyncMetadataPolicy(arg) {
			return nil, fmt.Errorf("rsync argument %q changes the metadata preservation policy and cannot be verified safely", arg)
		}
	}
	return result, nil
}

func changesRsyncMetadataPolicy(arg string) bool {
	if strings.HasPrefix(arg, "-") && !strings.HasPrefix(arg, "--") && strings.ContainsAny(strings.TrimPrefix(arg, "-"), "apogtAXH") {
		return true
	}
	for _, prefix := range []string{
		"--archive", "--chmod", "--chown", "--usermap", "--groupmap",
		"--no-perms", "--no-owner", "--no-group", "--no-times",
		"--no-acls", "--no-xattrs", "--no-hard-links",
		"--perms", "--owner", "--group", "--times", "--acls", "--xattrs", "--hard-links",
	} {
		if arg == prefix || strings.HasPrefix(arg, prefix+"=") {
			return true
		}
	}
	return false
}

func requiredBytesWithMargin(usedBytes int64, marginPercent int) (int64, error) {
	required := big.NewInt(usedBytes)
	margin := new(big.Int).Mul(big.NewInt(usedBytes), big.NewInt(int64(marginPercent)))
	margin.Div(margin, big.NewInt(100))
	required.Add(required, margin)
	if !required.IsInt64() {
		return 0, fmt.Errorf("required size with safety margin exceeds int64")
	}
	return required.Int64(), nil
}

func ensureTemporaryPVC(ctx context.Context, client kubernetes.Interface, namespace string, tempPVC *corev1.PersistentVolumeClaim, target resource.Quantity) (*corev1.PersistentVolumeClaim, bool, error) {
	created, err := client.CoreV1().PersistentVolumeClaims(namespace).Create(ctx, tempPVC, metav1.CreateOptions{})
	if err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return nil, false, fmt.Errorf("create temp PVC: %w", err)
		}
		existing, getErr := client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, tempPVC.Name, metav1.GetOptions{})
		if getErr != nil {
			return nil, false, fmt.Errorf("get existing temp PVC %s/%s: %w", namespace, tempPVC.Name, getErr)
		}
		if existing.Annotations[tempSourceUIDAnnotation] != tempPVC.Annotations[tempSourceUIDAnnotation] ||
			existing.Annotations[tempSourceNameAnnotation] != tempPVC.Annotations[tempSourceNameAnnotation] ||
			existing.Annotations[operation.AnnotationOperationID] != tempPVC.Annotations[operation.AnnotationOperationID] {
			return nil, false, fmt.Errorf("temporary PVC %s/%s already exists but is not owned by this source PVC; choose a different --temp-name", namespace, tempPVC.Name)
		}
		existingSize := pvcmanifest.CurrentSize(existing)
		if existingSize.Cmp(target) != 0 {
			return nil, false, fmt.Errorf("temporary PVC %s/%s already exists at size %s, but requested target is %s; delete it or pass a different --temp-name", namespace, tempPVC.Name, existingSize.String(), target.String())
		}
		return existing, true, nil
	}
	return created, false, nil
}
