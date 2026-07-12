package workflow

import (
	"bufio"
	"context"
	"fmt"
	"math/big"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/client-go/kubernetes"

	"github.com/keatsfonam/kubectl-shrink-pvc/internal/datamover"
	"github.com/keatsfonam/kubectl-shrink-pvc/internal/inspect"
	"github.com/keatsfonam/kubectl-shrink-pvc/internal/kube"
	"github.com/keatsfonam/kubectl-shrink-pvc/internal/naming"
	"github.com/keatsfonam/kubectl-shrink-pvc/internal/pvcmanifest"
)

type Config struct {
	PVCName             string
	TargetSize          string
	Yes                 bool
	DryRun              bool
	KeepTemp            bool
	NoScale             bool
	TempName            string
	Image               string
	RsyncExtraArgs      string
	RunAsUser           int64
	FSGroup             int64
	SafetyMarginPercent int
	Timeout             time.Duration
	IOStreams           genericclioptions.IOStreams
	ConfigFlags         *genericclioptions.ConfigFlags
}

// Status checks do not need to be configurable.
const pollInterval = 2 * time.Second

func Run(ctx context.Context, cfg Config) error {
	client, namespace, err := kube.Clientset(cfg.ConfigFlags)
	if err != nil {
		return err
	}
	if cfg.SafetyMarginPercent < 0 {
		return fmt.Errorf("--safety-margin must be non-negative")
	}
	if cfg.RunAsUser == 0 {
		return fmt.Errorf("--run-as-user must be a non-zero UID; omit it to run as root")
	}
	if cfg.FSGroup < 0 && cfg.RunAsUser > 0 {
		cfg.FSGroup = cfg.RunAsUser
	}

	target, err := resource.ParseQuantity(cfg.TargetSize)
	if err != nil {
		return fmt.Errorf("parse --size %q: %w", cfg.TargetSize, err)
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
		if err := kube.RestoreDeployments(context.Background(), client, plan.Deployments); err != nil {
			fmt.Fprintf(cfg.IOStreams.ErrOut, "Warning: failed to restore Deployment replicas: %v\n", err)
		}
	}()

	if !cfg.NoScale && len(plan.Deployments) > 0 {
		fmt.Fprintln(cfg.IOStreams.Out, "Scaling Deployments to zero...")
		if err := kube.ScaleDeployments(ctx, client, plan.Deployments, 0); err != nil {
			return err
		}
		scaled = true
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
	fmt.Fprintf(cfg.IOStreams.Out, "Creating temporary PVC %s/%s...\n", namespace, cfg.TempName)
	reused, err := ensureTemporaryPVC(ctx, client, namespace, tempPVC, target)
	if err != nil {
		return err
	}
	if reused {
		fmt.Fprintf(cfg.IOStreams.Out, "Temporary PVC %s/%s already exists at the requested size; reusing it.\n", namespace, cfg.TempName)
	}

	mover := datamover.RsyncMover{Client: client}
	fmt.Fprintf(cfg.IOStreams.Out, "Copying %s -> %s...\n", cfg.PVCName, cfg.TempName)
	if err := mover.Move(ctx, datamover.Request{
		Namespace: namespace, SourcePVC: cfg.PVCName, DestPVC: cfg.TempName, Image: cfg.Image,
		JobName: naming.SafeDNSLabel("shrink-copy-to-temp-" + cfg.PVCName), ExtraArgs: cfg.RsyncExtraArgs,
		RunAsUser: cfg.RunAsUser, FSGroup: cfg.FSGroup,
		WaitTimeout: cfg.Timeout, PollInterval: pollInterval,
	}); err != nil {
		return err
	}

	restoreOnExit = false
	fmt.Fprintf(cfg.IOStreams.Out, "Deleting original PVC %s/%s...\n", namespace, cfg.PVCName)
	if err := client.CoreV1().PersistentVolumeClaims(namespace).Delete(ctx, cfg.PVCName, metav1.DeleteOptions{}); err != nil {
		return fmt.Errorf("delete original PVC: %w", err)
	}
	if err := kube.WaitForPVCDeleted(ctx, client, namespace, cfg.PVCName, cfg.Timeout, pollInterval); err != nil {
		return err
	}

	finalPVC, err := pvcmanifest.Build(source, cfg.PVCName, target)
	if err != nil {
		return err
	}
	fmt.Fprintf(cfg.IOStreams.Out, "Recreating original PVC %s/%s at %s...\n", namespace, cfg.PVCName, target.String())
	if _, err := client.CoreV1().PersistentVolumeClaims(namespace).Create(ctx, finalPVC, metav1.CreateOptions{}); err != nil {
		return fmt.Errorf("recreate original PVC: %w", err)
	}

	fmt.Fprintf(cfg.IOStreams.Out, "Copying %s -> %s...\n", cfg.TempName, cfg.PVCName)
	if err := mover.Move(ctx, datamover.Request{
		Namespace: namespace, SourcePVC: cfg.TempName, DestPVC: cfg.PVCName, Image: cfg.Image,
		JobName: naming.SafeDNSLabel("shrink-copy-back-" + cfg.PVCName), ExtraArgs: cfg.RsyncExtraArgs,
		RunAsUser: cfg.RunAsUser, FSGroup: cfg.FSGroup,
		WaitTimeout: cfg.Timeout, PollInterval: pollInterval,
	}); err != nil {
		return err
	}
	restoreOnExit = true

	if !cfg.KeepTemp {
		fmt.Fprintf(cfg.IOStreams.Out, "Deleting temporary PVC %s/%s...\n", namespace, cfg.TempName)
		if err := client.CoreV1().PersistentVolumeClaims(namespace).Delete(ctx, cfg.TempName, metav1.DeleteOptions{}); err != nil {
			return fmt.Errorf("delete temp PVC: %w", err)
		}
	}

	fmt.Fprintln(cfg.IOStreams.Out, "PVC shrink workflow completed successfully.")
	return nil
}

func validateConsumers(plan *kube.ConsumerPlan, manual bool) error {
	if len(plan.Unsupported) > 0 {
		var items []string
		for _, c := range plan.Unsupported {
			items = append(items, fmt.Sprintf("%s/%s via pod %s", c.Kind, c.Name, c.Pod))
		}
		if manual {
			return fmt.Errorf("unsupported consumers still have pods using the PVC; scale them down first: %s", strings.Join(items, ", "))
		}
		return fmt.Errorf("unsupported PVC consumers in v1: %s", strings.Join(items, ", "))
	}
	if manual && len(plan.Pods) > 0 {
		return fmt.Errorf("--no-scale requires the PVC to already be unmounted; active pods: %v", plan.Pods)
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

func ensureTemporaryPVC(ctx context.Context, client kubernetes.Interface, namespace string, tempPVC *corev1.PersistentVolumeClaim, target resource.Quantity) (bool, error) {
	if _, err := client.CoreV1().PersistentVolumeClaims(namespace).Create(ctx, tempPVC, metav1.CreateOptions{}); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return false, fmt.Errorf("create temp PVC: %w", err)
		}
		existing, getErr := client.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, tempPVC.Name, metav1.GetOptions{})
		if getErr != nil {
			return false, fmt.Errorf("get existing temp PVC %s/%s: %w", namespace, tempPVC.Name, getErr)
		}
		existingSize := pvcmanifest.CurrentSize(existing)
		if existingSize.Cmp(target) != 0 {
			return false, fmt.Errorf("temporary PVC %s/%s already exists at size %s, but requested target is %s; delete it or pass a different --temp-name", namespace, tempPVC.Name, existingSize.String(), target.String())
		}
		return true, nil
	}
	return false, nil
}
