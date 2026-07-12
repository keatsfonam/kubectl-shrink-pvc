package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"
	"k8s.io/cli-runtime/pkg/genericclioptions"

	"github.com/keatsfonam/kubectl-shrink-pvc/internal/datamover"
	"github.com/keatsfonam/kubectl-shrink-pvc/internal/workflow"
)

// Overridden at release time via -ldflags "-X main.version=...".
var version = "dev"

func main() {
	if err := newRootCmd().ExecuteContext(context.Background()); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func newRootCmd() *cobra.Command {
	streams := genericclioptions.IOStreams{In: os.Stdin, Out: os.Stdout, ErrOut: os.Stderr}
	configFlags := genericclioptions.NewConfigFlags(true)

	cfg := workflow.Config{
		IOStreams:           streams,
		ConfigFlags:         configFlags,
		TempSuffix:          "shrink-tmp",
		InspectImage:        "alpine:3.20",
		CopyImage:           datamover.DefaultImage,
		SafetyMarginPercent: 10,
		RunAsUser:           -1,
		FSGroup:             -1,
		WaitTimeout:         10 * time.Minute,
		PollInterval:        2 * time.Second,
	}

	cmd := &cobra.Command{
		Use:           "kubectl-shrink_pvc PVC --size TARGET_SIZE",
		Short:         "Shrink a Kubernetes PVC by copying data through a temporary PVC",
		Version:       version,
		SilenceUsage:  true,
		SilenceErrors: true,
		Args:          cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg.PVCName = args[0]
			return workflow.Run(cmd.Context(), cfg)
		},
	}

	configFlags.AddFlags(cmd.Flags())
	cmd.Flags().StringVar(&cfg.TargetSize, "size", cfg.TargetSize, "target PVC size, e.g. 20Gi")
	cmd.Flags().BoolVar(&cfg.Yes, "yes", cfg.Yes, "skip confirmation prompts")
	cmd.Flags().BoolVar(&cfg.DryRun, "dry-run", cfg.DryRun, "print the execution plan without changing the cluster")
	cmd.Flags().BoolVar(&cfg.KeepTemp, "keep-temp", cfg.KeepTemp, "keep the temporary PVC after success")
	cmd.Flags().BoolVar(&cfg.ManualScale, "manual-scale", cfg.ManualScale, "do not scale Deployments; require workloads to already be stopped")
	cmd.Flags().BoolVar(&cfg.ReplaceOriginal, "replace-original", cfg.ReplaceOriginal, "delete and recreate the original PVC after the temp copy succeeds")
	cmd.Flags().StringVar(&cfg.TempName, "temp-name", cfg.TempName, "temporary PVC name (default: generated from source name)")
	cmd.Flags().StringVar(&cfg.TempSuffix, "temp-suffix", cfg.TempSuffix, "suffix used when generating the temporary PVC name")
	cmd.Flags().StringVar(&cfg.InspectImage, "inspect-image", cfg.InspectImage, "image used for the source usage inspection pod")
	cmd.Flags().StringVar(&cfg.CopyImage, "copy-image", cfg.CopyImage, "image used for rsync copy jobs")
	cmd.Flags().StringVar(&cfg.RsyncExtraArgs, "rsync-extra-args", cfg.RsyncExtraArgs, "extra arguments appended to rsync copy commands")
	cmd.Flags().Int64Var(&cfg.RunAsUser, "run-as-user", cfg.RunAsUser, "run inspect and copy pods as this non-root UID; file ownership is not preserved (default: run as root)")
	cmd.Flags().Int64Var(&cfg.FSGroup, "fs-group", cfg.FSGroup, "fsGroup for inspect and copy pods (default: the --run-as-user UID)")
	cmd.Flags().IntVar(&cfg.SafetyMarginPercent, "safety-margin", cfg.SafetyMarginPercent, "additional percentage of measured source usage required as free space in the target PVC")
	cmd.Flags().DurationVar(&cfg.WaitTimeout, "wait-timeout", cfg.WaitTimeout, "timeout for pods, jobs, PVC deletion, and workload scaling")
	cmd.Flags().DurationVar(&cfg.PollInterval, "poll-interval", cfg.PollInterval, "interval between Kubernetes status checks")

	_ = cmd.MarkFlagRequired("size")
	return cmd
}
