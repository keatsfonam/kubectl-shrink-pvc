package main

import (
	"strings"
	"testing"
)

func TestRootCommandExposesSafeRecoveryFlags(t *testing.T) {
	cmd := newRootCmd()
	for _, name := range []string{"resume", "rsync-arg", "rsync-extra-args", "size"} {
		if cmd.Flags().Lookup(name) == nil {
			t.Fatalf("expected --%s flag", name)
		}
	}
	if flag := cmd.Flags().Lookup("rsync-arg"); flag.Value.Type() != "stringArray" {
		t.Fatalf("--rsync-arg must preserve argument boundaries, got type %s", flag.Value.Type())
	}
	if flag := cmd.Flags().Lookup("rsync-extra-args"); flag.Deprecated == "" {
		t.Fatal("--rsync-extra-args must be marked deprecated")
	}
}

func TestRootCommandRejectsDryRunResume(t *testing.T) {
	cmd := newRootCmd()
	cmd.SetArgs([]string{"data", "--size=1Gi", "--dry-run", "--resume"})
	if err := cmd.Execute(); err == nil || !strings.Contains(err.Error(), "cannot be combined") {
		t.Fatalf("expected incompatible flag error, got %v", err)
	}
}

func TestRootCommandUsesPinnedDefaultImage(t *testing.T) {
	cmd := newRootCmd()
	value := cmd.Flags().Lookup("image").DefValue
	if value == "" || value == "latest" {
		t.Fatalf("unexpected default image %q", value)
	}
}
