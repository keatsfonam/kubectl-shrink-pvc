package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestRootCommandExposesSafeRecoveryFlags(t *testing.T) {
	cmd := newRootCmd()
	for _, name := range []string{"resume", "quiet", "rsync-arg", "rsync-extra-args", "size"} {
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
	quiet := cmd.LocalNonPersistentFlags().Lookup("quiet")
	if quiet == nil {
		t.Fatal("--quiet must be a local root flag")
	}
	if quiet.Value.Type() != "bool" || quiet.DefValue != "false" {
		t.Fatalf("--quiet type/default = %s/%s, want bool/false", quiet.Value.Type(), quiet.DefValue)
	}
	if quiet.Shorthand != "" {
		t.Fatalf("--quiet unexpectedly has shorthand -%s", quiet.Shorthand)
	}
	if cmd.PersistentFlags().Lookup("quiet") != nil {
		t.Fatal("--quiet must not be persistent")
	}
}

func TestRootHelpDocumentsQuietWithoutChangingHelpOrVersionBehavior(t *testing.T) {
	cmd := newRootCmd()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"--help"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("help returned error: %v", err)
	}
	if !strings.Contains(out.String(), "--quiet") || !strings.Contains(out.String(), "suppress live progress") {
		t.Fatalf("help does not document --quiet:\n%s", out.String())
	}

	versionCmd := newRootCmd()
	out.Reset()
	versionCmd.SetOut(&out)
	versionCmd.SetErr(&out)
	versionCmd.SetArgs([]string{"--version"})
	if err := versionCmd.Execute(); err != nil {
		t.Fatalf("version returned error: %v", err)
	}
	if !strings.Contains(out.String(), version) {
		t.Fatalf("version output = %q, want %q", out.String(), version)
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
