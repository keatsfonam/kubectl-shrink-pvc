package datamover

import (
	"strings"
	"testing"
)

func TestJobDisablesServiceAccountToken(t *testing.T) {
	job := buildJob(Request{Namespace: "ns", SourcePVC: "source", DestPVC: "dest", Image: "image", RunAsUser: -1, FSGroup: -1}, "job", "rsync /src/ /dest/", false, 0)
	value := job.Spec.Template.Spec.AutomountServiceAccountToken
	if value == nil || *value {
		t.Fatal("rsync Job must disable service account token automount")
	}
}

func TestRsyncCommand(t *testing.T) {
	cmd := rsyncCommand("--partial", false)
	for _, want := range []string{"rsync", "-aHAX", "--numeric-ids", "--exclude=lost+found", "--delete", "--info=progress2", "--partial", "/src/", "/dest/"} {
		if !strings.Contains(cmd, want) {
			t.Fatalf("command %q does not contain %q", cmd, want)
		}
	}
}

func TestRsyncCommandNonRoot(t *testing.T) {
	cmd := rsyncCommand("", true)
	for _, want := range []string{"-rlHt", "-O", "--exclude=lost+found"} {
		if !strings.Contains(cmd, want) {
			t.Fatalf("command %q does not contain %q", cmd, want)
		}
	}
	for _, banned := range []string{"-aHAX", "-rlHpt", "--numeric-ids"} {
		if strings.Contains(cmd, banned) {
			t.Fatalf("command %q must not contain %q without root", cmd, banned)
		}
	}
}

func TestUniqueNameFitsDNSLabel(t *testing.T) {
	got := uniqueName("abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz")
	if len(got) > 63 {
		t.Fatalf("uniqueName returned %d chars", len(got))
	}
	if got == "" {
		t.Fatal("uniqueName returned empty string")
	}
}
