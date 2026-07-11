package datamover

import (
	"strings"
	"testing"
)

func TestRsyncCommand(t *testing.T) {
	cmd := rsyncCommand("--partial")
	for _, want := range []string{"rsync", "-aHAX", "--numeric-ids", "--delete", "--info=progress2", "--partial", "/src/", "/dest/"} {
		if !strings.Contains(cmd, want) {
			t.Fatalf("command %q does not contain %q", cmd, want)
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
