package datamover

import (
	"reflect"
	"strings"
	"testing"
)

func TestJobDisablesServiceAccountToken(t *testing.T) {
	job := buildJob(Request{Namespace: "ns", SourcePVC: "source", DestPVC: "dest", Image: "image", RunAsUser: -1, FSGroup: -1}, "job", []string{"/src/", "/dest/"}, false, 0, false)
	value := job.Spec.Template.Spec.AutomountServiceAccountToken
	if value == nil || *value {
		t.Fatal("rsync Job must disable service account token automount")
	}
}

func TestRsyncArgs(t *testing.T) {
	got := rsyncArgs([]string{"--partial", "--exclude=path with spaces"}, false)
	want := []string{"-aHAX", "--numeric-ids", "--exclude=lost+found", "--delete", "--info=progress2", "--partial", "--exclude=path with spaces", "/src/", "/dest/"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected args:\n got: %#v\nwant: %#v", got, want)
	}
}

func TestRsyncArgsNonRoot(t *testing.T) {
	got := rsyncArgs(nil, true)
	want := []string{"-rlHt", "-O", "--exclude=lost+found", "--delete", "--info=progress2", "/src/", "/dest/"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected args:\n got: %#v\nwant: %#v", got, want)
	}
}

func TestBuildJobUsesDirectRsyncArgv(t *testing.T) {
	args := []string{"--partial", "/src/", "/dest/"}
	job := buildJob(Request{Namespace: "ns", SourcePVC: "source", DestPVC: "dest", Image: "image"}, "job", args, false, 0, false)
	container := job.Spec.Template.Spec.Containers[0]
	if !reflect.DeepEqual(container.Command, []string{"rsync"}) {
		t.Fatalf("unexpected command: %#v", container.Command)
	}
	if !reflect.DeepEqual(container.Args, args) {
		t.Fatalf("unexpected args: %#v", container.Args)
	}
}

func TestVerificationJobMountsBothPVCsReadOnly(t *testing.T) {
	job := buildJob(Request{Namespace: "ns", SourcePVC: "source", DestPVC: "dest", Image: "image"}, "verify", verifyArgs(nil, false), false, 0, true)
	for _, mount := range job.Spec.Template.Spec.Containers[0].VolumeMounts {
		if !mount.ReadOnly {
			t.Fatalf("verification mount %s is writable", mount.Name)
		}
	}
	for _, volume := range job.Spec.Template.Spec.Volumes {
		if volume.PersistentVolumeClaim == nil || !volume.PersistentVolumeClaim.ReadOnly {
			t.Fatalf("verification volume %s is writable", volume.Name)
		}
	}
}

func TestVerificationDifferencesIgnoresUnchangedEntries(t *testing.T) {
	logs := ".f          files/blob1\n.d          files\n"
	if got := verificationDifferences(logs); got != "" {
		t.Fatalf("unchanged entries reported as differences: %q", got)
	}
}

func TestVerificationDifferencesReportsChanges(t *testing.T) {
	logs := ".f          files/unchanged\n>fc........ files/changed\n*deleting   files/extra\n"
	want := ">fc........ files/changed\n*deleting   files/extra"
	if got := verificationDifferences(logs); got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestVerifyArgs(t *testing.T) {
	got := verifyArgs([]string{"--exclude=cache", "--bwlimit=10m"}, false)
	if got[len(got)-2] != "/src/" || got[len(got)-1] != "/dest/" {
		t.Fatalf("unexpected verification operands: %#v", got)
	}
	for _, required := range []string{"-aHAXniO", "--numeric-ids", "--checksum", "--delete", "--itemize-changes", "--exclude=cache"} {
		found := false
		for _, arg := range got {
			if arg == required {
				found = true
			}
		}
		if !found {
			t.Fatalf("verification args missing %s: %#v", required, got)
		}
	}
	for _, arg := range got {
		if arg == "--bwlimit=10m" {
			t.Fatal("non-selection copy option must not alter verification")
		}
		if strings.HasPrefix(arg, "--out-format") {
			t.Fatal("custom out-format reports unchanged files as verification differences")
		}
	}
}

func TestVerifyArgsNonRootOmitsPrivilegedMetadata(t *testing.T) {
	got := verifyArgs(nil, true)
	if got[0] != "-rlHtniO" {
		t.Fatalf("unexpected non-root verification policy: %#v", got)
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
