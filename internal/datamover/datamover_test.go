package datamover

import (
	"reflect"
	"testing"
)

func TestJobDisablesServiceAccountToken(t *testing.T) {
	job := buildJob(Request{Namespace: "ns", SourcePVC: "source", DestPVC: "dest", Image: "image", RunAsUser: -1, FSGroup: -1}, "job", []string{"/src/", "/dest/"}, false, 0)
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
	job := buildJob(Request{Namespace: "ns", SourcePVC: "source", DestPVC: "dest", Image: "image"}, "job", args, false, 0)
	container := job.Spec.Template.Spec.Containers[0]
	if !reflect.DeepEqual(container.Command, []string{"rsync"}) {
		t.Fatalf("unexpected command: %#v", container.Command)
	}
	if !reflect.DeepEqual(container.Args, args) {
		t.Fatalf("unexpected args: %#v", container.Args)
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
