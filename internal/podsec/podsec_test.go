package podsec

import "testing"

func TestPodDefaultsToImageUser(t *testing.T) {
	sc := Pod(-1, -1)
	if sc.RunAsNonRoot != nil || sc.RunAsUser != nil || sc.FSGroup != nil {
		t.Fatalf("expected no user constraints in root mode, got %#v", sc)
	}
	if sc.SeccompProfile == nil {
		t.Fatal("expected seccomp profile to always be set")
	}
}

func TestPodNonRoot(t *testing.T) {
	sc := Pod(1000, 2000)
	if sc.RunAsNonRoot == nil || !*sc.RunAsNonRoot || *sc.RunAsUser != 1000 || *sc.FSGroup != 2000 {
		t.Fatalf("unexpected non-root context: %#v", sc)
	}
}

func TestContainerCapabilities(t *testing.T) {
	root := Container(false, "CHOWN")
	if len(root.Capabilities.Add) != 1 || root.Capabilities.Add[0] != "CHOWN" {
		t.Fatalf("expected CHOWN added in root mode, got %#v", root.Capabilities)
	}
	nonRoot := Container(true, "CHOWN")
	if len(nonRoot.Capabilities.Add) != 0 {
		t.Fatalf("expected no added capabilities in non-root mode, got %#v", nonRoot.Capabilities)
	}
}
