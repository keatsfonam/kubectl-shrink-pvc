package inspect

import "testing"

func TestInspectionPodDisablesServiceAccountToken(t *testing.T) {
	pod := buildInspectionPod(Options{Namespace: "ns", PVCName: "data", Image: "image", RunAsUser: -1, FSGroup: -1})
	if pod.Spec.AutomountServiceAccountToken == nil || *pod.Spec.AutomountServiceAccountToken {
		t.Fatal("inspection pod must disable service account token automount")
	}
}

func TestParseUsageBytes(t *testing.T) {
	tests := []struct {
		name string
		logs string
		want int64
	}{
		{name: "plain", logs: "12345\n", want: 12345},
		{name: "with spaces", logs: "  98765  \n", want: 98765},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseUsageBytes(tt.logs)
			if err != nil {
				t.Fatalf("ParseUsageBytes returned error: %v", err)
			}
			if got != tt.want {
				t.Fatalf("got %d, want %d", got, tt.want)
			}
		})
	}
}

func TestParseUsageBytesRejectsInvalid(t *testing.T) {
	for _, logs := range []string{"", "not-a-number", "-1"} {
		if _, err := ParseUsageBytes(logs); err == nil {
			t.Fatalf("expected error for %q", logs)
		}
	}
}
