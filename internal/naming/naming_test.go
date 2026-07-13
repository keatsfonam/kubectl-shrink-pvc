package naming

import (
	"strings"
	"testing"
)

func TestSafeDNSLabel(t *testing.T) {
	long := strings.Repeat("a", 70)
	tests := []struct {
		name string
		in   string
		want string
	}{
		{name: "lowercases and replaces separators", in: "Data_Volume.Backup", want: "data-volume-backup-bcd02e4984"},
		{name: "trims dashes", in: "-Data-", want: "data"},
		{name: "hashes long names", in: long, want: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-6bd5e50348"},
		{name: "hashes rather than trimming at a dash", in: strings.Repeat("a", 62) + "-suffix", want: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-23fd32c7b9"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := SafeDNSLabel(tt.in); got != tt.want {
				t.Fatalf("SafeDNSLabel(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}

func TestSafeDNSLabelNormalizationDoesNotCollide(t *testing.T) {
	withDot := SafeDNSLabel("data.backup")
	withDash := SafeDNSLabel("data-backup")
	if withDot == withDash {
		t.Fatalf("normalized generated names collided: %q", withDot)
	}
	if got := LegacySafeDNSLabel("data.backup"); got != withDash {
		t.Fatalf("legacy normalization changed: got %q, want %q", got, withDash)
	}
}

func TestSafeDNSLabelLongPrefixDoesNotCollide(t *testing.T) {
	prefix := strings.Repeat("a", 63)
	first := SafeDNSLabel(prefix + "-one")
	second := SafeDNSLabel(prefix + "-two")
	if first == second {
		t.Fatalf("long generated names collided: %q", first)
	}
	if len(first) > 63 || len(second) > 63 {
		t.Fatalf("generated names exceed DNS label limit: %q %q", first, second)
	}
	if got := LegacySafeDNSLabel(prefix + "-one"); got != prefix {
		t.Fatalf("unexpected legacy name %q", got)
	}
}
