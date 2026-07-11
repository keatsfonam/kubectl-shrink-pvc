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
		{name: "lowercases and replaces separators", in: "Data_Volume.Backup", want: "data-volume-backup"},
		{name: "trims dashes", in: "-Data-", want: "data"},
		{name: "truncates", in: long, want: strings.Repeat("a", 63)},
		{name: "trims trailing dash after truncation", in: strings.Repeat("a", 62) + "-suffix", want: strings.Repeat("a", 62)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := SafeDNSLabel(tt.in); got != tt.want {
				t.Fatalf("SafeDNSLabel(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}
