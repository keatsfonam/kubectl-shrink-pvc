package naming

import (
	"strings"
)

// SafeDNSLabel returns a Kubernetes DNS label-safe name capped at 63 characters.
func SafeDNSLabel(name string) string {
	name = strings.ToLower(strings.NewReplacer("_", "-", ".", "-").Replace(name))
	if len(name) <= 63 {
		return strings.Trim(name, "-")
	}
	return strings.Trim(name[:63], "-")
}
