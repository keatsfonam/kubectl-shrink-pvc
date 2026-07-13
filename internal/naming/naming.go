package naming

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
)

// SafeDNSLabel returns a Kubernetes DNS label-safe name capped at 63
// characters. Long names retain a stable hash suffix so distinct valid input
// names cannot collide merely because they share a long prefix.
func SafeDNSLabel(name string) string {
	normalized := normalize(name)
	canonicalInput := strings.Trim(strings.ToLower(name), "-")
	if len(normalized) <= 63 && normalized == canonicalInput {
		return normalized
	}
	sum := sha256.Sum256([]byte(canonicalInput))
	suffix := hex.EncodeToString(sum[:])[:10]
	maxPrefix := 63 - len(suffix) - 1
	if len(normalized) > maxPrefix {
		normalized = normalized[:maxPrefix]
	}
	return strings.Trim(normalized, "-") + "-" + suffix
}

// LegacySafeDNSLabel implements the pre-hash truncation scheme. It is kept
// only for locating recovery objects created by older releases.
func LegacySafeDNSLabel(name string) string {
	normalized := normalize(name)
	if len(normalized) <= 63 {
		return normalized
	}
	return strings.Trim(normalized[:63], "-")
}

func normalize(name string) string {
	name = strings.ToLower(strings.NewReplacer("_", "-", ".", "-").Replace(name))
	return strings.Trim(name, "-")
}
