//go:build !darwin && !linux && !windows

package progress

func terminalWidth(uintptr) (int, bool) {
	return 0, false
}
