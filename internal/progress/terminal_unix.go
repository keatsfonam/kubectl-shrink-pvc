//go:build darwin || linux

package progress

import (
	"syscall"
	"unsafe"
)

type terminalWindowSize struct {
	row    uint16
	column uint16
	xpixel uint16
	ypixel uint16
}

func terminalWidth(fd uintptr) (int, bool) {
	var size terminalWindowSize
	_, _, errno := syscall.Syscall(
		syscall.SYS_IOCTL,
		fd,
		uintptr(syscall.TIOCGWINSZ),
		uintptr(unsafe.Pointer(&size)),
	)
	if errno != 0 {
		return 0, false
	}
	return int(size.column), true
}
