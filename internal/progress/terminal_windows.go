//go:build windows

package progress

import (
	"syscall"
	"unsafe"
)

type consoleCoordinate struct {
	x int16
	y int16
}

type consoleRectangle struct {
	left   int16
	top    int16
	right  int16
	bottom int16
}

type consoleScreenBufferInfo struct {
	size              consoleCoordinate
	cursorPosition    consoleCoordinate
	attributes        uint16
	window            consoleRectangle
	maximumWindowSize consoleCoordinate
}

var getConsoleScreenBufferInfo = syscall.NewLazyDLL("kernel32.dll").NewProc("GetConsoleScreenBufferInfo")

func terminalWidth(fd uintptr) (int, bool) {
	handle := syscall.Handle(fd)
	var mode uint32
	if err := syscall.GetConsoleMode(handle, &mode); err != nil {
		return 0, false
	}

	var info consoleScreenBufferInfo
	result, _, _ := getConsoleScreenBufferInfo.Call(
		uintptr(handle),
		uintptr(unsafe.Pointer(&info)),
	)
	if result == 0 {
		return 0, true
	}
	return int(info.window.right-info.window.left) + 1, true
}
