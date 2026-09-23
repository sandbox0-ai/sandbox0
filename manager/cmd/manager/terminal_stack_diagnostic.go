package main

import (
	"context"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"syscall"

	"go.uber.org/zap"
)

const maxTerminalStackBytes = 16 << 20

// SIGUSR2 records only function names from the terminal worker stack. It
// never logs goroutine arguments, which may contain tenant data or secrets.
func startTerminalStackDiagnostic(ctx context.Context, logger *zap.Logger) {
	if logger == nil {
		return
	}
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGUSR2)
	go func() {
		defer signal.Stop(signals)
		for {
			select {
			case <-ctx.Done():
				return
			case <-signals:
				frames, complete := terminalWorkerFrames()
				logger.Info("Terminal worker stack diagnostic",
					zap.Strings("frames", frames), zap.Bool("stack_complete", complete))
			}
		}
	}()
}

func terminalWorkerFrames() ([]string, bool) {
	for size := 1 << 20; size <= maxTerminalStackBytes; size *= 2 {
		buffer := make([]byte, size)
		n := runtime.Stack(buffer, true)
		frames := parseTerminalWorkerFrames(string(buffer[:n]))
		if n < size {
			return frames, true
		}
	}
	return nil, false
}

func parseTerminalWorkerFrames(stacks string) []string {
	for _, goroutine := range strings.Split(stacks, "\n\n") {
		if !strings.Contains(goroutine, "runtimeslotreconciler.(*Worker).Run(") {
			continue
		}
		frames := make([]string, 0, 16)
		for _, line := range strings.Split(goroutine, "\n") {
			if line == "" || strings.HasPrefix(line, "goroutine ") ||
				strings.HasPrefix(line, "\t") || strings.HasPrefix(line, "created by ") {
				continue
			}
			if index := strings.LastIndexByte(line, '('); index >= 0 {
				line = line[:index]
			}
			frames = append(frames, strings.TrimSpace(line))
			if len(frames) == 16 {
				break
			}
		}
		return frames
	}
	return nil
}
