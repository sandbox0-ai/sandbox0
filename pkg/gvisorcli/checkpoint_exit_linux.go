//go:build linux

package gvisorcli

import (
	"context"
	"errors"
	"fmt"

	"golang.org/x/sys/unix"
)

type checkpointPIDFD struct{ fd int }

func pinCheckpointProcess(pid int) (checkpointExitWaiter, error) {
	if pid <= 0 {
		return nil, fmt.Errorf("checkpoint process PID must be positive")
	}
	fd, err := unix.PidfdOpen(pid, 0)
	if err != nil {
		return nil, fmt.Errorf("open checkpoint process pidfd: %w", err)
	}
	return &checkpointPIDFD{fd: fd}, nil
}

func (p *checkpointPIDFD) Close() error { return unix.Close(p.fd) }

func (p *checkpointPIDFD) Wait(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		fds := []unix.PollFd{{Fd: int32(p.fd), Events: unix.POLLIN}}
		_, err := unix.Poll(fds, 25)
		if errors.Is(err, unix.EINTR) {
			continue
		}
		if err != nil {
			return err
		}
		if fds[0].Revents&(unix.POLLERR|unix.POLLNVAL) != 0 {
			return fmt.Errorf("checkpoint process pidfd became invalid")
		}
		if fds[0].Revents&(unix.POLLIN|unix.POLLHUP) != 0 {
			return ctx.Err()
		}
	}
}
