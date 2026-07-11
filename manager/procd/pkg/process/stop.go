package process

import (
	"errors"
	"syscall"
	"time"
)

const (
	defaultStopGracePeriod = 5 * time.Second
	defaultStopKillWait    = 2 * time.Second
)

// StopOptions controls graceful process termination and forced-kill waiting.
type StopOptions struct {
	GracePeriod time.Duration
	KillWait    time.Duration
}

func (o StopOptions) normalized() StopOptions {
	if o.GracePeriod <= 0 {
		o.GracePeriod = defaultStopGracePeriod
	}
	if o.KillWait <= 0 {
		o.KillWait = defaultStopKillWait
	}
	return o
}

func terminateProcess(base *BaseProcess, onStop func(), options StopOptions) error {
	options = options.normalized()
	if onStop != nil {
		defer onStop()
	}

	switch base.State() {
	case ProcessStateCreated, ProcessStateStopped, ProcessStateKilled, ProcessStateCrashed:
		if !processGroupExists(base.PID()) {
			return nil
		}
	case ProcessStatePaused:
		_ = signalProcessGroup(base.PID(), syscall.SIGCONT)
	}

	pid := base.PID()
	termErr := signalProcessGroup(pid, syscall.SIGTERM)
	if waitForProcessTreeFinish(base, pid, options.GracePeriod) {
		return nil
	}

	killErr := signalProcessGroup(pid, syscall.SIGKILL)
	if waitForProcessTreeFinish(base, pid, options.KillWait) {
		return nil
	}
	return errors.Join(ErrProcessStopTimeout, termErr, killErr)
}

func waitForProcessTreeFinish(base *BaseProcess, pid int, timeout time.Duration) bool {
	if base.IsFinished() && !processGroupExists(pid) {
		return true
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-timer.C:
			return base.IsFinished() && !processGroupExists(pid)
		case <-ticker.C:
			if base.IsFinished() && !processGroupExists(pid) {
				return true
			}
		}
	}
}

func signalProcessGroup(pid int, signal syscall.Signal) error {
	if pid <= 0 {
		return ErrProcessNotRunning
	}
	if err := syscall.Kill(-pid, signal); err == nil {
		return nil
	}
	if err := syscall.Kill(pid, signal); err != nil {
		return err
	}
	return nil
}

func processGroupExists(pid int) bool {
	if pid <= 0 {
		return false
	}
	err := syscall.Kill(-pid, 0)
	return err == nil || errors.Is(err, syscall.EPERM)
}
