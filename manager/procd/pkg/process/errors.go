package process

import "errors"

var (
	// ErrProcessNotFound is returned when a process is not found.
	ErrProcessNotFound = errors.New("process not found")

	// ErrProcessNotRunning is returned when trying to interact with a stopped process.
	ErrProcessNotRunning = errors.New("process not running")

	// ErrProcessAlreadyRunning is returned when trying to start an already running process.
	ErrProcessAlreadyRunning = errors.New("process already running")

	// ErrProcessNotPaused is returned when trying to resume a process that is not paused.
	ErrProcessNotPaused = errors.New("process not paused")

	// ErrProcessAlreadyPaused is returned when trying to pause an already paused process.
	ErrProcessAlreadyPaused = errors.New("process already paused")

	// ErrPauseFailed is returned when pause operation fails.
	ErrPauseFailed = errors.New("pause failed")

	// ErrResumeFailed is returned when resume operation fails.
	ErrResumeFailed = errors.New("resume failed")

	// ErrUnsupportedProcessType is returned for unknown process types.
	ErrUnsupportedProcessType = errors.New("unsupported process type")

	// ErrUnsupportedLanguage is returned for unknown languages.
	ErrUnsupportedLanguage = errors.New("unsupported language")

	// ErrInvalidCommand is returned for invalid commands.
	ErrInvalidCommand = errors.New("invalid command")

	// ErrProcessStartFailed is returned when process fails to start.
	ErrProcessStartFailed = errors.New("process start failed")

	// ErrProcessKilled is returned when process is killed.
	ErrProcessKilled = errors.New("process killed")

	// ErrProcessCrashed is returned when process crashes.
	ErrProcessCrashed = errors.New("process crashed")

	// ErrPermissionDenied is returned when permission is denied.
	ErrPermissionDenied = errors.New("permission denied")
)
