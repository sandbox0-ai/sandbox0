package file

import "errors"

var (
	// ErrFileNotFound is returned when a file is not found.
	ErrFileNotFound = errors.New("file not found")

	// ErrDirNotFound is returned when a directory is not found.
	ErrDirNotFound = errors.New("directory not found")

	// ErrPathOutsideRoot is returned when the path is outside the root.
	ErrPathOutsideRoot = errors.New("path outside root")

	// ErrFileTooLarge is returned when a file is too large.
	ErrFileTooLarge = errors.New("file too large")

	// ErrPermissionDenied is returned when permission is denied.
	ErrPermissionDenied = errors.New("permission denied")

	// ErrWatcherNotFound is returned when a watcher is not found.
	ErrWatcherNotFound = errors.New("watcher not found")

	// ErrWatcherClosed is returned when the watcher manager is closed.
	ErrWatcherClosed = errors.New("watcher manager closed")
)
