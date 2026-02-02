package file

import "errors"

var (
	// ErrFileNotFound is returned when a file is not found.
	ErrFileNotFound = errors.New("file not found")

	// ErrDirNotFound is returned when a directory is not found.
	ErrDirNotFound = errors.New("directory not found")

	// ErrFileTooLarge is returned when a file is too large.
	ErrFileTooLarge = errors.New("file too large")

	// ErrPermissionDenied is returned when permission is denied.
	ErrPermissionDenied = errors.New("permission denied")

	// ErrWatcherNotFound is returned when a watcher is not found.
	ErrWatcherNotFound = errors.New("watcher not found")

	// ErrWatcherClosed is returned when the watcher manager is closed.
	ErrWatcherClosed = errors.New("watcher manager closed")

	// ErrPathAlreadyExists is returned when a path already exists.
	ErrPathAlreadyExists = errors.New("path already exists")

	// ErrPathNotDir is returned when a path exists but is not a directory.
	ErrPathNotDir = errors.New("path is not a directory")
)
