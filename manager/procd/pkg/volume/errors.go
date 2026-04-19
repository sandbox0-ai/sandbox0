package volume

import "errors"

var (
	// ErrVolumeAlreadyMounted indicates the volume is already mounted.
	ErrVolumeAlreadyMounted = errors.New("volume already mounted")
	// ErrVolumeNotMounted indicates the volume is not mounted.
	ErrVolumeNotMounted = errors.New("volume not mounted")
	// ErrVolumeMountInProgress indicates a mount is currently in progress.
	ErrVolumeMountInProgress = errors.New("volume mount in progress")
	// ErrMountPointInUse indicates the mount point is already used by another volume.
	ErrMountPointInUse = errors.New("mount point already in use")
	// ErrInvalidMountPoint indicates the mount point is invalid.
	ErrInvalidMountPoint = errors.New("invalid mount point")
	// ErrMountSessionNotFound indicates the mount session is not found.
	ErrMountSessionNotFound = errors.New("mount session not found")
	// ErrNodeLocalMountUnavailable indicates node-local volume attach is not configured.
	ErrNodeLocalMountUnavailable = errors.New("node-local volume mount unavailable")
)
