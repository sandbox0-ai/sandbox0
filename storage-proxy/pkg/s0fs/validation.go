package s0fs

import (
	"fmt"
	"strings"
)

const (
	// MaxNameBytes matches the name length advertised by StatFs.
	MaxNameBytes = 255
	// MaxSymlinkTargetBytes bounds one WAL and manifest record.
	MaxSymlinkTargetBytes = 4096
)

func validateName(name string) error {
	switch {
	case name == "":
		return fmt.Errorf("%w: empty name", ErrInvalidInput)
	case len(name) > MaxNameBytes:
		return fmt.Errorf("%w: name exceeds %d bytes", ErrInvalidInput, MaxNameBytes)
	case strings.ContainsRune(name, '\x00'):
		return fmt.Errorf("%w: name contains NUL", ErrInvalidInput)
	case strings.ContainsRune(name, '/'):
		return fmt.Errorf("%w: name contains slash", ErrInvalidInput)
	default:
		return nil
	}
}

func validateSymlinkTarget(target string) error {
	switch {
	case target == "":
		return fmt.Errorf("%w: symlink target is required", ErrInvalidInput)
	case len(target) > MaxSymlinkTargetBytes:
		return fmt.Errorf("%w: symlink target exceeds %d bytes", ErrInvalidInput, MaxSymlinkTargetBytes)
	case strings.ContainsRune(target, '\x00'):
		return fmt.Errorf("%w: symlink target contains NUL", ErrInvalidInput)
	default:
		return nil
	}
}
