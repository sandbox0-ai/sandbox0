package s0fs

import "errors"

var (
	ErrClosed                = errors.New("s0fs engine is closed")
	ErrCommittedHeadConflict = errors.New("s0fs committed head conflict")
	ErrCommittedHeadNotFound = errors.New("s0fs committed head not found")
	ErrExists                = errors.New("entry already exists")
	ErrInvalidInput          = errors.New("invalid input")
	ErrIsDir                 = errors.New("inode is a directory")
	ErrNotEmpty              = errors.New("directory is not empty")
	ErrNotDir                = errors.New("inode is not a directory")
	ErrNotFound              = errors.New("entry not found")
)
