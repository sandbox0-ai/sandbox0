package s0fs

import "errors"

var (
	ErrClosed       = errors.New("s0fs engine is closed")
	ErrExists       = errors.New("entry already exists")
	ErrInvalidInput = errors.New("invalid input")
	ErrIsDir        = errors.New("inode is a directory")
	ErrNotDir       = errors.New("inode is not a directory")
	ErrNotFound     = errors.New("entry not found")
)
