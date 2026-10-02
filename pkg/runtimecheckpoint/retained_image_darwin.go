//go:build darwin

package runtimecheckpoint

import (
	"fmt"
	"os"
	"path/filepath"
)

func linkRetainedFile(input *os.File, destination *os.Root, name string) error {
	if err := os.Link(input.Name(), filepath.Join(destination.Name(), name)); err != nil {
		return err
	}
	before, err := input.Stat()
	if err != nil {
		return err
	}
	after, err := destination.Lstat(name)
	if err != nil {
		return err
	}
	if !after.Mode().IsRegular() || !os.SameFile(before, after) {
		return fmt.Errorf("retained link changed source inode")
	}
	return nil
}
