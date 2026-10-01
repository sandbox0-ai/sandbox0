//go:build !linux && !darwin

package runtimecheckpoint

import (
	"context"
	"fmt"
	"os"
)

func linkRetainedFile(*os.File, *os.Root, string) error {
	return fmt.Errorf("retained image links unavailable")
}
func lockRetainedImages(context.Context, *os.File) error {
	return fmt.Errorf("retained image locking unavailable")
}
func unlockRetainedImages(*os.File) {}
