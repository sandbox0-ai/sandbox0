//go:build !linux

package gvisorcli

import "context"

func startCheckpointWriteback(context.Context, string) (func() error, error) {
	return func() error { return nil }, nil
}
