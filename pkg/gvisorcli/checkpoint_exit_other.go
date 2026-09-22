//go:build !linux

package gvisorcli

import "fmt"

func pinCheckpointProcess(int) (checkpointExitWaiter, error) {
	return nil, fmt.Errorf("physical checkpoint source exit requires Linux pidfd support")
}
