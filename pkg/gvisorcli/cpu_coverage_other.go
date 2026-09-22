//go:build !linux

package gvisorcli

import (
	"context"
	"fmt"

	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
)

func (r *Command) CPUCoverage(context.Context, string) (protocol.MigrationCPUObservation, error) {
	return protocol.MigrationCPUObservation{}, fmt.Errorf("CPU coverage requires a Linux host")
}
