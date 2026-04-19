//go:build !linux

package volume

import (
	"context"
	"fmt"
	"os/exec"
	"strings"
)

type execNamespaceOperator struct{}

func newNamespaceOperator() NamespaceOperator {
	return execNamespaceOperator{}
}

func (execNamespaceOperator) EnsureMountPoint(ctx context.Context, mountNSPath, mountPoint string) error {
	return runNamespaceCommand(ctx, mountNSPath, "mkdir", "-p", mountPoint)
}

func (execNamespaceOperator) BindMount(ctx context.Context, mountNSPath, sourcePath, mountPoint string) error {
	return runNamespaceCommand(ctx, mountNSPath, "mount", "--bind", sourcePath, mountPoint)
}

func (execNamespaceOperator) Unmount(ctx context.Context, mountNSPath, mountPoint string) error {
	return runNamespaceCommand(ctx, mountNSPath, "umount", mountPoint)
}

func runNamespaceCommand(ctx context.Context, mountNSPath string, args ...string) error {
	cmdArgs := append([]string{"--mount=" + mountNSPath, "--"}, args...)
	cmd := exec.CommandContext(ctx, "nsenter", cmdArgs...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("nsenter %s: %w: %s", strings.Join(cmdArgs, " "), err, strings.TrimSpace(string(output)))
	}
	return nil
}
