package framework

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
)

// RunCommand executes a command and returns a detailed error on failure.
func RunCommand(ctx context.Context, name string, args ...string) error {
	fmt.Printf("Executing: %s %v\n", name, args)
	cmd := exec.CommandContext(ctx, name, args...)

	var output bytes.Buffer
	cmd.Stdout = &output
	cmd.Stderr = &output

	if err := cmd.Run(); err != nil {
		fmt.Printf("Command failed: %v\nOutput: %s\n", err, output.String())
		return fmt.Errorf("command failed: %s %v: %w\n%s", name, args, err, output.String())
	}

	return nil
}

// RunCommandOutput executes a command and returns stdout/stderr on success.
func RunCommandOutput(ctx context.Context, name string, args ...string) (string, error) {
	fmt.Printf("Executing: %s %v\n", name, args)
	cmd := exec.CommandContext(ctx, name, args...)

	var output bytes.Buffer
	cmd.Stdout = &output
	cmd.Stderr = &output

	if err := cmd.Run(); err != nil {
		return output.String(), fmt.Errorf("command failed: %s %v: %w\n%s", name, args, err, output.String())
	}

	return output.String(), nil
}
