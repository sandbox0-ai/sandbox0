package gvisorcli

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// CheckpointRunsc is the execution-state transfer boundary of stock runsc.
// It is separate from Runsc so ordinary create/start paths do not silently
// acquire migration semantics. The regional migration transaction, not this
// adapter, owns source fencing, RootFS consistency and restore authorization.
type CheckpointRunsc interface {
	Runsc
	Checkpoint(context.Context, string, string) error
	Restore(context.Context, string, string) error
}

var _ CheckpointRunsc = (*Command)(nil)

// Checkpoint saves execution state and stops the source guest. imagePath must
// be a new directory below a trusted, host-only parent. A failed or interrupted
// checkpoint is deliberately retained: its existence never proves completion,
// and retrying into the same directory could mix two inconsistent images.
//
// There is intentionally no leave-running option. Once the source has been
// checkpointed, only the regional transaction can authorize further execution.
func (r *Command) Checkpoint(ctx context.Context, containerID, imagePath string) error {
	if err := validateCheckpointArguments(ctx, containerID, imagePath); err != nil {
		return err
	}
	if err := os.Mkdir(imagePath, 0o700); err != nil {
		return fmt.Errorf("reserve runsc checkpoint directory: %w", err)
	}
	if err := r.run(ctx, "checkpoint", "--image-path="+imagePath,
		"--compression=none", containerID); err != nil {
		return err
	}
	// Stock runsc may return the successful non-resuming save RPC before its
	// sentry exits and OCI state observes that exit. Do not expose completed
	// capture custody to the filesystem sealer during this interval. Only an
	// exact stopped observation is success; never kill or resume to repair it.
	exitCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	for {
		state, err := r.State(exitCtx, containerID)
		if err != nil {
			return fmt.Errorf("observe checkpoint source exit: %w", err)
		}
		if state.ID != containerID {
			return fmt.Errorf("checkpoint source exit changed container identity")
		}
		if state.Status == "stopped" {
			return exitCtx.Err()
		}
		if state.Status != "running" {
			return fmt.Errorf("checkpoint source exit has unexpected state %q", state.Status)
		}
		timer := time.NewTimer(25 * time.Millisecond)
		select {
		case <-exitCtx.Done():
			timer.Stop()
			return fmt.Errorf("wait for checkpoint source exit: %w", exitCtx.Err())
		case <-timer.C:
		}
	}
}

// Restore resumes execution in an already-created destination container.
// The caller must verify the complete immutable checkpoint and establish sole
// execution/writer ownership before calling this method. Restore never falls
// back to Start: that would silently discard the workload's process state.
//
// Detached foreground loading returns after the full image has loaded rather
// than after the workload exits. Background loading requires separate custody
// of image files and is not enabled by this adapter.
func (r *Command) Restore(ctx context.Context, containerID, imagePath string) error {
	if err := validateCheckpointArguments(ctx, containerID, imagePath); err != nil {
		return err
	}
	info, err := os.Lstat(imagePath)
	if err != nil {
		return fmt.Errorf("inspect runsc checkpoint directory: %w", err)
	}
	if !info.IsDir() || info.Mode().Perm()&0o077 != 0 {
		return fmt.Errorf("runsc checkpoint must be a private directory, not a symlink")
	}
	state, err := r.State(ctx, containerID)
	if err != nil {
		return fmt.Errorf("inspect runsc restore destination: %w", err)
	}
	if state.ID != containerID || state.Status != "created" {
		return fmt.Errorf("runsc restore requires the exact created destination")
	}
	return r.run(ctx, "restore", "--image-path="+imagePath, "--detach", containerID)
}

func validateCheckpointArguments(ctx context.Context, containerID, imagePath string) error {
	if ctx == nil {
		return fmt.Errorf("checkpoint context is required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if containerID == "" || len(containerID) > 512 || strings.HasPrefix(containerID, "-") {
		return fmt.Errorf("checkpoint container identity is invalid")
	}
	for _, c := range containerID {
		if (c < 'a' || c > 'z') && (c < 'A' || c > 'Z') &&
			(c < '0' || c > '9') && c != '_' && c != '-' && c != '.' {
			return fmt.Errorf("checkpoint container identity is invalid")
		}
	}
	if !filepath.IsAbs(imagePath) || filepath.Clean(imagePath) != imagePath ||
		imagePath == string(filepath.Separator) || strings.ContainsRune(imagePath, '\x00') {
		return fmt.Errorf("checkpoint image path must be an absolute canonical directory")
	}
	return nil
}
