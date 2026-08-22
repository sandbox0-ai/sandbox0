// Copyright 2026 Sandbox0 Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package gvisorcli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"

	"github.com/containerd/errdefs"
)

const (
	maxRunscOutputBytes = 2 << 20
	maxRunscStderrBytes = 64 << 10
)

// Runsc is the subset of the stock gVisor CLI used by the driver.
type Runsc interface {
	Create(ctx context.Context, bundleDir, containerID string) error
	Start(ctx context.Context, containerID string) error
	Wait(ctx context.Context, containerID string) (WaitResult, error)
	Kill(ctx context.Context, containerID, signal string) error
	Delete(ctx context.Context, containerID string, force bool) error
	State(ctx context.Context, containerID string) (RunscState, error)
	Stats(ctx context.Context, containerID string) (RunscStats, error)
	Version(ctx context.Context) (string, error)
}

// WaitResult is the JSON result emitted by `runsc wait`.
type WaitResult struct {
	ID         string `json:"id"`
	ExitStatus int    `json:"exitStatus"`
}

// RunscState is the subset of `runsc state` persisted by the driver.
type RunscState struct {
	ID     string `json:"id"`
	Status string `json:"status"`
	Bundle string `json:"bundle"`
	PID    int    `json:"pid"`
}

// Command shells out to an unmodified runsc binary.
type Config struct {
	Path       string
	Root       string
	Platform   string
	Overlay2   string
	FileAccess string
	DirectFS   bool
}

type Command struct {
	config Config
}

// New returns the production stock-runsc adapter.
func New(config Config) Runsc {
	return &Command{config: config}
}

func (r *Command) Create(ctx context.Context, bundleDir, containerID string) error {
	return r.run(ctx, "create", "--bundle", bundleDir, containerID)
}

func (r *Command) Start(ctx context.Context, containerID string) error {
	return r.run(ctx, "start", containerID)
}

func (r *Command) Wait(ctx context.Context, containerID string) (WaitResult, error) {
	var result WaitResult
	output, err := r.output(ctx, "wait", containerID)
	if err != nil {
		return result, err
	}
	if err := json.Unmarshal(output, &result); err != nil {
		return result, fmt.Errorf("decode runsc wait result: %w", err)
	}
	return result, nil
}

func (r *Command) Kill(ctx context.Context, containerID, signal string) error {
	if signal == "" {
		signal = "TERM"
	}
	return r.run(ctx, "kill", "--all", containerID, signal)
}

func (r *Command) Delete(ctx context.Context, containerID string, force bool) error {
	args := []string{"delete"}
	if force {
		args = append(args, "--force")
	}
	args = append(args, containerID)
	return r.run(ctx, args...)
}

func (r *Command) State(ctx context.Context, containerID string) (RunscState, error) {
	var state RunscState
	output, err := r.output(ctx, "state", containerID)
	if err != nil {
		return state, err
	}
	if err := json.Unmarshal(output, &state); err != nil {
		return state, fmt.Errorf("decode runsc state: %w", err)
	}
	return state, nil
}

// Stats returns one identity-checked stock `runsc events --stats` sample.
func (r *Command) Stats(ctx context.Context, containerID string) (RunscStats, error) {
	var result RunscStats
	output, err := r.output(ctx, "events", "--stats", containerID)
	if err != nil {
		return result, err
	}
	decoder := json.NewDecoder(bytes.NewReader(output))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&result); err != nil {
		return RunscStats{}, fmt.Errorf("decode runsc stats: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return RunscStats{}, fmt.Errorf("decode runsc stats: output must contain exactly one JSON value")
	}
	if err := result.Validate(containerID); err != nil {
		return RunscStats{}, fmt.Errorf("validate runsc stats: %w", err)
	}
	return result, nil
}

func (r *Command) Version(ctx context.Context) (string, error) {
	output, err := r.output(ctx, "--version")
	if err != nil {
		return "", err
	}
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	if len(lines) == 0 || lines[0] == "" {
		return "", fmt.Errorf("runsc at %s returned an empty version", r.config.Path)
	}
	return strings.TrimSpace(lines[0]), nil
}

func (r *Command) globalArgs() []string {
	return []string{
		"--root=" + r.config.Root,
		"--platform=" + r.config.Platform,
		"--overlay2=" + r.config.Overlay2,
		"--file-access=" + r.config.FileAccess,
		fmt.Sprintf("--directfs=%t", r.config.DirectFS),
	}
}

// GlobalArgs returns the immutable stock-runsc flags used for every command.
func (r *Command) GlobalArgs() []string {
	return append([]string(nil), r.globalArgs()...)
}

func (r *Command) command(ctx context.Context, args ...string) *exec.Cmd {
	return exec.CommandContext(ctx, r.config.Path, append(r.globalArgs(), args...)...)
}

func (r *Command) run(ctx context.Context, args ...string) error {
	// gVisor's create/start commands leave Sentry and Gofer children running.
	// Do not give them an exec.Cmd stdout/stderr pipe: the parent would block
	// until those long-lived children close the inherited pipe descriptors.
	cmd := r.command(ctx, args...)
	temp, err := os.CreateTemp("", "sandbox0-runsc-stderr-*")
	if err != nil {
		return fmt.Errorf("create runsc stderr file: %w", err)
	}
	tempName := temp.Name()
	defer os.Remove(tempName)
	cmd.Stdout = nil
	cmd.Stderr = temp
	err = cmd.Run()
	_, seekErr := temp.Seek(0, io.SeekStart)
	if seekErr != nil {
		_ = temp.Close()
		if err == nil {
			return seekErr
		}
		return err
	}
	stderr, readErr := io.ReadAll(io.LimitReader(temp, maxRunscStderrBytes+1))
	_ = temp.Close()
	if len(stderr) > maxRunscStderrBytes {
		return fmt.Errorf("runsc %s stderr exceeds %d bytes", args[0], maxRunscStderrBytes)
	}
	if err != nil {
		return classifyRunscError(args[0], err, string(stderr))
	}
	if readErr != nil {
		return readErr
	}
	return nil
}

func (r *Command) output(ctx context.Context, args ...string) ([]byte, error) {
	cmd := r.command(ctx, args...)
	stdout := boundedBuffer{limit: maxRunscOutputBytes}
	stderr := boundedBuffer{limit: maxRunscStderrBytes}
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	if stdout.exceeded {
		return nil, fmt.Errorf("runsc %s stdout exceeds %d bytes", args[0], maxRunscOutputBytes)
	}
	if stderr.exceeded {
		return nil, fmt.Errorf("runsc %s stderr exceeds %d bytes", args[0], maxRunscStderrBytes)
	}
	if err != nil {
		return nil, classifyRunscError(args[0], err, stderr.String())
	}
	return append([]byte(nil), stdout.Bytes()...), nil
}

type boundedBuffer struct {
	buffer   bytes.Buffer
	limit    int
	exceeded bool
}

func (b *boundedBuffer) Write(payload []byte) (int, error) {
	if b.limit < 0 {
		b.limit = 0
	}
	remaining := b.limit - b.buffer.Len()
	if remaining > 0 {
		written := len(payload)
		if written > remaining {
			written = remaining
		}
		_, _ = b.buffer.Write(payload[:written])
	}
	if len(payload) > remaining {
		b.exceeded = true
	}
	return len(payload), nil
}

func (b *boundedBuffer) Bytes() []byte  { return b.buffer.Bytes() }
func (b *boundedBuffer) String() string { return b.buffer.String() }
func (b *boundedBuffer) Len() int       { return b.buffer.Len() }

func classifyRunscError(operation string, commandErr error, stderr string) error {
	message := strings.TrimSpace(stderr)
	result := fmt.Errorf("runsc %s: %w: %s", operation, commandErr, message)
	lower := strings.ToLower(message)
	if strings.Contains(lower, "not found") || strings.Contains(lower, "does not exist") ||
		strings.Contains(lower, "no such container") {
		return errors.Join(result, errdefs.ErrNotFound)
	}
	return result
}
