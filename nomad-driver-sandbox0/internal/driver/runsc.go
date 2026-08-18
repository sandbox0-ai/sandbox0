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

package driver

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"strings"
)

// Runsc is the subset of the stock gVisor CLI used by the driver.
type Runsc interface {
	Create(ctx context.Context, bundleDir, containerID string) error
	Start(ctx context.Context, containerID string) error
	Wait(ctx context.Context, containerID string) (WaitResult, error)
	Kill(ctx context.Context, containerID, signal string) error
	Delete(ctx context.Context, containerID string, force bool) error
	State(ctx context.Context, containerID string) (RunscState, error)
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

// CommandRunsc shells out to an unmodified runsc binary.
type CommandRunsc struct {
	config PluginConfig
}

// NewCommandRunsc returns the production stock-runsc adapter.
func NewCommandRunsc(config PluginConfig) Runsc {
	return &CommandRunsc{config: config}
}

func (r *CommandRunsc) Create(ctx context.Context, bundleDir, containerID string) error {
	return r.run(ctx, "create", "--bundle", bundleDir, containerID)
}

func (r *CommandRunsc) Start(ctx context.Context, containerID string) error {
	return r.run(ctx, "start", containerID)
}

func (r *CommandRunsc) Wait(ctx context.Context, containerID string) (WaitResult, error) {
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

func (r *CommandRunsc) Kill(ctx context.Context, containerID, signal string) error {
	if signal == "" {
		signal = "TERM"
	}
	return r.run(ctx, "kill", "--all", containerID, signal)
}

func (r *CommandRunsc) Delete(ctx context.Context, containerID string, force bool) error {
	args := []string{"delete"}
	if force {
		args = append(args, "--force")
	}
	args = append(args, containerID)
	return r.run(ctx, args...)
}

func (r *CommandRunsc) State(ctx context.Context, containerID string) (RunscState, error) {
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

func (r *CommandRunsc) Version(ctx context.Context) (string, error) {
	output, err := r.output(ctx, "--version")
	if err != nil {
		return "", err
	}
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	if len(lines) == 0 || lines[0] == "" {
		return "", fmt.Errorf("runsc at %s returned an empty version", r.config.RunscPath)
	}
	return strings.TrimSpace(lines[0]), nil
}

func (r *CommandRunsc) globalArgs() []string {
	return []string{
		"--root=" + r.config.RunscRoot,
		"--platform=" + r.config.Platform,
		"--overlay2=" + r.config.Overlay2,
		"--file-access=" + r.config.FileAccess,
		fmt.Sprintf("--directfs=%t", r.config.DirectFS),
	}
}

func (r *CommandRunsc) command(ctx context.Context, args ...string) *exec.Cmd {
	return exec.CommandContext(ctx, r.config.RunscPath, append(r.globalArgs(), args...)...)
}

func (r *CommandRunsc) run(ctx context.Context, args ...string) error {
	_, err := r.output(ctx, args...)
	return err
}

func (r *CommandRunsc) output(ctx context.Context, args ...string) ([]byte, error) {
	cmd := r.command(ctx, args...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	output, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("runsc %s: %w: %s", args[0], err, strings.TrimSpace(stderr.String()))
	}
	return output, nil
}
