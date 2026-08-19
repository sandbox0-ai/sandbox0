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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/nomad/plugins/drivers"

	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
)

type slotPhase string

const (
	phaseWarm     slotPhase = "warm"
	phaseClaiming slotPhase = "claiming"
	phaseActive   slotPhase = "active"
	phaseStopping slotPhase = "stopping"
	phaseExited   slotPhase = "exited"
	phasePoisoned slotPhase = "poisoned"
)

// ClaimRequest is the one-shot authorization sent by manager after RootFS and network handoff.
type ClaimRequest struct {
	RootfsPath  string                      `json:"rootfs_path"`
	PolicyToken string                      `json:"policy_token"`
	WriterEpoch string                      `json:"writer_epoch"`
	Stage       *rootfshandoff.StageRequest `json:"stage,omitempty"`
}

type claimMetadata struct {
	RootfsPath  string                      `json:"rootfs_path"`
	WriterEpoch string                      `json:"writer_epoch"`
	Stage       *rootfshandoff.StageRequest `json:"stage,omitempty"`
}

// PersistedState carries enough identity to recover a runsc task without reusing its claim token.
type PersistedState struct {
	TaskConfig  *drivers.TaskConfig `json:"task_config"`
	ContainerID string              `json:"container_id"`
	BundleDir   string              `json:"bundle_dir"`
	RootMount   string              `json:"root_mount"`
	StartedAt   time.Time           `json:"started_at"`
	Phase       slotPhase           `json:"phase"`
	RootMounted bool                `json:"root_mounted"`
	Claim       *claimMetadata      `json:"claim,omitempty"`
}

type taskHandleOptions struct {
	taskConfig        *drivers.TaskConfig
	driverConfig      TaskConfig
	bundleDir         string
	containerID       string
	rootMount         string
	socketPath        string
	runner            Runsc
	mounter           Mounter
	allowedRoot       string
	rootfsAllowedRoot string
	rootfs            RootFSRuntime
	logger            hclog.Logger
}

type taskHandle struct {
	mu sync.Mutex

	taskConfig        *drivers.TaskConfig
	driverConfig      TaskConfig
	bundleDir         string
	containerID       string
	rootMount         string
	socketPath        string
	allowedRoot       string
	runner            Runsc
	mounter           Mounter
	rootfsAllowedRoot string
	rootfs            RootFSRuntime
	logger            hclog.Logger

	phase       slotPhase
	startedAt   time.Time
	completedAt time.Time
	exitResult  *drivers.ExitResult
	rootMounted bool
	claim       *claimMetadata
	stage       *rootfshandoff.StageRequest
	closed      bool

	done chan struct{}

	controlOnce   sync.Once
	controlServer *http.Server
}

func (h *taskHandle) statePath() string {
	return filepath.Join(h.bundleDir, ".sandbox0-driver-state.json")
}

func (h *taskHandle) persistedLocked() PersistedState {
	return PersistedState{
		TaskConfig:  h.taskConfig,
		ContainerID: h.containerID,
		BundleDir:   h.bundleDir,
		RootMount:   h.rootMount,
		StartedAt:   h.startedAt,
		Phase:       h.phase,
		RootMounted: h.rootMounted,
		Claim:       h.claim,
	}
}

func (h *taskHandle) persist() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	return writePersistedState(h.persistedLocked(), h.statePath())
}

func writePersistedState(state PersistedState, path string) error {
	data, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("marshal driver state: %w", err)
	}
	temp, err := os.CreateTemp(filepath.Dir(path), ".sandbox0-driver-state-*")
	if err != nil {
		return fmt.Errorf("create driver state file: %w", err)
	}
	tempName := temp.Name()
	defer os.Remove(tempName)
	if err := temp.Chmod(0o600); err != nil {
		_ = temp.Close()
		return fmt.Errorf("secure driver state: %w", err)
	}
	if _, err := temp.Write(data); err != nil {
		_ = temp.Close()
		return fmt.Errorf("write driver state: %w", err)
	}
	if err := temp.Sync(); err != nil {
		_ = temp.Close()
		return fmt.Errorf("sync driver state: %w", err)
	}
	if err := temp.Close(); err != nil {
		return fmt.Errorf("close driver state: %w", err)
	}
	if err := os.Rename(tempName, path); err != nil {
		return fmt.Errorf("replace driver state: %w", err)
	}
	return nil
}

func readPersistedState(path string) (PersistedState, error) {
	var state PersistedState
	data, err := os.ReadFile(path)
	if err != nil {
		return state, fmt.Errorf("read driver state: %w", err)
	}
	if err := json.Unmarshal(data, &state); err != nil {
		return state, fmt.Errorf("decode driver state: %w", err)
	}
	return state, nil
}

func newTaskHandle(options taskHandleOptions) *taskHandle {
	return &taskHandle{
		taskConfig:        options.taskConfig,
		bundleDir:         options.bundleDir,
		containerID:       options.containerID,
		rootMount:         options.rootMount,
		socketPath:        options.socketPath,
		allowedRoot:       options.allowedRoot,
		runner:            options.runner,
		mounter:           options.mounter,
		rootfsAllowedRoot: options.rootfsAllowedRoot,
		rootfs:            options.rootfs,
		logger:            options.logger,
		phase:             phaseWarm,
		done:              make(chan struct{}),
	}
}

// Prepare creates a generic warm allocation without creating a gVisor container.
func (h *taskHandle) Prepare(config TaskConfig) error {
	prepared := false
	defer func() {
		if !prepared {
			_ = h.Close(false)
		}
	}()
	h.mu.Lock()
	h.driverConfig = config
	h.phase = phaseWarm
	h.startedAt = time.Now().Round(time.Millisecond)
	h.mu.Unlock()

	if err := os.MkdirAll(filepath.Dir(h.socketPath), 0o750); err != nil {
		return fmt.Errorf("create control directory: %w", err)
	}
	if err := os.MkdirAll(h.rootMount, 0o755); err != nil {
		return fmt.Errorf("create OCI rootfs mountpoint: %w", err)
	}
	if err := h.persist(); err != nil {
		_ = h.Close(false)
		return err
	}
	prepared = true
	return nil
}

func (h *taskHandle) writeClaimBundle() error {
	netnsPath := ""
	if h.taskConfig.NetworkIsolation != nil {
		netnsPath = h.taskConfig.NetworkIsolation.Path
		if netnsPath != "" && !filepath.IsAbs(netnsPath) {
			return errors.New("Nomad network namespace path must be absolute")
		}
	}
	var resources *driversResources
	if h.taskConfig.Resources != nil && h.taskConfig.Resources.LinuxResources != nil {
		linux := h.taskConfig.Resources.LinuxResources
		resources = &driversResources{
			CPUPeriod:        linux.CPUPeriod,
			CPUQuota:         linux.CPUQuota,
			CPUShares:        linux.CPUShares,
			MemoryLimitBytes: linux.MemoryLimitBytes,
		}
	}
	h.mu.Lock()
	command := h.driverConfig.Command
	args := h.driverConfig.Args
	h.mu.Unlock()
	spec := buildSpec(specOptions{
		Command:   command,
		Args:      args,
		AllocID:   h.taskConfig.AllocID,
		TaskID:    h.taskConfig.ID,
		NetNSPath: netnsPath,
		Resources: resources,
	})
	if err := writeBundle(h.bundleDir, spec); err != nil {
		return err
	}
	return nil
}

// Claim writes the OCI bundle, attaches D as its initial root, then creates and starts runsc.
func (h *taskHandle) Claim(request ClaimRequest) error {
	h.mu.Lock()
	if h.phase != phaseWarm {
		err := fmt.Errorf("claim is only valid in warm phase, current phase %s", h.phase)
		h.mu.Unlock()
		return err
	}
	h.phase = phaseClaiming
	h.mu.Unlock()

	if request.PolicyToken == "" || request.WriterEpoch == "" {
		h.setPhase(phaseWarm)
		return errors.New("policy token and writer epoch are required")
	}
	rootfsSource := request.RootfsPath
	allowedRoot := h.allowedRoot
	var durableStage *rootfshandoff.StageRequest
	sessionAttached := false
	claimSucceeded := false
	defer func() {
		if durableStage == nil || !sessionAttached || claimSucceeded || h.rootfs == nil {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		if _, err := h.rootfs.Retire(ctx, *durableStage, newRetireOperationID()); err != nil {
			h.logger.Error("failed RootFS abort after claim failure", "error", err)
		}
	}()
	if request.Stage != nil {
		if h.rootfs == nil {
			h.setPhase(phaseWarm)
			return errors.New("RootFS runtime is not enabled")
		}
		if err := request.Stage.Validate(); err != nil {
			h.setPhase(phaseWarm)
			return fmt.Errorf("validate RootFS stage: %w", err)
		}
		if request.PolicyToken != request.Stage.Identity.WriterGrantToken ||
			request.WriterEpoch != fmt.Sprintf("%d", request.Stage.Identity.WriterEpoch) {
			h.setPhase(phaseWarm)
			return errors.New("claim credentials do not match RootFS stage")
		}
		if request.Stage.Identity.PodUID != h.taskConfig.AllocID ||
			request.Stage.Identity.ContainerName != h.taskConfig.Name ||
			request.Stage.Identity.RuntimeName != PluginName ||
			request.Stage.Identity.Snapshotter != "nomad-driver" {
			h.setPhase(phaseWarm)
			return errors.New("RootFS stage is not bound to this Nomad allocation")
		}
		if request.RootfsPath != "" {
			h.setPhase(phaseWarm)
			return errors.New("RootFS stage and development rootfs_path are mutually exclusive")
		}
		durable := request.Stage.WithoutWriterGrantToken()
		durableStage = &durable
		allowedRoot = h.rootfsAllowedRoot
	}
	if err := h.writeClaimBundle(); err != nil {
		h.setPhase(phaseWarm)
		return err
	}
	h.mu.Lock()
	h.rootMounted = true
	h.phase = phaseClaiming
	h.mu.Unlock()
	if err := h.persist(); err != nil {
		_ = h.rollbackClaim()
		return fmt.Errorf("persist claiming state: %w", err)
	}

	if durableStage != nil {
		mount, err := h.rootfs.Ensure(context.Background(), *request.Stage)
		if err != nil {
			_ = h.rollbackClaim()
			return fmt.Errorf("attach RootFS session: %w", err)
		}
		rootfsSource = mount.Source
		sessionAttached = true
	}
	resolvedRootfs, err := validateRootfsPath(rootfsSource, allowedRoot)
	if err != nil {
		_ = h.rollbackClaim()
		return err
	}

	if err := h.mounter.Bind(resolvedRootfs, h.rootMount); err != nil {
		_ = h.rollbackClaim()
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := h.runner.Create(ctx, h.bundleDir, h.containerID); err != nil {
		_ = h.mounter.Unmount(h.rootMount)
		_ = h.runner.Delete(context.Background(), h.containerID, true)
		_ = h.markClaimFailed(fmt.Errorf("runsc create: %w", err))
		return fmt.Errorf("runsc create: %w", err)
	}
	cancel()
	startCtx, startCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer startCancel()
	if err := h.runner.Start(startCtx, h.containerID); err != nil {
		_ = h.mounter.Unmount(h.rootMount)
		_ = h.runner.Delete(context.Background(), h.containerID, true)
		_ = h.markClaimFailed(fmt.Errorf("runsc start: %w", err))
		_ = h.persist()
		return fmt.Errorf("runsc start: %w", err)
	}

	h.mu.Lock()
	h.phase = phaseActive
	h.claim = &claimMetadata{RootfsPath: resolvedRootfs, WriterEpoch: request.WriterEpoch, Stage: durableStage}
	h.stage = durableStage
	h.mu.Unlock()
	if err := h.persist(); err != nil {
		_ = h.runner.Kill(context.Background(), h.containerID, "KILL")
		_ = h.mounter.Unmount(h.rootMount)
		_ = h.runner.Delete(context.Background(), h.containerID, true)
		h.mu.Lock()
		h.rootMounted = false
		h.phase = phasePoisoned
		h.exitResult = &drivers.ExitResult{Err: fmt.Errorf("persist active state: %w", err)}
		if h.completedAt.IsZero() {
			h.completedAt = time.Now()
		}
		closeDoneLocked(h.done)
		h.mu.Unlock()
		_ = h.persist()
		return fmt.Errorf("persist active state: %w", err)
	}
	claimSucceeded = true
	go h.waitForExit()
	return nil
}

func (h *taskHandle) rollbackClaim() error {
	_ = h.mounter.Unmount(h.rootMount)
	h.mu.Lock()
	h.rootMounted = false
	h.phase = phaseWarm
	h.mu.Unlock()
	return h.persist()
}

func (h *taskHandle) markClaimFailed(err error) error {
	h.mu.Lock()
	h.rootMounted = false
	h.phase = phasePoisoned
	h.exitResult = &drivers.ExitResult{Err: err}
	if h.completedAt.IsZero() {
		h.completedAt = time.Now()
	}
	closeDoneLocked(h.done)
	h.mu.Unlock()
	return h.persist()
}

func (h *taskHandle) waitForExit() {
	result, err := h.runner.Wait(context.Background(), h.containerID)
	h.mu.Lock()
	if h.closed || h.phase == phaseExited || h.phase == phasePoisoned {
		h.mu.Unlock()
		return
	}
	if err != nil {
		h.phase = phasePoisoned
		h.exitResult = &drivers.ExitResult{Err: fmt.Errorf("runsc wait: %w", err)}
	} else {
		h.phase = phaseExited
		h.exitResult = &drivers.ExitResult{ExitCode: result.ExitStatus}
	}
	if h.completedAt.IsZero() {
		h.completedAt = time.Now()
	}
	closeDoneLocked(h.done)
	h.mu.Unlock()
	_ = h.persist()
}

func closeDoneLocked(done chan struct{}) {
	select {
	case <-done:
	default:
		close(done)
	}
}

// WaitChannel delivers one exit result to each Nomad waiter.
func (h *taskHandle) WaitChannel(ctx context.Context) <-chan *drivers.ExitResult {
	channel := make(chan *drivers.ExitResult, 1)
	go func() {
		defer close(channel)
		select {
		case <-ctx.Done():
			return
		case <-h.done:
			h.mu.Lock()
			result := h.exitResult
			if result == nil {
				result = &drivers.ExitResult{}
			}
			h.mu.Unlock()
			select {
			case channel <- result:
			case <-ctx.Done():
			}
		}
	}()
	return channel
}

// Stop terminates a warm or active one-shot container.
func (h *taskHandle) Stop(timeout time.Duration, signal string) error {
	if timeout < 0 {
		timeout = 0
	}
	h.mu.Lock()
	phase := h.phase
	h.mu.Unlock()
	if phase == phaseExited || phase == phasePoisoned || phase == phaseStopping {
		return nil
	}
	if signal == "" {
		signal = "TERM"
	}

	h.setPhase(phaseStopping)
	if phase == phaseActive {
		if err := h.runner.Kill(context.Background(), h.containerID, signal); err != nil {
			h.logger.Warn("runsc kill failed", "error", err)
		}
		select {
		case <-h.done:
		case <-time.After(timeout):
			forceCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			_ = h.runner.Delete(forceCtx, h.containerID, true)
			cancel()
		}
	} else {
		forceCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		_ = h.runner.Delete(forceCtx, h.containerID, true)
		cancel()
	}

	h.mu.Lock()
	h.phase = phaseExited
	if h.exitResult == nil {
		h.exitResult = &drivers.ExitResult{}
	}
	if h.completedAt.IsZero() {
		h.completedAt = time.Now()
	}
	closeDoneLocked(h.done)
	h.mu.Unlock()
	_ = h.persist()
	h.stopControl()
	return nil
}

// Signal forwards a signal to an active container only.
func (h *taskHandle) Signal(signal string) error {
	h.mu.Lock()
	phase := h.phase
	h.mu.Unlock()
	if phase != phaseActive {
		return fmt.Errorf("cannot signal slot in %s phase", phase)
	}
	if signal == "" {
		signal = "TERM"
	}
	return h.runner.Kill(context.Background(), h.containerID, signal)
}

// Close unmounts D, deletes runsc state, and removes the private bundle.
func (h *taskHandle) Close(force bool) error {
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return nil
	}
	h.closed = true
	rootMounted := h.rootMounted
	stage := h.stage
	if stage == nil && h.claim != nil {
		stage = h.claim.Stage
	}
	h.mu.Unlock()

	h.stopControl()
	cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if force {
		_ = h.runner.Kill(cleanupCtx, h.containerID, "KILL")
	}
	firstErr := h.runner.Delete(cleanupCtx, h.containerID, true)
	if rootMounted {
		if err := h.mounter.Unmount(h.rootMount); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if stage != nil && rootMounted && h.rootfs != nil {
		retireCtx, retireCancel := context.WithTimeout(context.Background(), 2*time.Minute)
		if _, err := h.rootfs.Retire(retireCtx, *stage, newRetireOperationID()); err != nil {
			retireCancel()
			h.mu.Lock()
			h.closed = false
			h.mu.Unlock()
			return errors.Join(firstErr, fmt.Errorf("retire RootFS session: %w", err))
		}
		retireCancel()
	}
	if err := os.Remove(h.socketPath); err != nil && !os.IsNotExist(err) && firstErr == nil {
		firstErr = err
	}
	if err := os.RemoveAll(h.bundleDir); err != nil && firstErr == nil {
		firstErr = err
	}

	h.mu.Lock()
	if h.phase != phaseExited && h.phase != phasePoisoned {
		h.phase = phaseExited
	}
	if h.completedAt.IsZero() {
		h.completedAt = time.Now()
	}
	closeDoneLocked(h.done)
	h.mu.Unlock()
	return firstErr
}

// IsRunning reports whether Nomad still owns a warm or active allocation.
func (h *taskHandle) IsRunning() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	switch h.phase {
	case phaseWarm, phaseClaiming, phaseActive, phaseStopping:
		return true
	default:
		return false
	}
}

// TaskStatus exposes non-secret slot state to Nomad.
func (h *taskHandle) TaskStatus() *drivers.TaskStatus {
	h.mu.Lock()
	defer h.mu.Unlock()
	state := drivers.TaskStateRunning
	if !h.IsRunningLocked() {
		state = drivers.TaskStateExited
	}
	return &drivers.TaskStatus{
		ID:          h.taskConfig.ID,
		Name:        h.taskConfig.Name,
		State:       state,
		StartedAt:   h.startedAt,
		CompletedAt: h.completedAt,
		ExitResult:  h.exitResult,
		DriverAttributes: map[string]string{
			"container_id": h.containerID,
			"phase":        string(h.phase),
			"root_mounted": fmt.Sprintf("%t", h.rootMounted),
		},
	}
}

func (h *taskHandle) IsRunningLocked() bool {
	switch h.phase {
	case phaseWarm, phaseClaiming, phaseActive, phaseStopping:
		return true
	default:
		return false
	}
}

// PersistedState snapshots durable task identity without the policy token.
func (h *taskHandle) PersistedState() PersistedState {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.persistedLocked()
}

// Recover validates actual runsc state before exposing a recovered slot.
func (h *taskHandle) Recover(state PersistedState) error {
	if state.TaskConfig == nil || state.ContainerID == "" || state.BundleDir == "" || state.RootMount == "" {
		return errors.New("persisted task state is incomplete")
	}
	if !filepath.IsAbs(state.BundleDir) || !filepath.IsAbs(state.RootMount) {
		return errors.New("persisted bundle and root mount paths must be absolute")
	}
	if localState, err := readPersistedState(filepath.Join(state.BundleDir, ".sandbox0-driver-state.json")); err == nil &&
		localState.ContainerID == state.ContainerID && localState.BundleDir == state.BundleDir &&
		localState.RootMount == state.RootMount && localState.TaskConfig != nil {
		state = localState
	}
	h.mu.Lock()
	h.taskConfig = state.TaskConfig
	h.startedAt = state.StartedAt
	h.rootMounted = state.RootMounted
	h.claim = state.Claim
	if state.Claim != nil {
		h.stage = state.Claim.Stage
	}
	h.phase = phaseWarm
	h.mu.Unlock()

	status := ""
	var runscState RunscState
	if state.RootMounted {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		runscState, err := h.runner.State(ctx, h.containerID)
		cancel()
		if err != nil {
			h.markPoisoned(fmt.Errorf("recover runsc state: %w", err))
			return err
		}
		status = strings.ToLower(strings.TrimSpace(runscState.Status))
	}
	switch {
	case !state.RootMounted:
		h.setPhase(phaseWarm)
	case status == "running":
		h.setPhase(phaseActive)
		go h.waitForExit()
	case status == "created" && !state.RootMounted:
		h.setPhase(phaseWarm)
	case status == "created" && state.RootMounted:
		h.markPoisoned(errors.New("recovered claim stopped between D bind and runsc start"))
	case status == "stopped":
		h.mu.Lock()
		h.phase = phaseExited
		if h.completedAt.IsZero() {
			h.completedAt = time.Now()
		}
		closeDoneLocked(h.done)
		h.mu.Unlock()
	default:
		h.markPoisoned(fmt.Errorf("unsupported recovered runsc state %q", runscState.Status))
	}
	return nil
}

func (h *taskHandle) setPhase(phase slotPhase) {
	h.mu.Lock()
	h.phase = phase
	h.mu.Unlock()
}

func (h *taskHandle) markPoisoned(err error) {
	h.mu.Lock()
	h.phase = phasePoisoned
	h.exitResult = &drivers.ExitResult{Err: err}
	if h.completedAt.IsZero() {
		h.completedAt = time.Now()
	}
	closeDoneLocked(h.done)
	h.mu.Unlock()
	h.logger.Error("slot poisoned", "error", err)
}

func (h *taskHandle) statusSnapshot() statusResponse {
	h.mu.Lock()
	defer h.mu.Unlock()
	result := statusResponse{
		TaskID:      h.taskConfig.ID,
		AllocID:     h.taskConfig.AllocID,
		ContainerID: h.containerID,
		Phase:       string(h.phase),
		RootMounted: h.rootMounted,
		StartedAt:   h.startedAt,
		CompletedAt: h.completedAt,
	}
	if h.claim != nil {
		result.WriterEpoch = h.claim.WriterEpoch
	}
	return result
}
