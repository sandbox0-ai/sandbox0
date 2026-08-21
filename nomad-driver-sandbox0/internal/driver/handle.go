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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/containerd/errdefs"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/nomad/plugins/drivers"

	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
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

// ClaimRequest is the shared one-shot region-to-driver claim contract.
type ClaimRequest = protocol.NodeClaimControlRequest

// CommandReadyRequest is the shared manager-to-driver command-ready contract.
type CommandReadyRequest = protocol.CommandReadyControlRequest

type claimMetadata struct {
	OperationID         string                      `json:"operation_id,omitempty"`
	ClaimID             string                      `json:"claim_id,omitempty"`
	LaunchAttempt       string                      `json:"launch_attempt,omitempty"`
	RootFSBindingDigest string                      `json:"rootfs_binding_digest,omitempty"`
	ClaimNetworkDigest  string                      `json:"claim_network_digest,omitempty"`
	ProcdInstanceID     string                      `json:"procd_instance_id,omitempty"`
	CommandReadyDigest  string                      `json:"command_ready_digest,omitempty"`
	RuntimeRevision     string                      `json:"runtime_revision,omitempty"`
	RootfsPath          string                      `json:"rootfs_path"`
	WriterEpoch         string                      `json:"writer_epoch"`
	Stage               *rootfshandoff.StageRequest `json:"stage,omitempty"`
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
	network           NetworkRuntime
	runtimeSlotNeeded bool
	procdPort         int
	logger            hclog.Logger
}

type taskHandle struct {
	mu sync.Mutex
	// closeMu keeps concurrent Nomad GC calls from treating an in-progress,
	// retryable cleanup as complete.
	closeMu sync.Mutex

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
	network           NetworkRuntime
	runtimeSlotNeeded bool
	procdPort         int
	networkChain      string
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

	controlOnce      sync.Once
	controlServer    *http.Server
	controlReady     chan struct{}
	controlReadyErr  error
	controlReadyOnce sync.Once
	leaseFenceOnce   sync.Once
	consumerCancel   context.CancelFunc
	waitCancel       context.CancelFunc
	runtimeSlot      *runtimeSlotLifecycle
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
		driverConfig:      options.driverConfig,
		bundleDir:         options.bundleDir,
		containerID:       options.containerID,
		rootMount:         options.rootMount,
		socketPath:        options.socketPath,
		allowedRoot:       options.allowedRoot,
		runner:            options.runner,
		mounter:           options.mounter,
		rootfsAllowedRoot: options.rootfsAllowedRoot,
		rootfs:            options.rootfs,
		network:           options.network,
		runtimeSlotNeeded: options.runtimeSlotNeeded,
		procdPort:         options.procdPort,
		logger:            options.logger,
		networkChain:      networkChainName(options.containerID),
		phase:             phaseWarm,
		done:              make(chan struct{}),
		controlReady:      make(chan struct{}),
	}
}

func (h *taskHandle) signalControlReady(err error) {
	h.mu.Lock()
	h.controlReadyErr = err
	h.mu.Unlock()
	h.controlReadyOnce.Do(func() { close(h.controlReady) })
}

func (h *taskHandle) waitControlReady(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-h.controlReady:
		h.mu.Lock()
		defer h.mu.Unlock()
		return h.controlReadyErr
	}
}

func (h *taskHandle) netnsPath() string {
	if h.taskConfig == nil || h.taskConfig.NetworkIsolation == nil {
		return ""
	}
	return h.taskConfig.NetworkIsolation.Path
}

func decodeNetworkPolicy(raw string) (NetworkPolicy, string, error) {
	if strings.TrimSpace(raw) == "" {
		return NetworkPolicy{Mode: networkPolicyBlockAll}, digestString(raw), nil
	}
	var policy NetworkPolicy
	if err := json.Unmarshal([]byte(raw), &policy); err != nil {
		return NetworkPolicy{}, "", fmt.Errorf("decode network policy: %w", err)
	}
	if err := policy.Validate(); err != nil {
		return NetworkPolicy{}, "", err
	}
	return policy, digestString(raw), nil
}

func digestString(raw string) string {
	sum := sha256.Sum256([]byte(raw))
	return "sha256:" + hex.EncodeToString(sum[:])
}

func (h *taskHandle) resetNetworkPolicy() {
	if h.runtimeSlotNeeded || h.network == nil || h.networkChain == "" || h.netnsPath() == "" {
		return
	}
	_ = h.network.Apply(context.Background(), h.netnsPath(), h.networkChain, NetworkPolicy{Mode: networkPolicyBlockAll})
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
	if !h.runtimeSlotNeeded && h.network != nil && h.netnsPath() != "" {
		if err := h.network.Apply(context.Background(), h.netnsPath(), h.networkChain, NetworkPolicy{Mode: networkPolicyBlockAll}); err != nil {
			_ = h.Close(false)
			return fmt.Errorf("apply warm default-deny policy: %w", err)
		}
	}
	if err := h.persist(); err != nil {
		_ = h.Close(false)
		return err
	}
	prepared = true
	return nil
}

func (h *taskHandle) writeClaimBundle(assignment *runtimecontrol.Assignment) error {
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
			CPUSetCpus:       linux.CpusetCpus,
			MemoryLimitBytes: linux.MemoryLimitBytes,
		}
	}
	if h.runtimeSlotNeeded {
		normalized, err := normalizedRuntimeSlotResources(h.taskConfig)
		if err != nil {
			return fmt.Errorf("normalize runtime slot resources: %w", err)
		}
		resources = &normalized
	}
	h.mu.Lock()
	command := h.driverConfig.Command
	args := h.driverConfig.Args
	procdPort := h.procdPort
	h.mu.Unlock()
	var runtimeEnv []string
	if assignment != nil {
		if procdPort != protocol.NomadProcdPort {
			return fmt.Errorf("static procd port must be %d", protocol.NomadProcdPort)
		}
		payload, err := json.Marshal(assignment)
		if err != nil {
			return fmt.Errorf("encode static procd runtime assignment: %w", err)
		}
		runtimeEnv = []string{
			"http_port=" + strconv.Itoa(procdPort),
			runtimecontrol.EnvControlMode + "=" + runtimecontrol.ControlModeStatic,
			runtimecontrol.EnvStaticAssignment + "=" + string(payload),
		}
	}
	spec := buildSpec(specOptions{
		Command:   command,
		Args:      args,
		Env:       runtimeEnv,
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
		if h.phase == phaseActive {
			matches, err := h.activeRegionalClaimRetryMatchesLocked(request)
			if err != nil {
				h.mu.Unlock()
				return err
			}
			if matches {
				h.mu.Unlock()
				return nil
			}
		}
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
	h.mu.Lock()
	runtimeSlotNeeded := h.runtimeSlotNeeded
	h.mu.Unlock()
	var err error
	networkPolicy := NetworkPolicy{}
	networkDigest := digestString(request.NetworkPolicy)
	if !runtimeSlotNeeded {
		networkPolicy, networkDigest, err = decodeNetworkPolicy(request.NetworkPolicy)
		if err != nil {
			h.setPhase(phaseWarm)
			return err
		}
	}
	runtimeRevision := ""
	if runtimeSlotNeeded && request.Stage == nil {
		h.setPhase(phaseWarm)
		return errors.New("regional runtime slot claims require a RootFS stage")
	}
	if runtimeSlotNeeded {
		if err := request.ValidateRegional(); err != nil {
			h.setPhase(phaseWarm)
			return fmt.Errorf("validate regional runtime slot claim: %w: %w", err, errdefs.ErrInvalidArgument)
		}
		var err error
		runtimeRevision, err = request.Runtime.Revision()
		if err != nil {
			h.setPhase(phaseWarm)
			return fmt.Errorf("derive static runtime assignment revision: %w: %w", err, errdefs.ErrInvalidArgument)
		}
	}
	rootfsSource := request.RootfsPath
	allowedRoot := h.allowedRoot
	var durableStage *rootfshandoff.StageRequest
	var runtimeSlot *runtimeSlotLifecycle
	var startingRequest *protocol.StartingRequest
	sessionAttached := false
	claimSucceeded := false
	defer func() {
		if runtimeSlotNeeded || durableStage == nil || !sessionAttached || claimSucceeded || h.rootfs == nil {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		if _, err := h.rootfs.Retire(ctx, *durableStage, retireOperationID(*durableStage)); err != nil {
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
		if request.Stage.ExpectedPolicyToken.PolicyDigest != networkDigest {
			h.setPhase(phaseWarm)
			return errors.New("network policy does not match RootFS stage token")
		}
		durable := request.Stage.WithoutWriterGrantToken()
		durableStage = &durable
		allowedRoot = h.rootfsAllowedRoot
		runtimeSlot, startingRequest, err = h.runtimeSlotStartingRequest(request, durable, networkDigest)
		if err != nil {
			h.setPhase(phaseWarm)
			return err
		}
	}
	if err := h.writeClaimBundle(request.Runtime); err != nil {
		h.setPhase(phaseWarm)
		return err
	}
	if !runtimeSlotNeeded && h.network != nil && h.netnsPath() != "" {
		if err := h.network.Apply(context.Background(), h.netnsPath(), h.networkChain, networkPolicy); err != nil {
			h.setPhase(phaseWarm)
			return fmt.Errorf("apply claim network policy: %w", err)
		}
	}
	h.mu.Lock()
	h.rootMounted = true
	h.phase = phaseClaiming
	if durableStage != nil {
		h.claim = &claimMetadata{WriterEpoch: request.WriterEpoch, Stage: durableStage}
		if startingRequest != nil {
			h.claim.OperationID = startingRequest.OperationID
			h.claim.ClaimID = startingRequest.ClaimID
			h.claim.LaunchAttempt = startingRequest.LaunchAttempt
			h.claim.RootFSBindingDigest = startingRequest.RootFSBindingDigest
			h.claim.ClaimNetworkDigest = startingRequest.ClaimNetworkDigest
			h.claim.RuntimeRevision = runtimeRevision
		}
		h.stage = durableStage
	}
	h.mu.Unlock()
	if err := h.persist(); err != nil {
		_ = h.rollbackClaim()
		return fmt.Errorf("persist claiming state: %w", err)
	}

	if durableStage != nil {
		attachCtx, attachCancel := context.WithTimeout(context.Background(), 3*time.Minute)
		mount, err := h.rootfs.Ensure(attachCtx, *request.Stage, h.handleWriterLeaseLoss)
		attachCancel()
		if err != nil {
			attachErr := fmt.Errorf("attach RootFS session: %w", err)
			var consumedErr *consumedRootFSAttachError
			if !errors.As(err, &consumedErr) {
				_ = h.rollbackClaim()
				return attachErr
			}
			if runtimeSlotNeeded {
				return h.poisonClaimLaunch(errors.Join(
					attachErr,
					errors.New("regional runtime-slot cleanup is required for the consumed writer"),
				), false)
			}
			fenceErr := h.crashAbandonPersistedRootFS(errors.New("RootFS attach failed; writer was crash-abandoned"))
			if fenceErr != nil {
				h.markPoisoned(errors.Join(attachErr, fenceErr))
				_ = h.persist()
				return errors.Join(attachErr, fmt.Errorf("crash-abandon failed RootFS attach: %w", fenceErr))
			}
			return errors.Join(attachErr, errors.New("writer was crash-abandoned"))
		}
		rootfsSource = mount.Source
		sessionAttached = true
	}
	resolvedRootfs, err := validateRootfsPath(rootfsSource, allowedRoot)
	if err != nil {
		return h.failClaimBeforeLaunch(err, durableStage != nil)
	}
	if durableStage != nil {
		hostMountNamespace, err := os.Readlink("/proc/self/ns/mnt")
		if err != nil {
			return h.poisonClaimLaunch(fmt.Errorf("read host mount namespace: %w", err), false)
		}
		registerCtx, registerCancel := context.WithTimeout(context.Background(), 5*time.Second)
		consumer := RootFSConsumerRequest{
			ActiveKey: h.taskConfig.ID, ContainerID: h.containerID,
			StableMount: h.rootMount, HostMountNamespace: hostMountNamespace,
		}
		if h.netnsPath() != "" && durableStage.ExpectedPolicyToken.NetNSIdentity != "" {
			consumer.NetNSPath = h.netnsPath()
			consumer.NetNSIdentity = durableStage.ExpectedPolicyToken.NetNSIdentity
			consumer.NetworkChain = h.networkChain
		}
		lease, err := h.rootfs.RegisterConsumer(registerCtx, *durableStage, consumer)
		registerCancel()
		if err != nil {
			return h.poisonClaimLaunch(fmt.Errorf("register RootFS runtime consumer: %w", err), false)
		}
		h.startConsumerRenewal(*durableStage, lease)
	}

	if err := h.mounter.Bind(resolvedRootfs, h.rootMount); err != nil {
		return h.failClaimBeforeLaunch(err, durableStage != nil)
	}
	if startingRequest != nil {
		startingCtx, startingCancel := context.WithTimeout(context.Background(), runtimeSlotStartingTimeout)
		_, err := runtimeSlot.reportStarting(startingCtx, *startingRequest)
		startingCancel()
		if err != nil {
			return h.poisonClaimLaunch(err, false)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := h.runner.Create(ctx, h.bundleDir, h.containerID); err != nil {
		return h.poisonClaimLaunch(fmt.Errorf("runsc create: %w", err), true)
	}
	cancel()
	startCtx, startCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer startCancel()
	if err := h.runner.Start(startCtx, h.containerID); err != nil {
		return h.poisonClaimLaunch(fmt.Errorf("runsc start: %w", err), true)
	}

	h.mu.Lock()
	h.phase = phaseActive
	if h.claim == nil {
		h.claim = &claimMetadata{WriterEpoch: request.WriterEpoch, Stage: durableStage}
	}
	h.claim.RootfsPath = resolvedRootfs
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
	h.startExitWatch()
	return nil
}

// activeRegionalClaimRetryMatchesLocked accepts only the byte-stable logical
// claim after runsc has started. This closes the response-loss window without
// making a one-shot slot reusable for another grant or policy.
func (h *taskHandle) activeRegionalClaimRetryMatchesLocked(request ClaimRequest) (bool, error) {
	if !h.runtimeSlotNeeded || h.claim == nil || h.claim.Stage == nil || request.Stage == nil {
		return false, nil
	}
	if err := request.ValidateRegional(); err != nil {
		return false, fmt.Errorf("validate active regional claim retry: %w: %w", err, errdefs.ErrInvalidArgument)
	}
	if request.OperationID != h.claim.OperationID || request.ClaimID != h.claim.ClaimID ||
		request.WriterEpoch != h.claim.WriterEpoch {
		return false, nil
	}
	requestDigest, err := request.Stage.BindingDigest()
	if err != nil {
		return false, fmt.Errorf("derive active claim retry binding: %w", err)
	}
	storedDigest, err := h.claim.Stage.BindingDigest()
	if err != nil {
		return false, fmt.Errorf("derive persisted active claim binding: %w", err)
	}
	requestDigestHex := hex.EncodeToString(requestDigest[:])
	runtimeRevision, err := request.Runtime.Revision()
	if err != nil {
		return false, fmt.Errorf("derive active runtime assignment revision: %w", err)
	}
	return requestDigest == storedDigest && requestDigestHex == h.claim.RootFSBindingDigest &&
		runtimeRevision == h.claim.RuntimeRevision, nil
}

// handleWriterLeaseLoss poisons the one-shot slot before exposing its exit and
// asynchronously runs the same exact-owner crash fence used after a plugin
// restart. Marking the phase first prevents a concurrent Nomad DestroyTask
// from attempting planned publication with an expired writer lease.
func (h *taskHandle) handleWriterLeaseLoss(cause error) {
	if cause == nil {
		return
	}
	h.leaseFenceOnce.Do(func() {
		leaseErr := fmt.Errorf("RootFS writer lease lost: %w", cause)
		h.mu.Lock()
		if h.closed || h.phase == phaseExited || h.phase == phasePoisoned {
			h.mu.Unlock()
			return
		}
		h.phase = phasePoisoned
		h.exitResult = &drivers.ExitResult{Err: leaseErr}
		if h.completedAt.IsZero() {
			h.completedAt = time.Now()
		}
		closeDoneLocked(h.done)
		h.mu.Unlock()
		if err := h.persist(); err != nil {
			h.logger.Error("persist writer lease loss", "error", err)
		}
		h.logger.Error("fencing expired RootFS writer", "error", leaseErr)
		go h.retryWriterLeaseFence()
	})
}

func (h *taskHandle) retryWriterLeaseFence() {
	delay := 100 * time.Millisecond
	for {
		if err := h.Close(true); err == nil {
			return
		} else {
			h.logger.Error("fence expired RootFS writer", "error", err)
		}
		time.Sleep(delay)
		if delay < 5*time.Second {
			delay *= 2
			if delay > 5*time.Second {
				delay = 5 * time.Second
			}
		}
	}
}

func (h *taskHandle) rollbackClaim() error {
	h.stopConsumerRenewal()
	_ = h.mounter.Unmount(h.rootMount)
	h.mu.Lock()
	h.rootMounted = false
	h.phase = phaseWarm
	h.claim = nil
	h.stage = nil
	h.mu.Unlock()
	h.resetNetworkPolicy()
	return h.persist()
}

func (h *taskHandle) failClaimBeforeLaunch(cause error, writerConsumed bool) error {
	if writerConsumed {
		return h.poisonClaimLaunch(cause, false)
	}
	if err := h.rollbackClaim(); err != nil {
		return errors.Join(cause, fmt.Errorf("rollback unconsumed claim: %w", err))
	}
	return cause
}

// poisonClaimLaunch prevents reuse after the writer was consumed or regional
// starting may have committed, and leaves failed detach state recoverable.
func (h *taskHandle) poisonClaimLaunch(cause error, deleteContainer bool) error {
	h.stopConsumerRenewal()
	unmountErr := h.mounter.Unmount(h.rootMount)
	var deleteErr error
	if deleteContainer {
		deleteCtx, deleteCancel := context.WithTimeout(context.Background(), 30*time.Second)
		deleteErr = h.runner.Delete(deleteCtx, h.containerID, true)
		deleteCancel()
	}
	h.resetNetworkPolicy()
	result := errors.Join(cause, unmountErr, deleteErr)
	h.mu.Lock()
	h.rootMounted = unmountErr != nil
	h.phase = phasePoisoned
	h.exitResult = &drivers.ExitResult{Err: result}
	if h.completedAt.IsZero() {
		h.completedAt = time.Now()
	}
	closeDoneLocked(h.done)
	h.mu.Unlock()
	return errors.Join(result, h.persist())
}

func (h *taskHandle) startExitWatch() {
	ctx, cancel := context.WithCancel(context.Background())
	h.mu.Lock()
	previous := h.waitCancel
	h.waitCancel = cancel
	h.mu.Unlock()
	if previous != nil {
		previous()
	}
	go h.waitForExit(ctx)
}

func (h *taskHandle) stopExitWatch() {
	h.mu.Lock()
	cancel := h.waitCancel
	h.waitCancel = nil
	h.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (h *taskHandle) waitForExit(ctx context.Context) {
	result, err := h.runner.Wait(ctx, h.containerID)
	if errors.Is(err, context.Canceled) {
		return
	}
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
	h.closeMu.Lock()
	defer h.closeMu.Unlock()

	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return nil
	}
	h.closed = true
	rootMounted := h.rootMounted
	phase := h.phase
	stage := h.stage
	if stage == nil && h.claim != nil {
		stage = h.claim.Stage
	}
	h.mu.Unlock()

	h.stopControl()
	h.stopExitWatch()
	h.stopConsumerRenewal()
	cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if force {
		_ = h.runner.Kill(cleanupCtx, h.containerID, "KILL")
	}
	firstErr := h.runner.Delete(cleanupCtx, h.containerID, true)
	if rootMounted && (h.runtimeSlotNeeded || phase != phasePoisoned || stage == nil) {
		if err := h.mounter.Unmount(h.rootMount); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	// The regional runtime-slot reconciler is the sole terminal writer owner.
	// The plugin removes its task-facing bind, but leaves the durable RootFS
	// session for authenticated node cleanup so Nomad StopTask cannot race a
	// crash-abandon transaction with a competing planned retirement.
	if !h.runtimeSlotNeeded && stage != nil && rootMounted && h.rootfs != nil {
		retireCtx, retireCancel := context.WithTimeout(context.Background(), 2*time.Minute)
		var err error
		if phase == phasePoisoned {
			err = h.crashAbandonPersistedRootFS(errors.New("poisoned RootFS writer was crash-abandoned during task cleanup"))
		} else {
			_, err = h.rootfs.Retire(retireCtx, *stage, retireOperationID(*stage))
		}
		if err != nil {
			retireCancel()
			h.mu.Lock()
			h.closed = false
			h.mu.Unlock()
			return errors.Join(firstErr, fmt.Errorf("terminate RootFS session: %w", err))
		}
		retireCancel()
	}
	if !h.runtimeSlotNeeded && h.network != nil && h.networkChain != "" && h.netnsPath() != "" {
		if err := h.network.Cleanup(context.Background(), h.netnsPath(), h.networkChain); err != nil && firstErr == nil {
			firstErr = err
		}
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
	if firstErr != nil {
		h.closed = false
	} else {
		h.rootMounted = false
	}
	h.mu.Unlock()
	return firstErr
}

func (h *taskHandle) startConsumerRenewal(stage rootfshandoff.StageRequest, lease RootFSConsumerLease) {
	h.stopConsumerRenewal()
	ctx, cancel := context.WithCancel(context.Background())
	h.mu.Lock()
	h.consumerCancel = cancel
	h.mu.Unlock()
	go func() {
		current := lease
		for {
			remaining := time.Until(current.ExpiresAt)
			if remaining <= 0 {
				h.handleWriterLeaseLoss(errors.New("RootFS consumer lease expired"))
				return
			}
			delay := remaining / 3
			if delay < time.Second {
				delay = time.Second
			}
			timer := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
			renewCtx, renewCancel := context.WithTimeout(ctx, 5*time.Second)
			next, err := h.rootfs.RenewConsumer(renewCtx, stage, current)
			renewCancel()
			if err != nil {
				h.handleWriterLeaseLoss(fmt.Errorf("renew RootFS consumer lease: %w", err))
				return
			}
			current = next
		}
	}()
}

func (h *taskHandle) stopConsumerRenewal() {
	h.mu.Lock()
	cancel := h.consumerCancel
	h.consumerCancel = nil
	h.mu.Unlock()
	if cancel != nil {
		cancel()
	}
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
	if state.RootMounted && h.stage != nil {
		return h.recoverCrashedRootFS()
	}

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
		h.startExitWatch()
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

func (h *taskHandle) recoverCrashedRootFS() error {
	if h.rootfs == nil || h.stage == nil {
		return errors.New("recovered RootFS task has no storage runtime")
	}
	cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cleanupCancel()
	_ = h.runner.Kill(cleanupCtx, h.containerID, "KILL")
	if err := h.runner.Delete(cleanupCtx, h.containerID, true); err != nil {
		return fmt.Errorf("delete crashed gVisor task: %w", err)
	}
	if h.runtimeSlotNeeded {
		if err := h.mounter.Unmount(h.rootMount); err != nil {
			return fmt.Errorf("unmount crashed runtime-slot task root: %w", err)
		}
		h.mu.Lock()
		h.rootMounted = false
		h.phase = phasePoisoned
		h.exitResult = &drivers.ExitResult{Err: errors.New("task driver restarted; regional runtime-slot cleanup is required")}
		if h.completedAt.IsZero() {
			h.completedAt = time.Now()
		}
		closeDoneLocked(h.done)
		h.mu.Unlock()
		return h.persist()
	}
	return h.crashAbandonPersistedRootFS(errors.New("task driver restarted; RootFS writer was crash-abandoned"))
}

func (h *taskHandle) crashAbandonPersistedRootFS(exitErr error) error {
	if h.rootfs == nil || h.stage == nil {
		return errors.New("crashed RootFS task has no storage runtime")
	}
	operationID := crashOperationID(*h.stage)
	if err := h.mounter.Unmount(h.rootMount); err != nil {
		return fmt.Errorf("unmount crashed task root: %w", err)
	}
	mountNamespace, err := os.Readlink("/proc/self/ns/mnt")
	if err != nil {
		return fmt.Errorf("read host mount namespace: %w", err)
	}
	fenceCtx, fenceCancel := context.WithTimeout(context.Background(), 45*time.Second)
	_, err = h.rootfs.CrashFence(fenceCtx, *h.stage, operationID, crashTaskObservation{
		ActiveKey: h.taskConfig.ID, ContainerID: h.containerID,
		HostMountNamespaceID: mountNamespace, ContainerAbsent: true, TaskAbsent: true,
		FrontendSnapshotAbsent: true, StableMountAbsent: true,
	})
	fenceCancel()
	if err != nil {
		return fmt.Errorf("crash-fence recovered RootFS task: %w", err)
	}
	if !h.runtimeSlotNeeded && h.network != nil && h.networkChain != "" && h.netnsPath() != "" {
		if err := h.network.Cleanup(context.Background(), h.netnsPath(), h.networkChain); err != nil {
			return fmt.Errorf("cleanup crashed task network policy: %w", err)
		}
	}
	h.mu.Lock()
	h.rootMounted = false
	h.phase = phasePoisoned
	h.exitResult = &drivers.ExitResult{Err: exitErr}
	if h.completedAt.IsZero() {
		h.completedAt = time.Now()
	}
	closeDoneLocked(h.done)
	h.mu.Unlock()
	return h.persist()
}

func crashOperationID(stage rootfshandoff.StageRequest) string {
	payload := fmt.Sprintf("%s\x00%s\x00%d", stage.Parent, stage.Identity.WriterGrantID, stage.Identity.WriterEpoch)
	sum := sha256.Sum256([]byte(payload))
	return "nomad-crash-" + hex.EncodeToString(sum[:16])
}

func retireOperationID(stage rootfshandoff.StageRequest) string {
	return rootfshandoff.PlannedRetireOperationID(
		stage.Parent,
		stage.Identity.WriterGrantID,
		stage.Identity.WriterEpoch,
	)
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
