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
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/containerd/errdefs"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/nomad/plugins/drivers"
	slotauthority "github.com/sandbox0-ai/sandbox0/manager/pkg/runtimeslotauthority"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"golang.org/x/sys/unix"
)

const (
	runtimeSlotProofVersion      = 1
	runtimeSlotActivationTimeout = 10 * time.Second
	runtimeSlotControlTimeout    = 5 * time.Second
	runtimeSlotMaxHeartbeatTTL   = 5 * time.Minute
	runtimeSlotStartingTimeout   = 10 * time.Second
)

type runtimeSlotAuthority interface {
	Register(context.Context, string, protocol.RegistrationRequest) (protocol.Observation, error)
	Observe(context.Context, string) (protocol.Observation, error)
	Ready(context.Context, string, protocol.ReadinessRequest) (protocol.Observation, error)
	Heartbeat(context.Context, string, protocol.HeartbeatRequest) (protocol.Observation, error)
	Starting(context.Context, string, protocol.StartingRequest) (protocol.Observation, error)
	CommandReady(context.Context, string, protocol.CommandReadyRequest) (protocol.Observation, error)
}

type runtimeSlotLifecycle struct {
	authority    runtimeSlotAuthority
	slotID       string
	registration protocol.RegistrationRequest
	readiness    protocol.ReadinessRequest
	heartbeat    protocol.HeartbeatRequest
	logger       hclog.Logger
}

type runtimeCompatibilityProof struct {
	Version          int    `json:"version"`
	Architecture     string `json:"architecture"`
	DriverVersion    string `json:"driver_version"`
	RunscVersion     string `json:"runsc_version"`
	Platform         string `json:"platform"`
	Overlay2         string `json:"overlay2"`
	FileAccess       string `json:"file_access"`
	DirectFS         bool   `json:"directfs"`
	Command          string `json:"command"`
	CPUPeriod        int64  `json:"cpu_period"`
	CPUQuota         int64  `json:"cpu_quota"`
	CPUShares        int64  `json:"cpu_shares"`
	MemoryLimitBytes int64  `json:"memory_limit_bytes"`
}

type runtimeReadyProof struct {
	Version             int    `json:"version"`
	SlotID              string `json:"slot_id"`
	CompatibilityDigest string `json:"compatibility_digest"`
	ControlEndpoint     string `json:"control_endpoint"`
	ContainerID         string `json:"container_id"`
	RootMount           string `json:"root_mount"`
}

type networkReadyProof struct {
	Version       int    `json:"version"`
	SlotID        string `json:"slot_id"`
	NetNSIdentity string `json:"netns_identity"`
	NetworkChain  string `json:"network_chain"`
	DefaultPolicy string `json:"default_policy"`
}

type storageReadyProof struct {
	Version           int    `json:"version"`
	SlotID            string `json:"slot_id"`
	SessiondSocket    string `json:"sessiond_socket"`
	RootFSMountRoot   string `json:"rootfs_mount_root"`
	MaxDirtyTailBytes int64  `json:"max_dirty_tail_bytes"`
}

type runtimeSlotClaimNetworkProof struct {
	Version             int                              `json:"version"`
	SlotID              string                           `json:"slot_id"`
	OperationID         string                           `json:"operation_id"`
	ClaimID             string                           `json:"claim_id"`
	NetNSIdentity       string                           `json:"netns_identity"`
	NetworkChain        string                           `json:"network_chain"`
	PolicyDigest        string                           `json:"policy_digest"`
	ExpectedPolicyToken rootfshandoff.NetworkPolicyToken `json:"expected_policy_token"`
}

type runtimeSlotStorageHealth interface {
	Ping(context.Context) error
}

func newRuntimeSlotAuthority(config *PluginConfig) (runtimeSlotAuthority, error) {
	if err := validateRuntimeSlotConfig(config); err != nil {
		return nil, err
	}
	client, err := slotauthority.NewClient(slotauthority.ClientConfig{
		BaseURL: config.RootFSAuthorityURL, CAFile: config.RootFSAuthorityCAFile,
		ClientCertFile: config.RootFSAuthorityClientCertFile,
		ClientKeyFile:  config.RootFSAuthorityClientKeyFile,
		TokenFile:      config.RootFSAuthorityTokenFile,
		Timeout:        2 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("create runtime slot authority client: %w", err)
	}
	return client, nil
}

func validateRuntimeSlotConfig(config *PluginConfig) error {
	if config == nil || !config.RuntimeSlotEnabled {
		return nil
	}
	if strings.TrimSpace(config.RuntimeSlotClusterID) == "" || len(config.RuntimeSlotClusterID) > 512 {
		return fmt.Errorf("runtime_slot_cluster_id is required and must not exceed 512 bytes")
	}
	if !config.RootFSEnabled || strings.TrimSpace(config.RootFSSessiondSocket) == "" {
		return fmt.Errorf("runtime slots require the node-scoped RootFS session daemon")
	}
	if !config.NetworkPolicyEnabled {
		return fmt.Errorf("runtime slots require network_policy_enabled")
	}
	if strings.TrimSpace(config.RootFSAuthorityURL) == "" {
		return fmt.Errorf("runtime slots require rootfs_authority_url")
	}
	for name, value := range map[string]string{
		"rootfs_authority_ca_file":          config.RootFSAuthorityCAFile,
		"rootfs_authority_client_cert_file": config.RootFSAuthorityClientCertFile,
		"rootfs_authority_client_key_file":  config.RootFSAuthorityClientKeyFile,
		"rootfs_authority_token_file":       config.RootFSAuthorityTokenFile,
		"runtime_slot_node_boot_id_file":    config.RuntimeSlotNodeBootIDFile,
	} {
		if !filepath.IsAbs(strings.TrimSpace(value)) || filepath.Clean(value) == "/" {
			return fmt.Errorf("%s must be a non-root absolute path", name)
		}
	}
	return nil
}

func validateRuntimeSlotTaskConfig(config *PluginConfig, task TaskConfig) error {
	if config == nil || !config.RuntimeSlotEnabled {
		return nil
	}
	if !task.WaitForClaim {
		return fmt.Errorf("regional runtime slots require wait_for_claim=true")
	}
	if task.Command != "/procd" || len(task.Args) != 0 {
		return fmt.Errorf("regional runtime slots require command=/procd without arguments")
	}
	return nil
}

func (p *Plugin) startTaskControl(handle *taskHandle) error {
	go handle.ServeControl(p.ctx)
	ctx, cancel := context.WithTimeout(p.ctx, runtimeSlotControlTimeout)
	defer cancel()
	if err := handle.waitControlReady(ctx); err != nil {
		p.stopTaskControl(handle)
		return fmt.Errorf("start task control endpoint: %w", err)
	}
	return nil
}

func (p *Plugin) stopTaskControl(handle *taskHandle) {
	if handle == nil {
		return
	}
	handle.stopControl()
	if err := os.Remove(handle.socketPath); err != nil && !os.IsNotExist(err) {
		p.logger.Warn("remove task control socket", "path", handle.socketPath, "error", err)
	}
}

func (p *Plugin) activateRuntimeSlot(
	handle *taskHandle,
	rootfs RootFSRuntime,
	newAllocation bool,
) (*runtimeSlotLifecycle, protocol.Observation, error) {
	if p.config == nil || !p.config.RuntimeSlotEnabled {
		return nil, protocol.Observation{}, nil
	}
	authority, err := p.runtimeSlotAuthority()
	if err != nil {
		return nil, protocol.Observation{}, err
	}
	ctx, cancel := context.WithTimeout(p.ctx, runtimeSlotActivationTimeout)
	defer cancel()
	lifecycle, err := newRuntimeSlotLifecycle(ctx, p.config, handle, rootfs, authority)
	if err != nil {
		return nil, protocol.Observation{}, err
	}
	handle.mu.Lock()
	phase := handle.phase
	handle.mu.Unlock()
	observation, err := lifecycle.activate(ctx, phase, newAllocation)
	if err != nil {
		return nil, protocol.Observation{}, err
	}
	handle.mu.Lock()
	handle.runtimeSlot = lifecycle
	handle.mu.Unlock()
	return lifecycle, observation, nil
}

func newRuntimeSlotLifecycle(
	ctx context.Context,
	config *PluginConfig,
	handle *taskHandle,
	rootfs RootFSRuntime,
	authority runtimeSlotAuthority,
) (*runtimeSlotLifecycle, error) {
	if config == nil || handle == nil || handle.taskConfig == nil || authority == nil {
		return nil, fmt.Errorf("runtime slot config, task, and authority are required")
	}
	task := handle.taskConfig
	for name, value := range map[string]string{
		"slot_id": task.ID, "allocation_id": task.AllocID, "allocation_namespace": task.Namespace,
		"node_id": task.NodeID,
	} {
		if strings.TrimSpace(value) == "" {
			return nil, fmt.Errorf("%s is required for a regional runtime slot", name)
		}
	}
	if task.NetworkIsolation == nil || strings.TrimSpace(task.NetworkIsolation.Path) == "" {
		return nil, fmt.Errorf("a Nomad network namespace is required for a regional runtime slot")
	}
	netnsIdentity, err := networkNamespaceIdentity(task.NetworkIsolation.Path)
	if err != nil {
		return nil, err
	}
	bootPayload, err := os.ReadFile(config.RuntimeSlotNodeBootIDFile)
	if err != nil {
		return nil, fmt.Errorf("read node boot ID: %w", err)
	}
	bootID := strings.TrimSpace(string(bootPayload))
	if bootID == "" || len(bootID) > 512 {
		return nil, fmt.Errorf("node boot ID is invalid")
	}
	versionCtx, cancelVersion := context.WithTimeout(ctx, 2*time.Second)
	runscVersion, err := handle.runner.Version(versionCtx)
	cancelVersion()
	if err != nil {
		return nil, fmt.Errorf("read runsc version for runtime slot: %w", err)
	}
	storage, ok := rootfs.(runtimeSlotStorageHealth)
	if !ok {
		return nil, fmt.Errorf("RootFS runtime cannot prove session-daemon health")
	}
	healthCtx, cancelHealth := context.WithTimeout(ctx, 2*time.Second)
	err = storage.Ping(healthCtx)
	cancelHealth()
	if err != nil {
		return nil, fmt.Errorf("prove RootFS session-daemon health: %w", err)
	}
	if _, err := os.Stat(handle.rootMount); err != nil {
		return nil, fmt.Errorf("stat runtime slot root mount: %w", err)
	}
	compatibility, err := runtimeCompatibilityDigest(config, task, runscVersion)
	if err != nil {
		return nil, err
	}
	controlEndpoint := "unix://" + handle.socketPath
	runtimeProof, err := proofDigest(runtimeReadyProof{
		Version: runtimeSlotProofVersion, SlotID: task.ID, CompatibilityDigest: compatibility,
		ControlEndpoint: controlEndpoint, ContainerID: handle.containerID, RootMount: handle.rootMount,
	})
	if err != nil {
		return nil, err
	}
	networkProof, err := proofDigest(networkReadyProof{
		Version: runtimeSlotProofVersion, SlotID: task.ID, NetNSIdentity: netnsIdentity,
		NetworkChain: handle.networkChain, DefaultPolicy: digestString(""),
	})
	if err != nil {
		return nil, err
	}
	storageProof, err := proofDigest(storageReadyProof{
		Version: runtimeSlotProofVersion, SlotID: task.ID,
		SessiondSocket: config.RootFSSessiondSocket, RootFSMountRoot: config.RootFSMountRoot,
		MaxDirtyTailBytes: config.RootFSMaxDirtyTailBytes,
	})
	if err != nil {
		return nil, err
	}
	registration := protocol.RegistrationRequest{
		ClusterID: config.RuntimeSlotClusterID, AllocationID: task.AllocID,
		AllocationNamespace: task.Namespace, NodeID: task.NodeID, NodeBootID: bootID,
		NetNSIdentity: netnsIdentity, ControlEndpoint: controlEndpoint,
		RuntimeCompatibility: compatibility,
	}
	readiness := protocol.ReadinessRequest{
		AllocationID: task.AllocID, NodeBootID: bootID,
		RuntimeReadyDigest: runtimeProof, NetworkReadyDigest: networkProof,
		StorageReadyDigest: storageProof,
	}
	if err := registration.Validate(); err != nil {
		return nil, fmt.Errorf("validate runtime slot registration: %w", err)
	}
	if err := readiness.Validate(); err != nil {
		return nil, fmt.Errorf("validate runtime slot readiness: %w", err)
	}
	return &runtimeSlotLifecycle{
		authority: authority, slotID: task.ID, registration: registration, readiness: readiness,
		heartbeat: protocol.HeartbeatRequest{AllocationID: task.AllocID, NodeBootID: bootID},
		logger:    handle.logger.Named("runtime-slot"),
	}, nil
}

// runtimeSlotStartingRequest binds the regional claim to the exact durable
// RootFS grant, applied network incarnation, and runsc launch attempt.
func (h *taskHandle) runtimeSlotStartingRequest(
	claim ClaimRequest,
	stage rootfshandoff.StageRequest,
	policyDigest string,
) (*runtimeSlotLifecycle, *protocol.StartingRequest, error) {
	h.mu.Lock()
	required := h.runtimeSlotNeeded
	lifecycle := h.runtimeSlot
	task := h.taskConfig
	containerID := h.containerID
	networkChain := h.networkChain
	h.mu.Unlock()
	if !required {
		return nil, nil, nil
	}
	if lifecycle == nil || task == nil {
		return nil, nil, fmt.Errorf("regional runtime slot is not registered: %w", errdefs.ErrFailedPrecondition)
	}
	if claim.ClaimID != stage.Identity.ClaimID {
		return nil, nil, fmt.Errorf("runtime slot claim ID does not match RootFS stage: %w", errdefs.ErrFailedPrecondition)
	}
	netnsIdentity, err := networkNamespaceIdentity(h.netnsPath())
	if err != nil {
		return nil, nil, err
	}
	if lifecycle.registration.NetNSIdentity != netnsIdentity ||
		stage.ExpectedPolicyToken.NetNSIdentity != netnsIdentity {
		return nil, nil, fmt.Errorf("runtime slot network namespace changed before claim: %w", errdefs.ErrFailedPrecondition)
	}
	if stage.ExpectedPolicyToken.PolicyDigest != policyDigest {
		return nil, nil, fmt.Errorf("runtime slot policy digest does not match RootFS stage: %w", errdefs.ErrFailedPrecondition)
	}
	bindingDigest, err := stage.BindingDigest()
	if err != nil {
		return nil, nil, fmt.Errorf("derive RootFS binding digest: %w", err)
	}
	networkDigest, err := proofDigest(runtimeSlotClaimNetworkProof{
		Version: runtimeSlotProofVersion, SlotID: task.ID,
		OperationID: claim.OperationID, ClaimID: claim.ClaimID,
		NetNSIdentity: netnsIdentity, NetworkChain: networkChain,
		PolicyDigest: policyDigest, ExpectedPolicyToken: stage.ExpectedPolicyToken,
	})
	if err != nil {
		return nil, nil, err
	}
	request := &protocol.StartingRequest{
		AllocationID: lifecycle.heartbeat.AllocationID, NodeBootID: lifecycle.heartbeat.NodeBootID,
		OperationID: claim.OperationID, ClaimID: claim.ClaimID,
		LaunchAttempt: stage.Identity.LaunchAttempt, RunscContainerID: containerID,
		RootFSBindingDigest: hex.EncodeToString(bindingDigest[:]), ClaimNetworkDigest: networkDigest,
	}
	if err := request.Validate(); err != nil {
		return nil, nil, fmt.Errorf("validate runtime slot starting request: %w", err)
	}
	return lifecycle, request, nil
}

// reportStarting retries only the exact idempotent transition. A terminal
// authority error is returned immediately, while an ambiguous response is
// retried within the caller's bounded launch deadline.
func (l *runtimeSlotLifecycle) reportStarting(
	ctx context.Context,
	request protocol.StartingRequest,
) (protocol.Observation, error) {
	backoff := 100 * time.Millisecond
	var lastErr error
	for {
		observation, err := l.authority.Starting(ctx, l.slotID, request)
		if err == nil {
			if err := validateRuntimeSlotObservation(l.slotID, observation); err != nil {
				return protocol.Observation{}, err
			}
			if observation.State != protocol.StateStarting ||
				observation.ClaimOperationID != request.OperationID || observation.ClaimID != request.ClaimID {
				return protocol.Observation{}, fmt.Errorf(
					"regional runtime slot returned a different starting claim: %w", errdefs.ErrFailedPrecondition,
				)
			}
			return observation, nil
		}
		lastErr = err
		if errdefs.IsInvalidArgument(err) || errdefs.IsPermissionDenied(err) ||
			errdefs.IsNotFound(err) || errdefs.IsFailedPrecondition(err) {
			return protocol.Observation{}, fmt.Errorf("report regional runtime slot starting: %w", err)
		}
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return protocol.Observation{}, fmt.Errorf(
				"report regional runtime slot starting: %w",
				errors.Join(lastErr, ctx.Err(), errdefs.ErrUnavailable),
			)
		case <-timer.C:
		}
		backoff *= 2
		if backoff > 2*time.Second {
			backoff = 2 * time.Second
		}
	}
}

func (l *runtimeSlotLifecycle) activate(
	ctx context.Context,
	phase slotPhase,
	newAllocation bool,
) (protocol.Observation, error) {
	observation, err := l.authority.Register(ctx, l.slotID, l.registration)
	if err != nil {
		return protocol.Observation{}, fmt.Errorf("register regional runtime slot: %w", err)
	}
	if err := validateRuntimeSlotObservation(l.slotID, observation); err != nil {
		return protocol.Observation{}, err
	}
	if observation.State == protocol.StateRegistered && phase == phaseWarm {
		observation, err = l.authority.Ready(ctx, l.slotID, l.readiness)
		if err != nil {
			return protocol.Observation{}, fmt.Errorf("report regional runtime slot readiness: %w", err)
		}
		if err := validateRuntimeSlotObservation(l.slotID, observation); err != nil {
			return protocol.Observation{}, err
		}
	}
	if err := validateRecoveredRuntimeSlotState(observation.State, phase); err != nil {
		return protocol.Observation{}, err
	}
	if newAllocation && observation.State != protocol.StateFastpathReady {
		return protocol.Observation{}, fmt.Errorf("new runtime slot is already in state %s: %w", observation.State, errdefs.ErrFailedPrecondition)
	}
	return observation, nil
}

func validateRuntimeSlotObservation(slotID string, observation protocol.Observation) error {
	if err := observation.Validate(); err != nil {
		return fmt.Errorf("validate regional runtime slot observation: %w: %w", err, errdefs.ErrUnavailable)
	}
	if observation.SlotID != slotID {
		return fmt.Errorf("regional runtime slot authority returned another slot: %w", errdefs.ErrUnavailable)
	}
	leaseWindow := observation.HeartbeatExpiresAt.Sub(observation.ServerTime)
	if leaseWindow <= 0 || leaseWindow > runtimeSlotMaxHeartbeatTTL {
		return fmt.Errorf("regional runtime slot heartbeat lease is outside the supported range: %w", errdefs.ErrUnavailable)
	}
	return nil
}

func validateRecoveredRuntimeSlotState(state protocol.State, phase slotPhase) error {
	valid := false
	switch state {
	case protocol.StateFastpathReady:
		valid = phase == phaseWarm
	case protocol.StateClaiming:
		valid = phase == phaseWarm || phase == phaseClaiming || phase == phaseActive || phase == phasePoisoned
	case protocol.StateStarting:
		valid = phase == phaseClaiming || phase == phaseActive || phase == phasePoisoned
	case protocol.StateActive:
		valid = phase == phaseActive || phase == phasePoisoned
	case protocol.StateQuiescing:
		valid = phase == phaseStopping || phase == phaseExited || phase == phasePoisoned
	}
	if !valid {
		return fmt.Errorf("runtime slot state %s does not match recovered driver phase %s: %w",
			state, phase, errdefs.ErrFailedPrecondition)
	}
	return nil
}

func (l *runtimeSlotLifecycle) runHeartbeat(
	ctx context.Context,
	done <-chan struct{},
	initial protocol.Observation,
	onLost func(error),
) {
	observation := initial
	leaseWindow := observation.HeartbeatExpiresAt.Sub(observation.ServerTime)
	deadline := time.Now().Add(leaseWindow)
	delay := runtimeSlotHeartbeatDelay(leaseWindow)
	timer := time.NewTimer(delay)
	defer timer.Stop()
	backoff := 100 * time.Millisecond
	for {
		select {
		case <-ctx.Done():
			return
		case <-done:
			return
		case <-timer.C:
		}
		heartbeatCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		next, err := l.authority.Heartbeat(heartbeatCtx, l.slotID, l.heartbeat)
		cancel()
		if ctx.Err() != nil {
			return
		}
		if err == nil {
			err = validateRuntimeSlotObservation(l.slotID, next)
		}
		if err == nil && next.State != protocol.StateOrphaned && next.State != protocol.StateTerminal {
			observation = next
			leaseWindow = observation.HeartbeatExpiresAt.Sub(observation.ServerTime)
			deadline = time.Now().Add(leaseWindow)
			delay = runtimeSlotHeartbeatDelay(leaseWindow)
			backoff = 100 * time.Millisecond
			timer.Reset(delay)
			continue
		}
		if err == nil {
			err = fmt.Errorf("regional runtime slot entered state %s: %w", next.State, errdefs.ErrFailedPrecondition)
		}
		l.logger.Error("runtime slot heartbeat failed", "error", err)
		if errdefs.IsPermissionDenied(err) || errdefs.IsNotFound(err) || errdefs.IsFailedPrecondition(err) ||
			time.Now().Add(backoff).After(deadline) {
			if onLost != nil {
				onLost(err)
			}
			return
		}
		timer.Reset(backoff)
		backoff *= 2
		if backoff > 2*time.Second {
			backoff = 2 * time.Second
		}
	}
}

func runtimeSlotHeartbeatDelay(leaseWindow time.Duration) time.Duration {
	if leaseWindow <= 0 {
		return 100 * time.Millisecond
	}
	delay := leaseWindow / 3
	if delay < 100*time.Millisecond {
		return 100 * time.Millisecond
	}
	if delay > 10*time.Second {
		return 10 * time.Second
	}
	return delay
}

func runtimeCompatibilityDigest(config *PluginConfig, task *drivers.TaskConfig, runscVersion string) (string, error) {
	var linux drivers.LinuxResources
	if task.Resources != nil && task.Resources.LinuxResources != nil {
		linux = *task.Resources.LinuxResources
	}
	payload, err := json.Marshal(runtimeCompatibilityProof{
		Version: runtimeSlotProofVersion, Architecture: runtime.GOARCH,
		DriverVersion: PluginVersion, RunscVersion: strings.TrimSpace(runscVersion),
		Platform: config.Platform, Overlay2: config.Overlay2, FileAccess: config.FileAccess,
		DirectFS: config.DirectFS, Command: "/procd", CPUPeriod: linux.CPUPeriod,
		CPUQuota: linux.CPUQuota, CPUShares: linux.CPUShares, MemoryLimitBytes: linux.MemoryLimitBytes,
	})
	if err != nil {
		return "", fmt.Errorf("encode runtime compatibility: %w", err)
	}
	sum := sha256.Sum256(payload)
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}

func proofDigest(value any) (string, error) {
	payload, err := json.Marshal(value)
	if err != nil {
		return "", fmt.Errorf("encode runtime slot proof: %w", err)
	}
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:]), nil
}

func networkNamespaceIdentity(path string) (string, error) {
	path = filepath.Clean(strings.TrimSpace(path))
	if !filepath.IsAbs(path) || path == "/" {
		return "", fmt.Errorf("Nomad network namespace path must be a non-root absolute path")
	}
	var stat unix.Stat_t
	if err := unix.Stat(path, &stat); err != nil {
		return "", fmt.Errorf("stat Nomad network namespace: %w", err)
	}
	return fmt.Sprintf("netns-v1:%x:%x", uint64(stat.Dev), stat.Ino), nil
}

func (h *taskHandle) runtimeSlotHeartbeatLost(err error) {
	if err == nil {
		return
	}
	h.mu.Lock()
	if h.phase != phaseWarm || h.closed {
		h.mu.Unlock()
		h.logger.Error("regional runtime slot authority lost after claim", "error", err)
		return
	}
	h.phase = phasePoisoned
	h.exitResult = &drivers.ExitResult{Err: fmt.Errorf("regional runtime slot authority lost: %w", err)}
	h.completedAt = time.Now()
	closeDoneLocked(h.done)
	h.mu.Unlock()
	if persistErr := h.persist(); persistErr != nil && !errors.Is(persistErr, os.ErrNotExist) {
		h.logger.Error("persist runtime slot authority loss", "error", persistErr)
	}
}
