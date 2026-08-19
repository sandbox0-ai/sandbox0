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
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/nomad/drivers/shared/eventer"
	"github.com/hashicorp/nomad/plugins/base"
	"github.com/hashicorp/nomad/plugins/drivers"
	"github.com/hashicorp/nomad/plugins/drivers/fsisolation"
	"github.com/hashicorp/nomad/plugins/shared/hclspec"
	"github.com/hashicorp/nomad/plugins/shared/structs"
	rootfssession "github.com/sandbox0-ai/sandbox0/pkg/rootfssession"
)

const (
	PluginName        = "sandbox0-gvisor"
	PluginVersion     = "0.1.0"
	taskHandleVersion = 1
	fingerprintPeriod = 30 * time.Second
)

var (
	pluginInfo = &base.PluginInfoResponse{
		Type:              base.PluginTypeDriver,
		PluginApiVersions: []string{drivers.ApiVersion010},
		PluginVersion:     PluginVersion,
		Name:              PluginName,
	}

	pluginConfigSpec = hclspec.NewObject(map[string]*hclspec.Spec{
		"runsc_path": hclspec.NewDefault(
			hclspec.NewAttr("runsc_path", "string", false),
			hclspec.NewLiteral(`"/usr/local/bin/runsc"`),
		),
		"runsc_root": hclspec.NewDefault(
			hclspec.NewAttr("runsc_root", "string", false),
			hclspec.NewLiteral(`"/var/run/sandbox0/runsc"`),
		),
		"control_dir": hclspec.NewDefault(
			hclspec.NewAttr("control_dir", "string", false),
			hclspec.NewLiteral(`"/var/run/sandbox0/nomad-slots"`),
		),
		"allowed_rootfs_dir": hclspec.NewDefault(
			hclspec.NewAttr("allowed_rootfs_dir", "string", false),
			hclspec.NewLiteral(`"/var/lib/sandbox0/rootfs"`),
		),
		"platform": hclspec.NewDefault(
			hclspec.NewAttr("platform", "string", false),
			hclspec.NewLiteral(`"systrap"`),
		),
		"overlay2": hclspec.NewDefault(
			hclspec.NewAttr("overlay2", "string", false),
			hclspec.NewLiteral(`"none"`),
		),
		"file_access": hclspec.NewDefault(
			hclspec.NewAttr("file_access", "string", false),
			hclspec.NewLiteral(`"shared"`),
		),
		"directfs": hclspec.NewDefault(
			hclspec.NewAttr("directfs", "bool", false),
			hclspec.NewLiteral(`true`),
		),
		"dev_smoke_enabled": hclspec.NewAttr("dev_smoke_enabled", "bool", false),
		"network_policy_enabled": hclspec.NewDefault(
			hclspec.NewAttr("network_policy_enabled", "bool", false),
			hclspec.NewLiteral(`true`),
		),
		"rootfs_enabled": hclspec.NewDefault(
			hclspec.NewAttr("rootfs_enabled", "bool", false),
			hclspec.NewLiteral(`false`),
		),
		"rootfs_sessiond_socket":     hclspec.NewAttr("rootfs_sessiond_socket", "string", false),
		"rootfs_consumer_mount_root": hclspec.NewAttr("rootfs_consumer_mount_root", "string", false),
		"rootfs_state_path":          hclspec.NewAttr("rootfs_state_path", "string", false),
		"rootfs_branch_root":         hclspec.NewAttr("rootfs_branch_root", "string", false),
		"rootfs_mount_root":          hclspec.NewAttr("rootfs_mount_root", "string", false),
		"rootfs_max_dirty_tail_bytes": hclspec.NewDefault(
			hclspec.NewAttr("rootfs_max_dirty_tail_bytes", "number", false),
			hclspec.NewLiteral(`10737418240`),
		),
		"rootfs_nbd_devices": hclspec.NewAttr("rootfs_nbd_devices", "list(string)", false),
		"rootfs_object_type": hclspec.NewDefault(
			hclspec.NewAttr("rootfs_object_type", "string", false),
			hclspec.NewLiteral(`"s3"`),
		),
		"rootfs_object_bucket": hclspec.NewAttr("rootfs_object_bucket", "string", false),
		"rootfs_object_region": hclspec.NewDefault(
			hclspec.NewAttr("rootfs_object_region", "string", false),
			hclspec.NewLiteral(`"us-east-1"`),
		),
		"rootfs_object_endpoint":            hclspec.NewAttr("rootfs_object_endpoint", "string", false),
		"rootfs_object_access_key":          hclspec.NewAttr("rootfs_object_access_key", "string", false),
		"rootfs_object_secret_key":          hclspec.NewAttr("rootfs_object_secret_key", "string", false),
		"rootfs_authority_url":              hclspec.NewAttr("rootfs_authority_url", "string", false),
		"rootfs_authority_ca_file":          hclspec.NewAttr("rootfs_authority_ca_file", "string", false),
		"rootfs_authority_client_cert_file": hclspec.NewAttr("rootfs_authority_client_cert_file", "string", false),
		"rootfs_authority_client_key_file":  hclspec.NewAttr("rootfs_authority_client_key_file", "string", false),
		"rootfs_authority_token_file":       hclspec.NewAttr("rootfs_authority_token_file", "string", false),
		"runtime_slot_enabled": hclspec.NewDefault(
			hclspec.NewAttr("runtime_slot_enabled", "bool", false),
			hclspec.NewLiteral(`false`),
		),
		"runtime_slot_cluster_id": hclspec.NewAttr("runtime_slot_cluster_id", "string", false),
		"runtime_slot_node_boot_id_file": hclspec.NewDefault(
			hclspec.NewAttr("runtime_slot_node_boot_id_file", "string", false),
			hclspec.NewLiteral(`"/proc/sys/kernel/random/boot_id"`),
		),
	})

	taskConfigSpec = hclspec.NewObject(map[string]*hclspec.Spec{
		"command": hclspec.NewDefault(
			hclspec.NewAttr("command", "string", false),
			hclspec.NewLiteral(`"/procd"`),
		),
		"args": hclspec.NewAttr("args", "list(string)", false),
		"wait_for_claim": hclspec.NewDefault(
			hclspec.NewAttr("wait_for_claim", "bool", false),
			hclspec.NewLiteral(`true`),
		),
		"rootfs_path": hclspec.NewAttr("rootfs_path", "string", false),
	})

	capabilities = &drivers.Capabilities{
		SendSignals:         true,
		Exec:                false,
		FSIsolation:         fsisolation.Image,
		NetIsolationModes:   []drivers.NetIsolationMode{drivers.NetIsolationModeGroup},
		MustInitiateNetwork: false,
	}
)

// PluginConfig is the node-wide driver configuration.
type PluginConfig struct {
	RunscPath            string `codec:"runsc_path"`
	RunscRoot            string `codec:"runsc_root"`
	ControlDir           string `codec:"control_dir"`
	AllowedRootfsDir     string `codec:"allowed_rootfs_dir"`
	Platform             string `codec:"platform"`
	Overlay2             string `codec:"overlay2"`
	FileAccess           string `codec:"file_access"`
	DirectFS             bool   `codec:"directfs"`
	DevSmokeEnabled      bool   `codec:"dev_smoke_enabled"`
	NetworkPolicyEnabled bool   `codec:"network_policy_enabled"`

	RootFSEnabled                 bool     `codec:"rootfs_enabled"`
	RootFSSessiondSocket          string   `codec:"rootfs_sessiond_socket"`
	RootFSConsumerMountRoot       string   `codec:"rootfs_consumer_mount_root"`
	RootFSStatePath               string   `codec:"rootfs_state_path"`
	RootFSBranchRoot              string   `codec:"rootfs_branch_root"`
	RootFSMountRoot               string   `codec:"rootfs_mount_root"`
	RootFSMaxDirtyTailBytes       int64    `codec:"rootfs_max_dirty_tail_bytes"`
	RootFSNBDDevices              []string `codec:"rootfs_nbd_devices"`
	RootFSObjectType              string   `codec:"rootfs_object_type"`
	RootFSObjectBucket            string   `codec:"rootfs_object_bucket"`
	RootFSObjectRegion            string   `codec:"rootfs_object_region"`
	RootFSObjectEndpoint          string   `codec:"rootfs_object_endpoint"`
	RootFSObjectAccessKey         string   `codec:"rootfs_object_access_key"`
	RootFSObjectSecretKey         string   `codec:"rootfs_object_secret_key"`
	RootFSAuthorityURL            string   `codec:"rootfs_authority_url"`
	RootFSAuthorityCAFile         string   `codec:"rootfs_authority_ca_file"`
	RootFSAuthorityClientCertFile string   `codec:"rootfs_authority_client_cert_file"`
	RootFSAuthorityClientKeyFile  string   `codec:"rootfs_authority_client_key_file"`
	RootFSAuthorityTokenFile      string   `codec:"rootfs_authority_token_file"`

	RuntimeSlotEnabled        bool   `codec:"runtime_slot_enabled"`
	RuntimeSlotClusterID      string `codec:"runtime_slot_cluster_id"`
	RuntimeSlotNodeBootIDFile string `codec:"runtime_slot_node_boot_id_file"`
}

// TaskConfig is the per-allocation driver configuration.
type TaskConfig struct {
	Command      string   `codec:"command"`
	Args         []string `codec:"args"`
	WaitForClaim bool     `codec:"wait_for_claim"`
	RootfsPath   string   `codec:"rootfs_path"`
}

// Plugin implements a Nomad task driver for generic gVisor warm slots.
type Plugin struct {
	eventer *eventer.Eventer
	config  *PluginConfig
	tasks   *taskStore

	newRunner         func(config PluginConfig) Runsc
	newNetwork        func(config *PluginConfig) NetworkRuntime
	rootfs            RootFSRuntime
	rootfsOnce        sync.Once
	rootfsErr         error
	slotAuthority     runtimeSlotAuthority
	slotAuthorityOnce sync.Once
	slotAuthorityErr  error
	newSlotAuthority  func(*PluginConfig) (runtimeSlotAuthority, error)

	ctx    context.Context
	cancel context.CancelFunc
	logger hclog.Logger
}

// NewPlugin returns a driver plugin wired to the production runsc runner.
func NewPlugin(logger hclog.Logger) drivers.DriverPlugin {
	return newPlugin(logger, func(config PluginConfig) Runsc {
		return NewCommandRunsc(config)
	})
}

func newPlugin(logger hclog.Logger, newRunner func(config PluginConfig) Runsc) drivers.DriverPlugin {
	ctx, cancel := context.WithCancel(context.Background())
	logger = logger.Named(PluginName)
	return &Plugin{
		eventer:          eventer.NewEventer(ctx, logger),
		config:           defaultPluginConfig(),
		tasks:            newTaskStore(),
		newRunner:        newRunner,
		newNetwork:       networkRuntime,
		newSlotAuthority: newRuntimeSlotAuthority,
		ctx:              ctx,
		cancel:           cancel,
		logger:           logger,
	}
}

func defaultPluginConfig() *PluginConfig {
	return &PluginConfig{
		RunscPath:                 "/usr/local/bin/runsc",
		RunscRoot:                 "/var/run/sandbox0/runsc",
		ControlDir:                "/var/run/sandbox0/nomad-slots",
		AllowedRootfsDir:          "/var/lib/sandbox0/rootfs",
		Platform:                  "systrap",
		Overlay2:                  "none",
		FileAccess:                "shared",
		DirectFS:                  true,
		DevSmokeEnabled:           false,
		RootFSMaxDirtyTailBytes:   rootfssession.DefaultMaxDirtyTailBytes,
		RuntimeSlotNodeBootIDFile: "/proc/sys/kernel/random/boot_id",
	}
}

// PluginInfo identifies the driver to Nomad.
func (p *Plugin) PluginInfo() (*base.PluginInfoResponse, error) {
	return pluginInfo, nil
}

// ConfigSchema returns the node-wide HCL schema.
func (p *Plugin) ConfigSchema() (*hclspec.Spec, error) {
	return pluginConfigSpec, nil
}

// SetConfig validates and stores node-wide driver configuration.
func (p *Plugin) SetConfig(config *base.Config) error {
	if config == nil {
		return errors.New("nil plugin config")
	}
	decoded := defaultPluginConfig()
	if len(config.PluginConfig) != 0 {
		if err := base.MsgPackDecode(config.PluginConfig, decoded); err != nil {
			return fmt.Errorf("decode plugin config: %w", err)
		}
	}
	decoded.RunscPath = strings.TrimSpace(decoded.RunscPath)
	decoded.RunscRoot = strings.TrimSpace(decoded.RunscRoot)
	decoded.ControlDir = strings.TrimSpace(decoded.ControlDir)
	decoded.AllowedRootfsDir = strings.TrimSpace(decoded.AllowedRootfsDir)
	decoded.Platform = strings.TrimSpace(decoded.Platform)
	decoded.Overlay2 = strings.TrimSpace(decoded.Overlay2)
	decoded.FileAccess = strings.TrimSpace(decoded.FileAccess)
	if decoded.RunscPath == "" || decoded.RunscRoot == "" || decoded.ControlDir == "" || decoded.AllowedRootfsDir == "" {
		return errors.New("runsc, control, and rootfs paths must be non-empty")
	}
	if !filepath.IsAbs(decoded.RunscPath) || !filepath.IsAbs(decoded.RunscRoot) || !filepath.IsAbs(decoded.ControlDir) || !filepath.IsAbs(decoded.AllowedRootfsDir) {
		return errors.New("runsc, control, and rootfs paths must be absolute")
	}
	if decoded.Platform == "" || decoded.Overlay2 == "" || decoded.FileAccess == "" {
		return errors.New("platform, overlay2, and file_access cannot be empty")
	}
	if decoded.Overlay2 != "none" {
		return errors.New("sandbox0 gVisor driver requires overlay2=none for persistent upper writes")
	}
	decoded.RootFSObjectType = strings.TrimSpace(decoded.RootFSObjectType)
	decoded.RootFSSessiondSocket = strings.TrimSpace(decoded.RootFSSessiondSocket)
	decoded.RootFSConsumerMountRoot = strings.TrimSpace(decoded.RootFSConsumerMountRoot)
	decoded.RootFSObjectBucket = strings.TrimSpace(decoded.RootFSObjectBucket)
	decoded.RootFSObjectEndpoint = strings.TrimSpace(decoded.RootFSObjectEndpoint)
	decoded.RootFSObjectAccessKey = strings.TrimSpace(decoded.RootFSObjectAccessKey)
	decoded.RootFSObjectSecretKey = strings.TrimSpace(decoded.RootFSObjectSecretKey)
	decoded.RootFSAuthorityURL = strings.TrimSpace(decoded.RootFSAuthorityURL)
	decoded.RootFSAuthorityCAFile = strings.TrimSpace(decoded.RootFSAuthorityCAFile)
	decoded.RootFSAuthorityClientCertFile = strings.TrimSpace(decoded.RootFSAuthorityClientCertFile)
	decoded.RootFSAuthorityClientKeyFile = strings.TrimSpace(decoded.RootFSAuthorityClientKeyFile)
	decoded.RootFSAuthorityTokenFile = strings.TrimSpace(decoded.RootFSAuthorityTokenFile)
	decoded.RuntimeSlotClusterID = strings.TrimSpace(decoded.RuntimeSlotClusterID)
	decoded.RuntimeSlotNodeBootIDFile = strings.TrimSpace(decoded.RuntimeSlotNodeBootIDFile)
	if decoded.RootFSObjectType == "" {
		decoded.RootFSObjectType = "s3"
	}
	if err := validateRootFSConfig(decoded); err != nil {
		return err
	}
	if err := validateRuntimeSlotConfig(decoded); err != nil {
		return err
	}
	p.config = decoded
	return nil
}

// rootfsRuntime lazily opens the singleton bbolt journal. Nomad probes plugin
// configurations in short-lived loader processes, so doing this in SetConfig
// can briefly create competing database lock holders.
func (p *Plugin) rootfsRuntime() (RootFSRuntime, error) {
	p.rootfsOnce.Do(func() {
		if p.config.RootFSEnabled {
			p.rootfs, p.rootfsErr = newRootFSRuntime(p.config, p.logger.Named("rootfs"))
		}
	})
	if p.rootfsErr != nil {
		return nil, p.rootfsErr
	}
	return p.rootfs, nil
}

func (p *Plugin) runtimeSlotAuthority() (runtimeSlotAuthority, error) {
	p.slotAuthorityOnce.Do(func() {
		if p.config.RuntimeSlotEnabled {
			p.slotAuthority, p.slotAuthorityErr = p.newSlotAuthority(p.config)
		}
	})
	if p.slotAuthorityErr != nil {
		return nil, p.slotAuthorityErr
	}
	return p.slotAuthority, nil
}

// TaskConfigSchema returns the per-task HCL schema.
func (p *Plugin) TaskConfigSchema() (*hclspec.Spec, error) {
	return taskConfigSpec, nil
}

// Capabilities declares the driver's supported Nomad features.
func (p *Plugin) Capabilities() (*drivers.Capabilities, error) {
	return capabilities, nil
}

// Fingerprint reports whether the node can run the configured gVisor runtime.
func (p *Plugin) Fingerprint(ctx context.Context) (<-chan *drivers.Fingerprint, error) {
	channel := make(chan *drivers.Fingerprint)
	go p.handleFingerprint(ctx, channel)
	return channel, nil
}

func (p *Plugin) handleFingerprint(ctx context.Context, channel chan<- *drivers.Fingerprint) {
	defer close(channel)
	ticker := time.NewTimer(0)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-p.ctx.Done():
			return
		case <-ticker.C:
			channel <- p.buildFingerprint()
			ticker.Reset(fingerprintPeriod)
		}
	}
}

func (p *Plugin) buildFingerprint() *drivers.Fingerprint {
	runner := p.newRunner(*p.config)
	version, err := runner.Version(context.Background())
	if err != nil {
		return &drivers.Fingerprint{
			Health:            drivers.HealthStateUndetected,
			HealthDescription: fmt.Sprintf("runsc not available at %s: %v", p.config.RunscPath, err),
		}
	}
	if p.config.RootFSEnabled && p.config.RootFSSessiondSocket != "" {
		client, err := newRootFSSessionClient(p.config.RootFSSessiondSocket)
		if err != nil {
			return &drivers.Fingerprint{Health: drivers.HealthStateUndetected, HealthDescription: err.Error()}
		}
		healthCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		err = client.Ping(healthCtx)
		cancel()
		if err != nil {
			return &drivers.Fingerprint{
				Health:            drivers.HealthStateUndetected,
				HealthDescription: fmt.Sprintf("RootFS session daemon unavailable at %s: %v", p.config.RootFSSessiondSocket, err),
			}
		}
	}
	return &drivers.Fingerprint{
		Health:            drivers.HealthStateHealthy,
		HealthDescription: drivers.DriverHealthy,
		Attributes: map[string]*structs.Attribute{
			"driver.sandbox0_gvisor":                 structs.NewBoolAttribute(true),
			"driver.sandbox0_gvisor.version":         structs.NewStringAttribute(version),
			"driver.sandbox0_gvisor.platform":        structs.NewStringAttribute(p.config.Platform),
			"driver.sandbox0_gvisor.overlay2":        structs.NewStringAttribute(p.config.Overlay2),
			"driver.sandbox0_gvisor.rootfs_sessiond": structs.NewBoolAttribute(p.config.RootFSSessiondSocket != ""),
		},
	}
}

// StartTask creates a warm allocation and returns before any gVisor container exists.
func (p *Plugin) StartTask(config *drivers.TaskConfig) (*drivers.TaskHandle, *drivers.DriverNetwork, error) {
	if config == nil {
		return nil, nil, errors.New("nil task config")
	}
	if config.ID == "" {
		return nil, nil, errors.New("task ID cannot be empty")
	}
	if _, exists := p.tasks.Get(config.ID); exists {
		return nil, nil, fmt.Errorf("task %q already exists", config.ID)
	}

	var taskConfig TaskConfig
	if err := config.DecodeDriverConfig(&taskConfig); err != nil {
		return nil, nil, fmt.Errorf("decode task config: %w", err)
	}
	taskConfig.Command = strings.TrimSpace(taskConfig.Command)
	if taskConfig.Command == "" {
		taskConfig.Command = "/procd"
	}
	if !filepath.IsAbs(taskConfig.Command) {
		return nil, nil, errors.New("task command must be absolute")
	}
	if !taskConfig.WaitForClaim {
		if !p.config.DevSmokeEnabled {
			return nil, nil, errors.New("wait_for_claim=false requires dev_smoke_enabled on the client")
		}
		if taskConfig.RootfsPath == "" {
			return nil, nil, errors.New("rootfs_path is required when wait_for_claim is false")
		}
		if _, err := validateRootfsPath(taskConfig.RootfsPath, p.config.AllowedRootfsDir); err != nil {
			return nil, nil, err
		}
	}
	if err := validateRuntimeSlotTaskConfig(p.config, taskConfig); err != nil {
		return nil, nil, err
	}

	taskDir := config.TaskDir().Dir
	bundleDir := filepath.Join(taskDir, "gvisor-bundle")
	containerID := safeContainerID(config.ID)
	rootMount := filepath.Join(bundleDir, "rootfs")
	socketPath := controlSocketPath(p.config.ControlDir, config.ID)
	rootfs, err := p.rootfsRuntime()
	if err != nil {
		return nil, nil, err
	}

	handle := newTaskHandle(taskHandleOptions{
		taskConfig:        config,
		bundleDir:         bundleDir,
		containerID:       containerID,
		rootMount:         rootMount,
		socketPath:        socketPath,
		runner:            p.newRunner(*p.config),
		mounter:           systemMounter{},
		allowedRoot:       p.config.AllowedRootfsDir,
		rootfsAllowedRoot: p.config.RootFSMountRoot,
		rootfs:            rootfs,
		network:           p.newNetwork(p.config),
		logger:            p.logger.Named("task").With("task_id", config.ID, "container_id", containerID),
	})

	if err := handle.Prepare(taskConfig); err != nil {
		return nil, nil, err
	}
	if err := p.startTaskControl(handle); err != nil {
		_ = handle.Close(false)
		return nil, nil, err
	}

	nomadHandle := drivers.NewTaskHandle(taskHandleVersion)
	nomadHandle.Config = config
	if err := nomadHandle.SetDriverState(handle.PersistedState()); err != nil {
		_ = handle.Close(false)
		return nil, nil, fmt.Errorf("persist driver state: %w", err)
	}
	lifecycle, observation, err := p.activateRuntimeSlot(handle, rootfs, true)
	if err != nil {
		_ = handle.Close(false)
		return nil, nil, err
	}

	p.tasks.Set(config.ID, handle)
	p.emit(config.ID, "warm-slot-created")
	if lifecycle != nil {
		go lifecycle.runHeartbeat(p.ctx, handle.done, observation, handle.runtimeSlotHeartbeatLost)
	}

	if !taskConfig.WaitForClaim {
		if err := handle.Claim(ClaimRequest{
			RootfsPath:  taskConfig.RootfsPath,
			PolicyToken: "task-config",
			WriterEpoch: "task-config",
		}); err != nil {
			p.tasks.Delete(config.ID)
			_ = handle.Close(false)
			return nil, nil, err
		}
	}

	return nomadHandle, nil, nil
}

// RecoverTask reconstructs plugin state after a Nomad client or plugin restart.
func (p *Plugin) RecoverTask(handle *drivers.TaskHandle) error {
	if handle == nil || handle.Config == nil {
		return errors.New("task handle and config are required")
	}
	if _, exists := p.tasks.Get(handle.Config.ID); exists {
		return nil
	}
	var state PersistedState
	if err := handle.GetDriverState(&state); err != nil {
		return fmt.Errorf("decode persisted driver state: %w", err)
	}
	if state.TaskConfig == nil || state.ContainerID == "" || state.BundleDir == "" || state.RootMount == "" {
		return errors.New("persisted driver state is incomplete")
	}
	var taskConfig TaskConfig
	if err := handle.Config.DecodeDriverConfig(&taskConfig); err != nil {
		return fmt.Errorf("decode recovered task config: %w", err)
	}
	taskConfig.Command = strings.TrimSpace(taskConfig.Command)
	if taskConfig.Command == "" {
		taskConfig.Command = "/procd"
	}
	if !filepath.IsAbs(taskConfig.Command) {
		return errors.New("recovered task command must be absolute")
	}
	if err := validateRuntimeSlotTaskConfig(p.config, taskConfig); err != nil {
		return err
	}
	if state.TaskConfig.ID != handle.Config.ID || state.TaskConfig.AllocID != handle.Config.AllocID {
		return errors.New("persisted task identity does not match the Nomad task handle")
	}
	state.TaskConfig = handle.Config

	runner := p.newRunner(*p.config)
	rootfs, err := p.rootfsRuntime()
	if err != nil {
		return err
	}
	recovered := newTaskHandle(taskHandleOptions{
		taskConfig:        state.TaskConfig,
		driverConfig:      taskConfig,
		bundleDir:         state.BundleDir,
		containerID:       state.ContainerID,
		rootMount:         state.RootMount,
		socketPath:        controlSocketPath(p.config.ControlDir, state.TaskConfig.ID),
		runner:            runner,
		mounter:           systemMounter{},
		allowedRoot:       p.config.AllowedRootfsDir,
		rootfsAllowedRoot: p.config.RootFSMountRoot,
		rootfs:            rootfs,
		network:           p.newNetwork(p.config),
		logger:            p.logger.Named("task").With("task_id", state.TaskConfig.ID, "container_id", state.ContainerID),
	})
	if err := recovered.Recover(state); err != nil {
		return err
	}
	if err := p.startTaskControl(recovered); err != nil {
		return err
	}
	lifecycle, observation, err := p.activateRuntimeSlot(recovered, rootfs, false)
	if err != nil {
		p.stopTaskControl(recovered)
		return err
	}
	p.tasks.Set(state.TaskConfig.ID, recovered)
	if lifecycle != nil {
		go lifecycle.runHeartbeat(p.ctx, recovered.done, observation, recovered.runtimeSlotHeartbeatLost)
	}
	p.emit(state.TaskConfig.ID, "warm-slot-recovered")
	return nil
}

// WaitTask returns a channel that closes when the warm or active task exits.
func (p *Plugin) WaitTask(ctx context.Context, taskID string) (<-chan *drivers.ExitResult, error) {
	handle, ok := p.tasks.Get(taskID)
	if !ok {
		return nil, drivers.ErrTaskNotFound
	}
	return handle.WaitChannel(ctx), nil
}

// StopTask stops a warm or active one-shot gVisor container.
func (p *Plugin) StopTask(taskID string, timeout time.Duration, signal string) error {
	handle, ok := p.tasks.Get(taskID)
	if !ok {
		return drivers.ErrTaskNotFound
	}
	return handle.Stop(timeout, signal)
}

// DestroyTask cleans runsc state, mounts, sockets, and bundle files.
func (p *Plugin) DestroyTask(taskID string, force bool) error {
	handle, ok := p.tasks.Get(taskID)
	if !ok {
		return drivers.ErrTaskNotFound
	}
	if handle.IsRunning() && !force {
		return errors.New("cannot destroy running task")
	}
	if err := handle.Close(force); err != nil {
		p.logger.Error("task cleanup failed", "task_id", taskID, "error", err)
		return fmt.Errorf("cleanup task %s: %w", taskID, err)
	}
	p.tasks.Delete(taskID)
	return nil
}

// InspectTask returns driver-owned status for a warm or active slot.
func (p *Plugin) InspectTask(taskID string) (*drivers.TaskStatus, error) {
	handle, ok := p.tasks.Get(taskID)
	if !ok {
		return nil, drivers.ErrTaskNotFound
	}
	return handle.TaskStatus(), nil
}

// TaskStats is intentionally minimal until cgroup accounting is integrated.
func (p *Plugin) TaskStats(ctx context.Context, taskID string, interval time.Duration) (<-chan *drivers.TaskResourceUsage, error) {
	if _, ok := p.tasks.Get(taskID); !ok {
		return nil, drivers.ErrTaskNotFound
	}
	channel := make(chan *drivers.TaskResourceUsage)
	go func() {
		defer close(channel)
		<-ctx.Done()
	}()
	return channel, nil
}

// TaskEvents returns driver lifecycle events.
func (p *Plugin) TaskEvents(ctx context.Context) (<-chan *drivers.TaskEvent, error) {
	return p.eventer.TaskEvents(ctx)
}

// SignalTask forwards a signal to an active container. Warm slots reject signals.
func (p *Plugin) SignalTask(taskID string, signal string) error {
	handle, ok := p.tasks.Get(taskID)
	if !ok {
		return drivers.ErrTaskNotFound
	}
	return handle.Signal(signal)
}

// ExecTask is not supported because guest execution belongs to procd.
func (p *Plugin) ExecTask(taskID string, command []string, timeout time.Duration) (*drivers.ExecTaskResult, error) {
	return nil, errors.New("exec is not supported by the sandbox0 gVisor driver")
}

func (p *Plugin) emit(taskID, message string) {
	_ = p.eventer.EmitEvent(&drivers.TaskEvent{TaskID: taskID, Timestamp: time.Now(), Message: message})
}

func safeContainerID(taskID string) string {
	sum := sha256.Sum256([]byte(taskID))
	return "s0-" + hex.EncodeToString(sum[:16])
}

func controlSocketPath(controlDir, taskID string) string {
	sum := sha256.Sum256([]byte(taskID))
	return filepath.Join(controlDir, hex.EncodeToString(sum[:12])+".sock")
}
