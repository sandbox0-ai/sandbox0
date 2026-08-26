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

package nomadruntime

import (
	"bufio"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/containerd/errdefs"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsrebase"
	rootfssession "github.com/sandbox0-ai/sandbox0/pkg/rootfssession"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"go.uber.org/zap"
)

const (
	rootFSSessionReconcileInterval  = time.Second
	rootFSSessionAttachGrace        = 2 * time.Minute
	rootFSSessionReconcileTimeout   = 3 * time.Minute
	runtimeSlotJournalPruneInterval = time.Minute
	nomadAllocationResponseMaxBytes = 64 << 20
	nodeRuntimeStartupTimeout       = 30 * time.Second
	nodeRuntimeHealthInterval       = time.Second
)

// Service is the HA-primary-scoped Nomad node runtime owned by ctld.
type Service struct {
	config      Config
	nomadConfig NomadAllocationConfig
	logger      *zap.Logger
	ready       atomic.Bool
}

// NewService validates a ctld Nomad runtime without opening its exclusive
// Bolt, NBD, mount, runsc, or Unix-socket resources.
func NewService(config Config, nomadConfig NomadAllocationConfig, logger *zap.Logger) (*Service, error) {
	config.ApplyDefaults()
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("validate ctld Nomad runtime config: %w", err)
	}
	if err := validateNomadAllocationConfig(nomadConfig); err != nil {
		return nil, err
	}
	return &Service{config: config, nomadConfig: nomadConfig, logger: logger}, nil
}

// Ready reports whether the active ctld primary owns a healthy local runtime.
func (s *Service) Ready() bool {
	return s != nil && s.ready.Load()
}

// Run owns the complete privileged runtime until the ctld primary context is
// canceled. RPC health controls readiness, but only confirmed runtime exit is
// terminal; an unresponsive owner must not trigger overlapping HA promotion.
func (s *Service) Run(ctx context.Context) error {
	if s == nil {
		return fmt.Errorf("ctld Nomad runtime service is nil")
	}
	serviceCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	s.ready.Store(false)
	defer s.ready.Store(false)

	runErrors := make(chan error, 1)
	go func() {
		runErrors <- run(serviceCtx, s.config, s.nomadConfig, s.logger)
	}()
	client, err := NewClient(s.config.SocketPath)
	if err != nil {
		cancel()
		return errors.Join(err, <-runErrors)
	}
	startupDeadline := time.NewTimer(nodeRuntimeStartupTimeout)
	defer startupDeadline.Stop()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	started := false
	for {
		select {
		case err := <-runErrors:
			if ctx.Err() != nil && (err == nil || errors.Is(err, context.Canceled)) {
				return nil
			}
			if err == nil {
				err = fmt.Errorf("ctld Nomad runtime stopped unexpectedly")
			}
			return err
		case <-ctx.Done():
			cancel()
			err := <-runErrors
			if err == nil || errors.Is(err, context.Canceled) {
				return nil
			}
			return err
		case <-startupDeadline.C:
			if !started {
				cancel()
				return errors.Join(fmt.Errorf("ctld Nomad runtime startup deadline exceeded"), <-runErrors)
			}
		case <-ticker.C:
			healthCtx, healthCancel := context.WithTimeout(serviceCtx, 2*time.Second)
			healthErr := client.Ping(healthCtx)
			healthCancel()
			if healthErr != nil {
				s.ready.Store(false)
				continue
			}
			s.ready.Store(true)
			if !started {
				started = true
				s.ready.Store(true)
				ticker.Reset(nodeRuntimeHealthInterval)
			}
		}
	}
}

type nodeRuntime struct {
	runtime nodeRuntimeBackend
	runner  Runsc
	mounter Mounter
	config  Config
	logger  logger

	mu                 sync.Mutex
	wg                 sync.WaitGroup
	inflight           map[string]*reconciliationState
	preempting         map[string]bool
	trigger            chan string
	allocations        nomadAllocationSource
	runtimeSlotNetwork runtimeSlotNetworkControl
	resourceCgroups    runtimeResourceCgroup
	journal            *runtimeSlotJournal
	lastJournalPrune   time.Time
	clusterID          string
	nodeID             string
	nodeUID            string
}

type reconciliationState struct {
	cancel context.CancelFunc
	done   chan struct{}
}

type nodeRuntimeBackend interface {
	Runtime
	RecoverySessions() ([]rootfssession.RecoverySession, error)
	FenceLocalRootFSWriter(context.Context, rootfshandoff.StageRequest, string, CrashTaskObservation) (rootfshandoff.CrashFenceProof, error)
	ReclaimVerifiedTerminal(context.Context, rootfshandoff.StageRequest) error
	ReclaimExternallyRetired(context.Context, rootfshandoff.StageRequest) (bool, error)
}

type pausedRebaseRuntime interface {
	ExecutePausedRebase(context.Context, rootfsrebase.WorkerRequest) (rootfsrebase.WorkerResult, error)
	RejectPausedRebase(context.Context, rootfsrebase.WorkerRequest) (rootfsrebase.WorkerRejection, error)
	AcknowledgePausedRebase(rootfsrebase.WorkerRequest, string) error
}

type dirtyTailPressureRuntime interface {
	DirtyTailPressureSignal() <-chan struct{}
	DirtyTailPressureSessions() ([]rootfssession.DirtyTailPressureSession, error)
	PlanDirtyTailPressure(context.Context, rootfssession.DirtyTailPressureSession) (string, error)
}

// RejectPausedRootFSRebase serializes termination with exact node execution.
func (d *nodeRuntime) RejectPausedRootFSRebase(
	ctx context.Context,
	target protocol.NodeChannelTarget,
	request protocol.NodePausedRebaseControlRequest,
) (rootfsrebase.WorkerRejection, error) {
	if err := request.Validate(); err != nil || !request.Reject || request.AcknowledgeProofDigest != "" {
		return rootfsrebase.WorkerRejection{},
			fmt.Errorf("validate paused RootFS rebase rejection: %v: %w", err, errdefs.ErrInvalidArgument)
	}
	if d == nil || d.runtime == nil || target.ClusterID != d.clusterID || target.NodeID != d.nodeID ||
		target.NodeUID != d.nodeUID || target.SlotID != "" || target.AllocationID != "" || target.ControlEndpoint != "" {
		return rootfsrebase.WorkerRejection{},
			fmt.Errorf("paused RootFS rebase rejection target is invalid: %w", errdefs.ErrPermissionDenied)
	}
	runtime, ok := d.runtime.(pausedRebaseRuntime)
	if !ok {
		return rootfsrebase.WorkerRejection{},
			fmt.Errorf("paused RootFS rebase runtime is unavailable: %w", errdefs.ErrFailedPrecondition)
	}
	return runtime.RejectPausedRebase(ctx, request.Worker)
}

// AcknowledgePausedRootFSRebase releases one exact cached node result after a
// permanent regional outcome.
func (d *nodeRuntime) AcknowledgePausedRootFSRebase(
	_ context.Context,
	target protocol.NodeChannelTarget,
	request protocol.NodePausedRebaseControlRequest,
) error {
	if err := request.Validate(); err != nil || request.AcknowledgeProofDigest == "" {
		return fmt.Errorf("validate paused RootFS rebase acknowledgement: %v: %w", err, errdefs.ErrInvalidArgument)
	}
	if d == nil || d.runtime == nil || target.ClusterID != d.clusterID || target.NodeID != d.nodeID ||
		target.NodeUID != d.nodeUID || target.SlotID != "" || target.AllocationID != "" || target.ControlEndpoint != "" {
		return fmt.Errorf("paused RootFS rebase acknowledgement target is invalid: %w", errdefs.ErrPermissionDenied)
	}
	runtime, ok := d.runtime.(pausedRebaseRuntime)
	if !ok {
		return fmt.Errorf("paused RootFS rebase runtime is unavailable: %w", errdefs.ErrFailedPrecondition)
	}
	return runtime.AcknowledgePausedRebase(request.Worker, request.AcknowledgeProofDigest)
}

// ExecutePausedRootFSRebase runs an offline worker without consulting a
// runtime-slot journal. The command is already bound to this authenticated
// node boot and carries a PostgreSQL-created immutable pre-operation.
func (d *nodeRuntime) ExecutePausedRootFSRebase(
	ctx context.Context,
	target protocol.NodeChannelTarget,
	request protocol.NodePausedRebaseControlRequest,
) (rootfsrebase.WorkerResult, error) {
	if err := request.Validate(); err != nil || request.Reject || request.AcknowledgeProofDigest != "" {
		return rootfsrebase.WorkerResult{},
			fmt.Errorf("validate paused RootFS rebase request: %w: %w", err, errdefs.ErrInvalidArgument)
	}
	if d == nil || d.runtime == nil {
		return rootfsrebase.WorkerResult{},
			fmt.Errorf("paused RootFS rebase runtime is unavailable: %w", errdefs.ErrUnavailable)
	}
	if target.ClusterID != d.clusterID || target.NodeID != d.nodeID || target.NodeUID != d.nodeUID ||
		target.SlotID != "" || target.AllocationID != "" || target.ControlEndpoint != "" {
		return rootfsrebase.WorkerResult{},
			fmt.Errorf("paused RootFS rebase target does not match this daemon: %w", errdefs.ErrPermissionDenied)
	}
	runtime, ok := d.runtime.(pausedRebaseRuntime)
	if !ok {
		return rootfsrebase.WorkerResult{},
			fmt.Errorf("paused RootFS rebase runtime is unavailable: %w", errdefs.ErrFailedPrecondition)
	}
	return runtime.ExecutePausedRebase(ctx, request.Worker)
}

type nomadAllocationSource interface {
	ActiveAllocations(context.Context) (map[string]bool, error)
}

// NomadAllocationConfig identifies the server-side allocation catalog used as
// a reconciliation trigger when the local task-driver process misses Destroy.
type NomadAllocationConfig struct {
	ClusterID                     string
	Address                       string
	NodeID                        string
	Namespace                     string
	JobID                         string
	TokenFile                     string
	CAFile                        string
	CertFile                      string
	KeyFile                       string
	RuntimeSlotChannelEnabled     bool
	RuntimeSlotNodeUID            string
	RuntimeSlotChannelPeerURISAN  string
	RuntimeSlotControlRoot        string
	RuntimeSlotNodeControlTimeout time.Duration
	RuntimeSlotCtldNetworkSocket  string
	RuntimeResourceCPUMillicores  int64
	RuntimeResourceMemoryBytes    int64
	RuntimeResourceCPUSetCPUs     string
	RuntimeResourceCPUSetMems     string
}

// RunNodeRuntime runs the node-scoped owner for writer leases,
// NBD/XFS/Overlay sessions, and terminal reconciliation. The Nomad task-driver
// process talks to it only over the root-owned Unix socket.
func run(
	ctx context.Context,
	config Config,
	nomadConfig NomadAllocationConfig,
	zapLogger *zap.Logger,
) error {
	config.ApplyDefaults()
	if err := config.Validate(); err != nil {
		return fmt.Errorf("validate ctld Nomad runtime config: %w", err)
	}
	logger := newLogger(zapLogger)
	if strings.TrimSpace(config.RootFSConsumerMountRoot) == "" {
		return fmt.Errorf("rootfs_consumer_mount_root is required for the ctld Nomad runtime")
	}
	if strings.TrimSpace(config.RootFSConsumerNetNSRoot) == "" {
		return fmt.Errorf("rootfs_consumer_netns_root is required for the ctld Nomad runtime")
	}
	if strings.TrimSpace(config.RootFSAuthorityURL) == "" {
		return fmt.Errorf("rootfs_authority_url is required for the ctld Nomad runtime")
	}
	if strings.TrimSpace(nomadConfig.ClusterID) == "" {
		return fmt.Errorf("cluster_id is required for the ctld Nomad runtime")
	}
	if strings.TrimSpace(nomadConfig.RuntimeSlotCtldNetworkSocket) == "" {
		return fmt.Errorf("runtime_slot_ctld_network_socket is required for warm-slot default deny")
	}
	if strings.TrimSpace(nomadConfig.RuntimeSlotNodeUID) == "" {
		return fmt.Errorf("runtime_slot_node_uid is required for ctld network identity")
	}
	if !filepath.IsAbs(strings.TrimSpace(config.RuntimeSlotJournalPath)) || filepath.Clean(config.RuntimeSlotJournalPath) == "/" {
		return fmt.Errorf("runtime_slot_journal_path must be a non-root absolute path")
	}
	for name, value := range map[string]string{
		"runsc": config.RunscPath, "runsc_root": config.RunscRoot,
		"rootfs_consumer_mount_root": config.RootFSConsumerMountRoot,
		"rootfs_consumer_netns_root": config.RootFSConsumerNetNSRoot,
	} {
		if !filepath.IsAbs(value) || filepath.Clean(value) == "/" {
			return fmt.Errorf("%s must be a non-root absolute path", name)
		}
	}
	consumerMountRoot, err := filepath.EvalSymlinks(config.RootFSConsumerMountRoot)
	if err != nil {
		return fmt.Errorf("resolve rootfs_consumer_mount_root: %w", err)
	}
	config.RootFSConsumerMountRoot = consumerMountRoot
	if err := os.MkdirAll(config.RootFSConsumerNetNSRoot, 0o755); err != nil {
		return fmt.Errorf("create rootfs_consumer_netns_root: %w", err)
	}
	consumerNetNSRoot, err := filepath.EvalSymlinks(config.RootFSConsumerNetNSRoot)
	if err != nil {
		return fmt.Errorf("resolve rootfs_consumer_netns_root: %w", err)
	}
	config.RootFSConsumerNetNSRoot = consumerNetNSRoot
	if err := validateRootFSConfig(&config); err != nil {
		return err
	}
	allocations, err := newNomadAllocationSource(nomadConfig)
	if err != nil {
		return err
	}
	if allocations == nil {
		return fmt.Errorf("nomad allocation authority is required for the ctld Nomad runtime")
	}
	journal, err := newRuntimeSlotJournal(config.RuntimeSlotJournalPath, runtimeSlotProofRetention)
	if err != nil {
		return err
	}
	defer journal.Close()
	if _, err := journal.Prune(time.Now()); err != nil {
		return fmt.Errorf("prune runtime slot journal: %w", err)
	}
	runtime, err := newRuntime(ctx, &config, logger.Named("runtime"))
	if err != nil {
		return err
	}
	defer runtime.Close()
	resourceCgroups, err := newRuntimeResourceCgroup(
		config.RuntimeResourceCgroupRoot,
		runtimeNodeCapacity(nomadConfig),
	)
	if err != nil {
		return fmt.Errorf("open runtime resource cgroup root: %w", err)
	}
	var runtimeSlotNetwork runtimeSlotNetworkControl
	if controlSocket := strings.TrimSpace(nomadConfig.RuntimeSlotCtldNetworkSocket); controlSocket != "" {
		runtimeSlotNetwork, err = protocol.NewRuntimeSlotNetworkClient(
			controlSocket,
			runtimeSlotNodeControlTimeout(nomadConfig),
		)
		if err != nil {
			return fmt.Errorf("create ctld runtime slot network client: %w", err)
		}
	}
	daemon := &nodeRuntime{
		runtime: runtime, runner: newCommandRunsc(config), mounter: systemMounter{},
		config: config, logger: logger, inflight: make(map[string]*reconciliationState), trigger: make(chan string, 128),
		allocations: allocations, runtimeSlotNetwork: runtimeSlotNetwork, journal: journal,
		clusterID: strings.TrimSpace(nomadConfig.ClusterID), nodeID: strings.TrimSpace(nomadConfig.NodeID),
		nodeUID:         strings.TrimSpace(nomadConfig.RuntimeSlotNodeUID),
		resourceCgroups: resourceCgroups,
	}
	daemonCtx, cancelDaemon := context.WithCancel(ctx)
	defer cancelDaemon()
	nodeChannelAgent, err := newNodeRuntimeChannelAgent(config, nomadConfig, daemon, runtimeSlotNetwork, resourceCgroups)
	if err != nil {
		return err
	}
	daemon.wg.Add(1)
	go func() {
		defer daemon.wg.Done()
		daemon.reconcileLoop(daemonCtx)
	}()
	var nodeChannelErr <-chan error
	if nodeChannelAgent != nil {
		errorsCh := make(chan error, 1)
		nodeChannelErr = errorsCh
		go func() {
			agentErr := nodeChannelAgent.Run(daemonCtx)
			errorsCh <- agentErr
			if agentErr != nil && daemonCtx.Err() == nil {
				cancelDaemon()
			}
		}()
	}
	err = serveNodeRuntime(daemonCtx, config.SocketPath, runtime, daemon.writerLeaseLost, daemon.health, daemon)
	cancelDaemon()
	if nodeChannelErr != nil {
		agentErr := <-nodeChannelErr
		if err == nil && ctx.Err() == nil && !errors.Is(agentErr, context.Canceled) {
			err = fmt.Errorf("runtime slot node channel stopped: %w", agentErr)
		}
	}
	daemon.wg.Wait()
	return err
}

func validateNomadAllocationConfig(config NomadAllocationConfig) error {
	for name, value := range map[string]string{
		"cluster_id":                       config.ClusterID,
		"nomad_address":                    config.Address,
		"nomad_node_id":                    config.NodeID,
		"nomad_namespace":                  config.Namespace,
		"nomad_job_id":                     config.JobID,
		"runtime_slot_node_uid":            config.RuntimeSlotNodeUID,
		"runtime_slot_ctld_network_socket": config.RuntimeSlotCtldNetworkSocket,
	} {
		trimmed := strings.TrimSpace(value)
		if value != trimmed || trimmed == "" || len(value) > 512 {
			return fmt.Errorf("%s is required for the ctld Nomad runtime and must not exceed 512 bytes", name)
		}
	}
	if !config.RuntimeSlotChannelEnabled {
		return fmt.Errorf("runtime slot node channel is required for the ctld Nomad runtime")
	}
	capacity := runtimeNodeCapacity(config)
	if err := capacity.Validate(); err != nil {
		return fmt.Errorf("runtime node capacity: %w", err)
	}
	if err := validateCanonicalHTTPSOrigin("nomad_address", config.Address); err != nil {
		return err
	}
	for _, file := range []struct{ name, value string }{
		{"nomad_token_file", config.TokenFile},
		{"nomad_ca_file", config.CAFile},
		{"nomad_cert_file", config.CertFile},
		{"nomad_key_file", config.KeyFile},
	} {
		if err := validateCanonicalAbsolutePath(file.name, file.value); err != nil {
			return err
		}
	}
	if err := validateCanonicalAbsolutePath("runtime_slot_control_root", config.RuntimeSlotControlRoot); err != nil {
		return err
	}
	if err := validateCanonicalAbsolutePath("runtime_slot_ctld_network_socket", config.RuntimeSlotCtldNetworkSocket); err != nil {
		return err
	}
	if timeout := config.RuntimeSlotNodeControlTimeout; timeout != 0 &&
		(timeout < time.Second || timeout > time.Minute) {
		return fmt.Errorf("runtime_slot_node_control_timeout must be between one second and one minute")
	}
	peerURI, err := url.Parse(config.RuntimeSlotChannelPeerURISAN)
	if err != nil || peerURI.Scheme != "spiffe" || peerURI.Host == "" || peerURI.User != nil ||
		peerURI.RawQuery != "" || peerURI.Fragment != "" || peerURI.String() != config.RuntimeSlotChannelPeerURISAN ||
		len(config.RuntimeSlotChannelPeerURISAN) > 2048 {
		return fmt.Errorf("runtime_slot_channel_peer_uri_san must be canonical SPIFFE")
	}
	return nil
}

func runtimeNodeCapacity(config NomadAllocationConfig) protocol.NodeChannelCapacity {
	return protocol.NodeChannelCapacity{
		CPUMillicores:   config.RuntimeResourceCPUMillicores,
		MemoryBytes:     config.RuntimeResourceMemoryBytes,
		CPUSetCPUs:      config.RuntimeResourceCPUSetCPUs,
		CPUSetMems:      config.RuntimeResourceCPUSetMems,
		TTLMilliseconds: protocol.DefaultNodeChannelCapacityTTLMilliseconds,
	}
}

// RegisterRuntimeSlot records all physical warm-slot identities before the
// regional authority is allowed to expose that slot as fast-path ready.
func (d *nodeRuntime) RegisterRuntimeSlot(
	ctx context.Context,
	registration RuntimeSlotRegistration,
) error {
	if err := registration.Validate(); err != nil {
		return fmt.Errorf("validate runtime slot journal registration: %w: %w", err, errdefs.ErrInvalidArgument)
	}
	if d.journal == nil {
		return fmt.Errorf("runtime slot journal is unavailable: %w", errdefs.ErrUnavailable)
	}
	if registration.ClusterID != d.clusterID || registration.NodeID != d.nodeID {
		return fmt.Errorf("runtime slot registration target does not match this daemon: %w", errdefs.ErrPermissionDenied)
	}
	stableMount, err := validateRootfsPath(registration.StableMount, d.config.RootFSConsumerMountRoot)
	if err != nil {
		return fmt.Errorf("validate runtime slot stable mount: %w: %w", err, errdefs.ErrFailedPrecondition)
	}
	stableMountID, err := stableMountIdentity(stableMount)
	if err != nil {
		return err
	}
	if stableMountID != registration.StableMountID {
		return fmt.Errorf("runtime slot stable mount incarnation changed: %w", errdefs.ErrFailedPrecondition)
	}
	mountNamespaceID, err := os.Readlink("/proc/self/ns/mnt")
	if err != nil {
		return fmt.Errorf("read ctld Nomad runtime mount namespace: %w", err)
	}
	if mountNamespaceID != registration.MountNamespaceID {
		return fmt.Errorf("runtime slot mount namespace differs from ctld Nomad runtime: %w", errdefs.ErrFailedPrecondition)
	}
	netnsPath, err := validateExistingPath(registration.NetNSPath, d.config.RootFSConsumerNetNSRoot)
	if err != nil {
		return fmt.Errorf("validate runtime slot network namespace: %w: %w", err, errdefs.ErrFailedPrecondition)
	}
	netnsIdentity, err := networkNamespaceIdentity(netnsPath)
	if err != nil {
		return err
	}
	if netnsIdentity != registration.NetNSIdentity {
		return fmt.Errorf("runtime slot network namespace incarnation changed: %w", errdefs.ErrFailedPrecondition)
	}
	registration.StableMount = stableMount
	registration.NetNSPath = netnsPath
	if err := d.journal.Register(registration); err != nil {
		return err
	}
	if d.runtimeSlotNetwork == nil {
		return fmt.Errorf("ctld runtime slot network control is unavailable: %w", errdefs.ErrUnavailable)
	}
	local, err := d.runtimeSlotNetworkRegistrationRequest(registration)
	if err != nil {
		return err
	}
	if err := d.runtimeSlotNetwork.Register(ctx, local); err != nil {
		return fmt.Errorf("apply ctld warm-slot default-deny policy: %w", err)
	}
	return nil
}

func (d *nodeRuntime) runtimeSlotNetworkRegistrationRequest(
	registration RuntimeSlotRegistration,
) (protocol.RuntimeSlotNetworkRegistrationRequest, error) {
	if d == nil || d.clusterID == "" || d.nodeID == "" || d.nodeUID == "" {
		return protocol.RuntimeSlotNetworkRegistrationRequest{}, fmt.Errorf("runtime slot network daemon identity is unavailable: %w", errdefs.ErrUnavailable)
	}
	if registration.ClusterID != d.clusterID || registration.NodeID != d.nodeID {
		return protocol.RuntimeSlotNetworkRegistrationRequest{}, fmt.Errorf("runtime slot network registration does not match this daemon: %w", errdefs.ErrPermissionDenied)
	}
	consumer := &rootfssession.ConsumerRegistration{
		NetNSPath: registration.NetNSPath, NetNSIdentity: registration.NetNSIdentity,
		NetworkChain: registration.NetworkChain,
	}
	resolved, err := d.runtimeSlotNetworkPath(consumer, registration.NetNSIdentity)
	if err != nil {
		return protocol.RuntimeSlotNetworkRegistrationRequest{}, err
	}
	if resolved == "" {
		return protocol.RuntimeSlotNetworkRegistrationRequest{}, fmt.Errorf("runtime slot network namespace is absent: %w", errdefs.ErrFailedPrecondition)
	}
	relative, err := filepath.Rel(d.config.RootFSConsumerNetNSRoot, resolved)
	if err != nil {
		return protocol.RuntimeSlotNetworkRegistrationRequest{}, fmt.Errorf("derive ctld network namespace relative path: %w", err)
	}
	request := protocol.RuntimeSlotNetworkRegistrationRequest{
		SlotID: registration.SlotID, ClusterID: registration.ClusterID,
		AllocationID: registration.AllocationID, NodeID: registration.NodeID,
		NodeUID: d.nodeUID, NodeBootID: registration.NodeBootID,
		NetNSIdentity: registration.NetNSIdentity, NetNSRelativePath: relative,
	}
	if err := request.Validate(); err != nil {
		return protocol.RuntimeSlotNetworkRegistrationRequest{}, fmt.Errorf("validate ctld network registration request: %w: %w", err, errdefs.ErrFailedPrecondition)
	}
	return request, nil
}

func (d *nodeRuntime) runtimeSlotNetworkPrepareRequest(
	request protocol.NodeNetworkPrepareControlRequest,
) (protocol.RuntimeSlotNetworkPrepareRequest, error) {
	if err := request.Validate(); err != nil {
		return protocol.RuntimeSlotNetworkPrepareRequest{}, fmt.Errorf("validate runtime slot network prepare request: %w: %w", err, errdefs.ErrInvalidArgument)
	}
	if d == nil || d.journal == nil || d.runtimeSlotNetwork == nil {
		return protocol.RuntimeSlotNetworkPrepareRequest{}, fmt.Errorf("ctld runtime slot network control is unavailable: %w", errdefs.ErrUnavailable)
	}
	if request.ClusterID != d.clusterID || request.NodeID != d.nodeID {
		return protocol.RuntimeSlotNetworkPrepareRequest{}, fmt.Errorf("runtime slot network request does not match this daemon: %w", errdefs.ErrPermissionDenied)
	}
	record, err := d.journal.Get(request.SlotID)
	if err != nil {
		return protocol.RuntimeSlotNetworkPrepareRequest{}, fmt.Errorf("read runtime slot network registration: %w", err)
	}
	registration := record.Registration
	localRegistration, err := d.runtimeSlotNetworkRegistrationRequest(registration)
	if err != nil {
		return protocol.RuntimeSlotNetworkPrepareRequest{}, err
	}
	if !localRegistration.MatchesPrepare(request) {
		return protocol.RuntimeSlotNetworkPrepareRequest{}, fmt.Errorf("runtime slot network registration belongs to another incarnation: %w", errdefs.ErrFailedPrecondition)
	}
	local := protocol.RuntimeSlotNetworkPrepareRequest{
		Request: request, NetNSRelativePath: localRegistration.NetNSRelativePath,
	}
	if err := local.Validate(); err != nil {
		return protocol.RuntimeSlotNetworkPrepareRequest{}, fmt.Errorf("validate ctld network prepare request: %w: %w", err, errdefs.ErrFailedPrecondition)
	}
	return local, nil
}

// CaptureRunningRootFSFork resolves the exact live writer from durable node
// state, then lets the RootFS runtime freeze, checkpoint, and regionally
// publish it. No task-driver plugin process participates in this path.
func (d *nodeRuntime) CaptureRunningRootFSFork(
	ctx context.Context,
	target protocol.NodeChannelTarget,
	request protocol.NodeRunningForkControlRequest,
) (rootfshandoff.RunningForkCheckpointResult, error) {
	if err := request.Validate(); err != nil {
		return rootfshandoff.RunningForkCheckpointResult{},
			fmt.Errorf("validate running RootFS fork request: %w: %w", err, errdefs.ErrInvalidArgument)
	}
	if d == nil || d.runtime == nil || d.journal == nil {
		return rootfshandoff.RunningForkCheckpointResult{},
			fmt.Errorf("running RootFS fork dependencies are unavailable: %w", errdefs.ErrUnavailable)
	}
	if target.ClusterID != d.clusterID || target.NodeID != d.nodeID || target.NodeUID != d.nodeUID {
		return rootfshandoff.RunningForkCheckpointResult{},
			fmt.Errorf("running RootFS fork target does not match this daemon: %w", errdefs.ErrPermissionDenied)
	}
	journalRecord, err := d.journal.Get(target.SlotID)
	if err != nil {
		return rootfshandoff.RunningForkCheckpointResult{}, fmt.Errorf("read running-fork slot journal: %w", err)
	}
	registration := journalRecord.Registration
	if registration.ClusterID != target.ClusterID || registration.NodeID != target.NodeID ||
		registration.NodeBootID != target.NodeBootID || registration.AllocationID != target.AllocationID {
		return rootfshandoff.RunningForkCheckpointResult{},
			fmt.Errorf("running RootFS fork slot incarnation changed: %w", errdefs.ErrFailedPrecondition)
	}
	sessions, err := d.runtime.RecoverySessions()
	if err != nil {
		return rootfshandoff.RunningForkCheckpointResult{}, fmt.Errorf("list running-fork RootFS sessions: %w", err)
	}
	var matched *rootfssession.RecoverySession
	for index := range sessions {
		stage := sessions[index].Stage
		if stage.Identity.SlotNonce != target.SlotID || stage.Identity.AllocationID != target.AllocationID ||
			stage.Identity.NodeUID != target.NodeUID || stage.Identity.BootID != target.NodeBootID ||
			stage.Identity.RootFSID != request.SourceFilesystemID ||
			stage.Identity.WriterGrantID != request.SourceWriterGrantID ||
			stage.Identity.WriterEpoch != request.SourceWriterEpoch ||
			stage.BindingVersion != request.BindingVersion ||
			stage.InitialGeneration != request.ExpectedSourceGenerationID {
			continue
		}
		binding, bindingErr := stage.BindingDigest()
		if bindingErr != nil || hex.EncodeToString(binding[:]) != request.BindingDigest {
			continue
		}
		if matched != nil {
			return rootfshandoff.RunningForkCheckpointResult{},
				fmt.Errorf("multiple RootFS sessions match the running fork: %w", errdefs.ErrFailedPrecondition)
		}
		copy := sessions[index]
		matched = &copy
	}
	if matched == nil {
		return rootfshandoff.RunningForkCheckpointResult{},
			fmt.Errorf("exact live RootFS writer session is missing: %w", errdefs.ErrNotFound)
	}
	if !matched.Live || matched.Consumer == nil {
		return rootfshandoff.RunningForkCheckpointResult{},
			fmt.Errorf("exact RootFS writer is not live and attached: %w", errdefs.ErrFailedPrecondition)
	}
	return d.runtime.CaptureRunningFork(ctx, matched.Stage, request.Fork)
}

// CleanupRuntimeSlot removes one exact runtime without calling the Nomad task
// driver or choosing a new regional writer outcome. It may replay the exact
// already-authoritative planned operation. The proof is journaled before it is
// returned, making a lost response byte-stable on retry.
func (d *nodeRuntime) CleanupRuntimeSlot(
	ctx context.Context,
	request protocol.NodeCleanupControlRequest,
) (protocol.NodeCleanupControlProof, error) {
	if err := request.Validate(); err != nil {
		return protocol.NodeCleanupControlProof{}, fmt.Errorf("validate node cleanup request: %w: %w", err, errdefs.ErrInvalidArgument)
	}
	if d.clusterID == "" || d.nodeID == "" || request.ClusterID != d.clusterID || request.NodeID != d.nodeID {
		return protocol.NodeCleanupControlProof{}, fmt.Errorf("node cleanup target does not match this daemon: %w", errdefs.ErrPermissionDenied)
	}
	var journalRecord *runtimeSlotJournalRecord
	if d.journal != nil {
		record, err := d.journal.BeginCleanup(request)
		if err == nil {
			journalRecord = &record
			if record.Proof != nil {
				return *record.Proof, nil
			}
		} else if request.WriterGrantID == "" || !errdefs.IsNotFound(err) {
			return protocol.NodeCleanupControlProof{}, fmt.Errorf("begin runtime slot cleanup journal: %w", err)
		}
	} else if request.WriterGrantID == "" {
		return protocol.NodeCleanupControlProof{}, fmt.Errorf("grantless runtime cleanup requires the node slot journal: %w", errdefs.ErrFailedPrecondition)
	}
	if d.runner == nil || d.mounter == nil || d.runtimeSlotNetwork == nil {
		return protocol.NodeCleanupControlProof{}, fmt.Errorf("node runtime cleanup dependencies are unavailable: %w", errdefs.ErrUnavailable)
	}
	if !request.Resources.IsZero() && d.resourceCgroups == nil {
		return protocol.NodeCleanupControlProof{}, fmt.Errorf("runtime resource cgroup cleanup is unavailable: %w", errdefs.ErrUnavailable)
	}
	if request.WriterGrantID != "" && d.runtime == nil {
		return protocol.NodeCleanupControlProof{}, fmt.Errorf("RootFS runtime cleanup dependency is unavailable: %w", errdefs.ErrUnavailable)
	}
	if err := d.beginExternalReconciliation(ctx, request.SlotID); err != nil {
		return protocol.NodeCleanupControlProof{}, fmt.Errorf("runtime slot cleanup is already in progress: %w", err)
	}
	defer d.endReconciliation(request.SlotID)
	if request.WriterGrantID == "" {
		return d.cleanupGrantlessRuntimeSlot(ctx, request, *journalRecord)
	}
	return d.cleanupWriterRuntimeSlot(ctx, request, journalRecord)
}

func (d *nodeRuntime) cleanupWriterRuntimeSlot(
	ctx context.Context,
	request protocol.NodeCleanupControlRequest,
	journalRecord *runtimeSlotJournalRecord,
) (protocol.NodeCleanupControlProof, error) {
	sessions, err := d.runtime.RecoverySessions()
	if err != nil {
		return protocol.NodeCleanupControlProof{}, fmt.Errorf("list durable RootFS sessions: %w", err)
	}
	matched, err := matchRuntimeSlotCleanupSession(sessions, request)
	if err != nil {
		return protocol.NodeCleanupControlProof{}, err
	}
	if matched == nil {
		if journalRecord == nil {
			return protocol.NodeCleanupControlProof{}, fmt.Errorf("RootFS writer session and runtime slot journal are absent: %w", errdefs.ErrNotFound)
		}
		return d.cleanupJournaledWriterRuntimeSlot(ctx, request, *journalRecord)
	}
	if err := validateRuntimeSlotCleanupSession(*matched, request); err != nil {
		return protocol.NodeCleanupControlProof{}, err
	}
	if request.WriterRetireKind == protocol.WriterRetireKindPrelaunchAbort {
		return protocol.NodeCleanupControlProof{}, fmt.Errorf("prelaunch-aborted writer still has a local RootFS session: %w", errdefs.ErrFailedPrecondition)
	}
	if matched.CrashOperationID != "" && matched.CrashOperationID != request.WriterOperationID {
		return protocol.NodeCleanupControlProof{}, fmt.Errorf("RootFS crash fence belongs to another authority operation: %w", errdefs.ErrFailedPrecondition)
	}
	netnsPath := ""
	if matched.Consumer != nil {
		netnsPath, err = d.runtimeSlotNetworkPath(matched.Consumer, request.NetNSIdentity)
		if err != nil {
			return protocol.NodeCleanupControlProof{}, err
		}
	}
	observation, err := d.fenceHostRuntime(ctx, *matched)
	if err != nil {
		return protocol.NodeCleanupControlProof{}, err
	}
	rootFSProofDigest := ""
	switch request.WriterRetireKind {
	case protocol.WriterRetireKindPlannedPublish:
		if matched.Kind != rootfssession.RecoveryPlannedRetire || matched.RetireOperationID != request.WriterOperationID {
			return protocol.NodeCleanupControlProof{}, fmt.Errorf("planned RootFS session belongs to another retirement: %w", errdefs.ErrFailedPrecondition)
		}
		retired, retireErr := d.runtime.Retire(ctx, matched.Stage, request.WriterOperationID)
		if retireErr != nil {
			return protocol.NodeCleanupControlProof{}, retireErr
		}
		if len(retired.DetachProof) == 0 {
			remaining, listErr := d.runtime.RecoverySessions()
			if listErr != nil {
				return protocol.NodeCleanupControlProof{}, fmt.Errorf("verify forgotten planned RootFS session: %w", listErr)
			}
			stillPresent, matchErr := matchRuntimeSlotCleanupSession(remaining, request)
			if matchErr != nil {
				return protocol.NodeCleanupControlProof{}, matchErr
			}
			if stillPresent != nil {
				return protocol.NodeCleanupControlProof{}, fmt.Errorf("planned RootFS session remains after retirement: %w", errdefs.ErrFailedPrecondition)
			}
		} else {
			if err := validatePlannedCleanupResult(retired, matched.Stage, request); err != nil {
				return protocol.NodeCleanupControlProof{}, err
			}
			if len(retired.DetachProof) != sha256.Size || hex.EncodeToString(retired.DetachProof) != request.WriterAuthorityDigest {
				return protocol.NodeCleanupControlProof{}, fmt.Errorf("planned RootFS detach proof does not match regional authority: %w", errdefs.ErrFailedPrecondition)
			}
		}
		rootFSProofDigest = request.WriterAuthorityDigest
	case protocol.WriterRetireKindCrashAbandon, protocol.WriterRetireKindCanceled:
		if matched.CrashOperationID != "" && !matched.ExternalCrash {
			if request.WriterRetireKind != protocol.WriterRetireKindCrashAbandon {
				return protocol.NodeCleanupControlProof{}, fmt.Errorf("terminal RootFS crash proof cannot satisfy %s retirement: %w", request.WriterRetireKind, errdefs.ErrFailedPrecondition)
			}
			if reclaimErr := d.runtime.ReclaimVerifiedTerminal(ctx, matched.Stage); reclaimErr != nil {
				return protocol.NodeCleanupControlProof{}, fmt.Errorf("reclaim verified terminal RootFS session: %w", reclaimErr)
			}
			rootFSProofDigest = request.WriterAuthorityDigest
		} else {
			crashProof, fenceErr := d.runtime.FenceLocalRootFSWriter(ctx, matched.Stage, request.WriterOperationID, observation)
			if fenceErr != nil {
				return protocol.NodeCleanupControlProof{}, fenceErr
			}
			if proofErr := validateCrashCleanupProof(crashProof, matched.Stage, observation, request); proofErr != nil {
				return protocol.NodeCleanupControlProof{}, proofErr
			}
			crashDigest, digestErr := crashProof.Digest()
			if digestErr != nil {
				return protocol.NodeCleanupControlProof{}, fmt.Errorf("digest local RootFS crash proof: %w", digestErr)
			}
			rootFSProofDigest = hex.EncodeToString(crashDigest[:])
		}
	default:
		return protocol.NodeCleanupControlProof{}, fmt.Errorf("unsupported writer retirement kind %q: %w", request.WriterRetireKind, errdefs.ErrInvalidArgument)
	}
	if matched.Consumer == nil {
		if journalRecord == nil {
			return protocol.NodeCleanupControlProof{}, fmt.Errorf("unbound RootFS cleanup requires the runtime slot journal: %w", errdefs.ErrFailedPrecondition)
		}
		return d.cleanupJournaledRuntimeSlot(ctx, request, *journalRecord, rootFSProofDigest)
	}
	if err := d.cleanupRuntimeSlotNetwork(ctx, request, matched.Consumer, request.NetNSIdentity, netnsPath); err != nil {
		return protocol.NodeCleanupControlProof{}, err
	}
	if err := d.cleanupRuntimeResourceCgroup(ctx, request); err != nil {
		return protocol.NodeCleanupControlProof{}, err
	}
	proof := completedNodeCleanupProof(request, rootFSProofDigest)
	proof.ProofDigest, err = proof.Digest()
	if err != nil {
		return protocol.NodeCleanupControlProof{}, err
	}
	if err := proof.Validate(); err != nil {
		return protocol.NodeCleanupControlProof{}, fmt.Errorf("validate node cleanup proof: %w", err)
	}
	if journalRecord != nil {
		if err := d.journal.CompleteCleanup(request, proof); err != nil {
			return protocol.NodeCleanupControlProof{}, fmt.Errorf("persist runtime slot cleanup proof: %w", err)
		}
	}
	return proof, nil
}

// validatePlannedCleanupResult prevents an adapter from satisfying one
// regional retirement with the detach result of another local writer.
func validatePlannedCleanupResult(
	result rootfssession.RetireResult,
	stage rootfshandoff.StageRequest,
	request protocol.NodeCleanupControlRequest,
) error {
	if result.Parent != stage.Parent || result.RootFSID != stage.Identity.RootFSID ||
		result.WriterEpoch != stage.Identity.WriterEpoch || result.OperationID != request.WriterOperationID {
		return fmt.Errorf("planned RootFS result belongs to another writer operation: %w", errdefs.ErrFailedPrecondition)
	}
	return nil
}

// validateCrashCleanupProof binds every caller-relevant fact in the local
// crash proof to the durable session and exact regional cleanup request.
func validateCrashCleanupProof(
	proof rootfshandoff.CrashFenceProof,
	stage rootfshandoff.StageRequest,
	observation CrashTaskObservation,
	request protocol.NodeCleanupControlRequest,
) error {
	if err := proof.Validate(); err != nil {
		return fmt.Errorf("validate local RootFS crash proof: %w", err)
	}
	if stage.Generation == nil {
		return fmt.Errorf("local RootFS crash proof lacks its durable binding: %w", errdefs.ErrFailedPrecondition)
	}
	bindingDigest, err := stage.BindingDigest()
	if err != nil {
		return fmt.Errorf("digest local RootFS writer binding: %w", err)
	}
	if proof.OperationID != request.WriterOperationID ||
		proof.Parent != stage.Parent || proof.ClaimID != stage.Identity.ClaimID ||
		proof.WriterGrantID != request.WriterGrantID || proof.WriterEpoch != stage.Identity.WriterEpoch ||
		proof.BindingVersion != stage.BindingVersion || proof.BindingDigest != hex.EncodeToString(bindingDigest[:]) ||
		proof.RootFSID != stage.Identity.RootFSID || proof.InitialGeneration != stage.InitialGeneration ||
		proof.InitialBlockHead != stage.Generation.CurrentBlockHead ||
		proof.NodeUID != request.NodeUID || proof.BootID != request.NodeBootID ||
		proof.RuntimeGeneration != stage.Identity.RuntimeGeneration || proof.AllocationID != request.AllocationID ||
		proof.NetworkIncarnationID != stage.Identity.NetworkIncarnationID || proof.TaskName != protocol.NomadTaskName ||
		proof.SlotNonce != request.SlotID || proof.ActiveKey != observation.ActiveKey ||
		proof.ConsumerBound != (observation.ContainerID != "") || proof.ContainerID != observation.ContainerID ||
		proof.HostMountNamespaceID != observation.HostMountNamespaceID ||
		proof.ContainerAbsent != observation.ContainerAbsent || proof.TaskAbsent != observation.TaskAbsent ||
		proof.FrontendSnapshotAbsent != observation.FrontendSnapshotAbsent ||
		proof.StableMountAbsent != observation.StableMountAbsent {
		return fmt.Errorf("local RootFS crash proof belongs to another runtime slot: %w", errdefs.ErrFailedPrecondition)
	}
	if proof.ConsumerBound {
		if proof.ActiveKey != request.SlotID || proof.ContainerID != request.RunscContainerID {
			return fmt.Errorf("local RootFS crash proof belongs to another bound consumer: %w", errdefs.ErrFailedPrecondition)
		}
	} else if proof.ActiveKey != stage.Identity.ClaimID || proof.ContainerID != "" {
		return fmt.Errorf("local RootFS crash proof belongs to another unbound claim: %w", errdefs.ErrFailedPrecondition)
	}
	return nil
}

// matchRuntimeSlotCleanupSession rejects ambiguous ownership before any
// destructive host operation and returns the only exact writer session.
func matchRuntimeSlotCleanupSession(
	sessions []rootfssession.RecoverySession,
	request protocol.NodeCleanupControlRequest,
) (*rootfssession.RecoverySession, error) {
	var matched *rootfssession.RecoverySession
	for index := range sessions {
		session := sessions[index]
		stage := session.Stage
		consumer := session.Consumer
		sameIncarnation := stage.Identity.SlotNonce == request.SlotID ||
			stage.Identity.AllocationID == request.AllocationID ||
			consumer != nil && (consumer.ActiveKey == request.SlotID || consumer.ContainerID == request.RunscContainerID)
		if stage.Identity.WriterGrantID != request.WriterGrantID {
			if sameIncarnation {
				return nil, fmt.Errorf("runtime slot incarnation is owned by another RootFS writer: %w", errdefs.ErrFailedPrecondition)
			}
			continue
		}
		if matched != nil {
			return nil, fmt.Errorf("writer grant has multiple local sessions: %w", errdefs.ErrFailedPrecondition)
		}
		candidate := session
		matched = &candidate
	}
	return matched, nil
}

func (d *nodeRuntime) cleanupJournaledWriterRuntimeSlot(
	ctx context.Context,
	request protocol.NodeCleanupControlRequest,
	record runtimeSlotJournalRecord,
) (protocol.NodeCleanupControlProof, error) {
	rootFSProofDigest, err := journaledRootFSAbsenceDigest(request, record.Registration)
	if err != nil {
		return protocol.NodeCleanupControlProof{}, err
	}
	return d.cleanupJournaledRuntimeSlot(ctx, request, record, rootFSProofDigest)
}

// journaledRootFSAbsenceDigest binds a forgotten writer's absence to the
// immutable physical slot registration and terminal regional authority.
func journaledRootFSAbsenceDigest(
	request protocol.NodeCleanupControlRequest,
	registration RuntimeSlotRegistration,
) (string, error) {
	payload, err := json.Marshal(struct {
		Version               int    `json:"version"`
		OperationID           string `json:"operation_id"`
		WriterOperationID     string `json:"writer_operation_id"`
		WriterRetireKind      string `json:"writer_retire_kind"`
		WriterGrantID         string `json:"writer_grant_id"`
		WriterAuthorityDigest string `json:"writer_authority_digest"`
		SlotID                string `json:"slot_id"`
		AllocationID          string `json:"allocation_id"`
		NodeBootID            string `json:"node_boot_id"`
		RunscContainerID      string `json:"runsc_container_id"`
		StableMountID         string `json:"stable_mount_id"`
		MountNamespaceID      string `json:"mount_namespace_id"`
	}{
		Version: protocol.NodeCleanupProofVersion, OperationID: request.OperationID,
		WriterOperationID: request.WriterOperationID, WriterRetireKind: request.WriterRetireKind,
		WriterGrantID: request.WriterGrantID, WriterAuthorityDigest: request.WriterAuthorityDigest,
		SlotID: request.SlotID, AllocationID: request.AllocationID, NodeBootID: request.NodeBootID,
		RunscContainerID: request.RunscContainerID, StableMountID: registration.StableMountID,
		MountNamespaceID: registration.MountNamespaceID,
	})
	if err != nil {
		return "", fmt.Errorf("encode journaled RootFS absence proof: %w", err)
	}
	digest := sha256.Sum256(payload)
	return hex.EncodeToString(digest[:]), nil
}

func completedNodeCleanupProof(
	request protocol.NodeCleanupControlRequest,
	rootFSProofDigest string,
) protocol.NodeCleanupControlProof {
	return protocol.NodeCleanupControlProof{
		Version: protocol.NodeCleanupProofVersion, OperationID: request.OperationID,
		WriterOperationID: request.WriterOperationID, WriterRetireKind: request.WriterRetireKind,
		SlotID: request.SlotID, ClusterID: request.ClusterID, AllocationID: request.AllocationID,
		NodeID: request.NodeID, NodeUID: request.NodeUID, NodeBootID: request.NodeBootID,
		NetNSIdentity: request.NetNSIdentity, RunscContainerID: request.RunscContainerID,
		WriterGrantID: request.WriterGrantID, WriterAuthorityDigest: request.WriterAuthorityDigest,
		RootFSOperationID: request.WriterOperationID, RootFSProofDigest: rootFSProofDigest,
		Resources: request.Resources, ResourceLeaseID: request.Resources.LeaseID,
		ResourceLeaseDigest: request.ResourceLeaseDigest,
		RunscAbsent:         true, StableMountAbsent: true, RootFSWriterAbsent: true, NetworkPolicyAbsent: true,
		ResourceCgroupAbsent: !request.Resources.IsZero(),
	}
}

func (d *nodeRuntime) cleanupGrantlessRuntimeSlot(
	ctx context.Context,
	request protocol.NodeCleanupControlRequest,
	record runtimeSlotJournalRecord,
) (protocol.NodeCleanupControlProof, error) {
	return d.cleanupJournaledRuntimeSlot(ctx, request, record, "")
}

func (d *nodeRuntime) cleanupJournaledRuntimeSlot(
	ctx context.Context,
	request protocol.NodeCleanupControlRequest,
	record runtimeSlotJournalRecord,
	rootFSProofDigest string,
) (protocol.NodeCleanupControlProof, error) {
	registration := record.Registration
	stableMount, err := d.runtimeSlotStableMountPath(registration)
	if err != nil {
		return protocol.NodeCleanupControlProof{}, err
	}
	consumer := &rootfssession.ConsumerRegistration{
		ContainerID: registration.RunscContainerID, NetNSPath: registration.NetNSPath,
		NetNSIdentity: registration.NetNSIdentity, NetworkChain: registration.NetworkChain,
	}
	netnsPath, err := d.runtimeSlotNetworkPath(consumer, request.NetNSIdentity)
	if err != nil {
		return protocol.NodeCleanupControlProof{}, err
	}
	if err := d.fenceJournalRunsc(ctx, registration.RunscContainerID); err != nil {
		return protocol.NodeCleanupControlProof{}, err
	}
	if stableMount != "" {
		if err := d.mounter.Unmount(stableMount); err != nil {
			return protocol.NodeCleanupControlProof{}, err
		}
		attached, err := hostMountAttached(stableMount)
		if err != nil {
			return protocol.NodeCleanupControlProof{}, err
		}
		if attached {
			return protocol.NodeCleanupControlProof{}, fmt.Errorf("stable task root %s remains mounted: %w", stableMount, errdefs.ErrFailedPrecondition)
		}
	}
	if err := d.cleanupRuntimeSlotNetwork(ctx, request, consumer, request.NetNSIdentity, netnsPath); err != nil {
		return protocol.NodeCleanupControlProof{}, err
	}
	if err := d.cleanupRuntimeResourceCgroup(ctx, request); err != nil {
		return protocol.NodeCleanupControlProof{}, err
	}
	proof := completedNodeCleanupProof(request, rootFSProofDigest)
	proof.ProofDigest, err = proof.Digest()
	if err != nil {
		return protocol.NodeCleanupControlProof{}, err
	}
	if err := proof.Validate(); err != nil {
		return protocol.NodeCleanupControlProof{}, fmt.Errorf("validate journaled node cleanup proof: %w", err)
	}
	if err := d.journal.CompleteCleanup(request, proof); err != nil {
		return protocol.NodeCleanupControlProof{}, fmt.Errorf("persist journaled runtime slot cleanup proof: %w", err)
	}
	return proof, nil
}

func (d *nodeRuntime) cleanupRuntimeResourceCgroup(
	ctx context.Context,
	request protocol.NodeCleanupControlRequest,
) error {
	if request.Resources.IsZero() {
		return nil
	}
	if d.resourceCgroups == nil {
		return fmt.Errorf("runtime resource cgroup cleanup is unavailable: %w", errdefs.ErrUnavailable)
	}
	absent, err := d.resourceCgroups.RemoveAndConfirm(ctx, request.Resources)
	if err != nil {
		return fmt.Errorf("remove runtime resource cgroup: %w", err)
	}
	if !absent {
		return fmt.Errorf("runtime resource cgroup remains present: %w", errdefs.ErrFailedPrecondition)
	}
	return nil
}

func (d *nodeRuntime) runtimeSlotStableMountPath(
	registration RuntimeSlotRegistration,
) (string, error) {
	if _, err := os.Lstat(registration.StableMount); errors.Is(err, os.ErrNotExist) {
		return "", nil
	} else if err != nil {
		return "", fmt.Errorf("inspect runtime slot stable mount: %w", err)
	}
	resolved, err := validateExistingPath(registration.StableMount, d.config.RootFSConsumerMountRoot)
	if err != nil {
		return "", fmt.Errorf("validate runtime slot stable mount: %w: %w", err, errdefs.ErrFailedPrecondition)
	}
	if resolved != registration.StableMount {
		return "", fmt.Errorf("runtime slot stable mount is not a canonical resolved path: %w", errdefs.ErrFailedPrecondition)
	}
	attached, err := hostMountAttached(resolved)
	if err != nil {
		return "", err
	}
	if !attached {
		return "", nil
	}
	stableMountID, err := stableMountIdentity(resolved)
	if err != nil {
		return "", err
	}
	if stableMountID != registration.StableMountID {
		return "", fmt.Errorf("runtime slot stable mount incarnation changed: %w", errdefs.ErrFailedPrecondition)
	}
	return resolved, nil
}

func (d *nodeRuntime) fenceJournalRunsc(ctx context.Context, containerID string) error {
	_ = d.runner.Kill(ctx, containerID, "KILL")
	if err := d.runner.Delete(ctx, containerID, true); err != nil && !errdefs.IsNotFound(err) {
		return fmt.Errorf("delete journaled gVisor container: %w", err)
	}
	if _, err := d.runner.State(ctx, containerID); err == nil {
		return fmt.Errorf("gVisor container %s remains present: %w", containerID, errdefs.ErrFailedPrecondition)
	} else if !errdefs.IsNotFound(err) {
		return fmt.Errorf("attest journaled gVisor container absence: %w", err)
	}
	return nil
}

func validateRuntimeSlotCleanupSession(
	session rootfssession.RecoverySession,
	request protocol.NodeCleanupControlRequest,
) error {
	stage := session.Stage
	consumer := session.Consumer
	if session.Kind == rootfssession.RecoveryUnavailable ||
		stage.Identity.SlotNonce != request.SlotID || stage.Identity.AllocationID != request.AllocationID ||
		stage.Identity.NodeUID != request.NodeUID || stage.Identity.BootID != request.NodeBootID ||
		stage.Identity.WriterGrantID != request.WriterGrantID || stage.Identity.TaskName != protocol.NomadTaskName ||
		stage.ExpectedPolicyToken.NetNSIdentity != request.NetNSIdentity {
		return fmt.Errorf("RootFS session does not match the runtime slot incarnation: %w", errdefs.ErrFailedPrecondition)
	}
	if request.WriterRetireKind == protocol.WriterRetireKindCanceled {
		if consumer != nil || session.Kind != rootfssession.RecoveryCrashAbandon {
			return fmt.Errorf("canceled writer has a consumed RootFS session: %w", errdefs.ErrFailedPrecondition)
		}
		return nil
	}
	if consumer == nil {
		if request.WriterRetireKind == protocol.WriterRetireKindCrashAbandon &&
			session.Kind == rootfssession.RecoveryCrashAbandon {
			return nil
		}
		return fmt.Errorf("non-canceled writer lacks its RootFS consumer: %w", errdefs.ErrFailedPrecondition)
	}
	if consumer.ActiveKey != request.SlotID || consumer.ContainerID != request.RunscContainerID ||
		consumer.NetNSIdentity != request.NetNSIdentity || consumer.NetworkChain != networkChainName(consumer.ContainerID) ||
		consumer.NetNSPath == "" {
		return fmt.Errorf("RootFS session does not match the runtime slot incarnation: %w", errdefs.ErrFailedPrecondition)
	}
	return nil
}

func (d *nodeRuntime) cleanupRuntimeSlotNetwork(
	ctx context.Context,
	request protocol.NodeCleanupControlRequest,
	consumer *rootfssession.ConsumerRegistration,
	expectedIdentity string,
	validatedPath string,
) error {
	if d.runtimeSlotNetwork == nil {
		return fmt.Errorf("runtime slot network cleanup identity is unavailable: %w", errdefs.ErrFailedPrecondition)
	}
	if consumer == nil || consumer.NetNSPath == "" || consumer.NetNSIdentity != expectedIdentity {
		return fmt.Errorf("runtime slot network cleanup identity is unavailable: %w", errdefs.ErrFailedPrecondition)
	}
	currentPath, err := d.runtimeSlotNetworkPath(consumer, expectedIdentity)
	if err != nil {
		return err
	}
	if currentPath != validatedPath {
		return fmt.Errorf("runtime slot network namespace path changed: %w", errdefs.ErrFailedPrecondition)
	}
	if err := d.runtimeSlotNetwork.Cleanup(ctx, request); err != nil {
		return fmt.Errorf("remove ctld runtime slot network policy: %w", err)
	}
	return nil
}

func (d *nodeRuntime) runtimeSlotNetworkPath(
	consumer *rootfssession.ConsumerRegistration,
	expectedIdentity string,
) (string, error) {
	if consumer == nil || consumer.NetNSPath == "" || consumer.NetNSIdentity != expectedIdentity ||
		strings.TrimSpace(d.config.RootFSConsumerNetNSRoot) == "" {
		return "", fmt.Errorf("runtime slot network cleanup identity is unavailable: %w", errdefs.ErrFailedPrecondition)
	}
	if _, err := os.Lstat(consumer.NetNSPath); errors.Is(err, os.ErrNotExist) {
		return "", nil
	} else if err != nil {
		return "", fmt.Errorf("inspect runtime slot network namespace: %w", err)
	}
	resolvedPath, err := validateExistingPath(consumer.NetNSPath, d.config.RootFSConsumerNetNSRoot)
	if err != nil {
		return "", fmt.Errorf("validate runtime slot network namespace: %w: %w", err, errdefs.ErrFailedPrecondition)
	}
	if resolvedPath != consumer.NetNSPath {
		return "", fmt.Errorf("runtime slot network namespace is not a canonical resolved path: %w", errdefs.ErrFailedPrecondition)
	}
	identity, err := networkNamespaceIdentity(resolvedPath)
	if err != nil {
		return "", err
	}
	if identity != expectedIdentity {
		return "", fmt.Errorf("runtime slot network namespace incarnation changed: %w", errdefs.ErrFailedPrecondition)
	}
	return resolvedPath, nil
}

func (d *nodeRuntime) health(ctx context.Context) error {
	if _, err := d.runtime.RecoverySessions(); err != nil {
		return fmt.Errorf("read durable RootFS recovery journal: %w: %w", err, errdefs.ErrUnavailable)
	}
	if d.allocations != nil {
		if _, err := d.allocations.ActiveAllocations(ctx); err != nil {
			return fmt.Errorf("read Nomad allocation authority: %w: %w", err, errdefs.ErrUnavailable)
		}
	}
	if d.journal == nil {
		return fmt.Errorf("runtime slot journal is unavailable: %w", errdefs.ErrUnavailable)
	}
	if err := d.journal.Ping(); err != nil {
		return err
	}
	if d.runtimeSlotNetwork != nil {
		if err := d.runtimeSlotNetwork.Ping(ctx); err != nil {
			return fmt.Errorf("check ctld runtime slot network control: %w", err)
		}
	}
	return nil
}

func (d *nodeRuntime) writerLeaseLost(stage rootfshandoff.StageRequest, cause error) {
	d.logger.Error("RootFS writer authority lease lost", "parent", stage.Parent, "error", cause)
	select {
	case d.trigger <- stage.Parent:
	default:
		d.logger.Error("RootFS terminal trigger queue is full", "parent", stage.Parent)
	}
}

func (d *nodeRuntime) reconcileLoop(ctx context.Context) {
	ticker := time.NewTicker(rootFSSessionReconcileInterval)
	defer ticker.Stop()
	var pressureSignal <-chan struct{}
	if runtime, ok := d.runtime.(dirtyTailPressureRuntime); ok {
		pressureSignal = runtime.DirtyTailPressureSignal()
	}
	d.scanDirtyTailPressures(ctx)
	d.scan(ctx, "")
	for {
		select {
		case <-ctx.Done():
			return
		case <-pressureSignal:
			d.scanDirtyTailPressures(ctx)
		case parent := <-d.trigger:
			d.scan(ctx, parent)
		case <-ticker.C:
			d.scanDirtyTailPressures(ctx)
			d.scan(ctx, "")
		}
	}
}

func (d *nodeRuntime) scanDirtyTailPressures(ctx context.Context) {
	runtime, ok := d.runtime.(dirtyTailPressureRuntime)
	if !ok {
		return
	}
	pressures, err := runtime.DirtyTailPressureSessions()
	if err != nil {
		d.logger.Error("list blocked RootFS dirty-tail writers", "error", err)
		return
	}
	for _, pressure := range pressures {
		parent := pressure.Stage.Parent
		inflightKey := "pressure:" + parent
		pressureCtx, cancel := context.WithTimeout(ctx, rootFSSessionReconcileTimeout)
		if !d.beginReconciliation(inflightKey, cancel) {
			cancel()
			continue
		}
		d.mu.Lock()
		d.wg.Add(1)
		d.mu.Unlock()
		go func(pressure rootfssession.DirtyTailPressureSession, inflightKey string) {
			defer cancel()
			_, planErr := runtime.PlanDirtyTailPressure(pressureCtx, pressure)
			d.endReconciliation(inflightKey)
			d.wg.Done()
			if planErr != nil {
				if !errors.Is(planErr, context.Canceled) {
					d.logger.Error("plan pressured RootFS writer retirement", "parent", pressure.Stage.Parent, "error", planErr)
				}
				return
			}
			select {
			case d.trigger <- pressure.Stage.Parent:
			default:
				d.logger.Error("RootFS pressure retirement trigger queue is full", "parent", pressure.Stage.Parent)
			}
		}(pressure, inflightKey)
	}
}

func (d *nodeRuntime) scan(ctx context.Context, onlyParent string) {
	now := time.Now()
	if d.journal != nil && (d.lastJournalPrune.IsZero() || now.Sub(d.lastJournalPrune) >= runtimeSlotJournalPruneInterval) {
		if _, err := d.journal.Prune(now); err != nil {
			d.logger.Error("prune runtime slot cleanup proofs", "error", err)
		} else {
			d.lastJournalPrune = now
		}
	}
	sessions, err := d.runtime.RecoverySessions()
	if err != nil {
		d.logger.Error("list durable RootFS recovery sessions", "error", err)
		return
	}
	if len(sessions) == 0 {
		return
	}
	now = time.Now()
	var activeAllocations map[string]bool
	if d.allocations != nil {
		activeAllocations, err = d.allocations.ActiveAllocations(ctx)
		if err != nil {
			d.logger.Error("list active Nomad allocations for RootFS reconciliation", "error", err)
			activeAllocations = nil
		}
	}
	for _, session := range sessions {
		if onlyParent != "" && session.Stage.Parent != onlyParent {
			continue
		}
		if session.Kind == rootfssession.RecoveryUnavailable {
			d.logger.Error("legacy RootFS session lacks an independent recovery binding", "state", session.State)
			continue
		}
		if session.PressureOperationID != "" {
			// A durable local pressure marker is not retirement authority. Its
			// exact regional request must be recovered before any crash path is
			// allowed to discard the dirty branch.
			continue
		}
		if d.reconciliationInFlight("pressure:" + session.Stage.Parent) {
			continue
		}
		allocationPurged := activeAllocations != nil && !activeAllocations[session.Stage.Identity.AllocationID] &&
			now.Sub(session.CreatedAt) >= rootFSSessionReconcileInterval
		if !rootFSSessionNeedsReconciliation(session, now, onlyParent != "" || allocationPurged) {
			continue
		}
		inflightKey := session.Stage.Identity.SlotNonce
		if inflightKey == "" {
			inflightKey = session.Stage.Parent
		}
		if d.runtimeSlotCleanupPending(inflightKey) {
			continue
		}
		reconcileCtx, cancel := context.WithTimeout(ctx, rootFSSessionReconcileTimeout)
		if !d.beginReconciliation(inflightKey, cancel) {
			cancel()
			continue
		}
		d.mu.Lock()
		d.wg.Add(1)
		d.mu.Unlock()
		go func(session rootfssession.RecoverySession, inflightKey string) {
			defer cancel()
			defer func() {
				d.endReconciliation(inflightKey)
				d.wg.Done()
			}()
			if err := d.reconcile(reconcileCtx, session); err != nil && !errors.Is(err, context.Canceled) {
				d.logger.Error("reconcile orphan RootFS writer", "parent", session.Stage.Parent, "error", err)
			}
		}(session, inflightKey)
	}
}

func (d *nodeRuntime) runtimeSlotCleanupPending(slotID string) bool {
	if d.journal == nil {
		return false
	}
	record, err := d.journal.Get(slotID)
	if err != nil {
		if !errdefs.IsNotFound(err) {
			d.logger.Error("inspect pending runtime slot cleanup", "slot_id", slotID, "error", err)
			return true
		}
		return false
	}
	return record.Cleanup != nil && record.Proof == nil
}

func (d *nodeRuntime) beginReconciliation(key string, cancel context.CancelFunc) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.inflight == nil {
		d.inflight = make(map[string]*reconciliationState)
	}
	if d.inflight[key] != nil || d.preempting[key] {
		return false
	}
	d.inflight[key] = &reconciliationState{cancel: cancel, done: make(chan struct{})}
	return true
}

// beginExternalReconciliation gives a durable regional cleanup priority over
// a speculative node-local recovery. Canceling the local attempt prevents an
// old planned marker from occupying the slot for the full recovery timeout
// after the regional authority has selected crash abandonment.
func (d *nodeRuntime) beginExternalReconciliation(ctx context.Context, key string) error {
	for {
		d.mu.Lock()
		if d.inflight == nil {
			d.inflight = make(map[string]*reconciliationState)
		}
		current := d.inflight[key]
		if current == nil {
			d.inflight[key] = &reconciliationState{done: make(chan struct{})}
			delete(d.preempting, key)
			d.mu.Unlock()
			return nil
		}
		if current.cancel == nil {
			d.mu.Unlock()
			return errdefs.ErrUnavailable
		}
		if d.preempting == nil {
			d.preempting = make(map[string]bool)
		}
		d.preempting[key] = true
		cancel := current.cancel
		done := current.done
		d.mu.Unlock()

		cancel()
		select {
		case <-ctx.Done():
			return errors.Join(ctx.Err(), errdefs.ErrUnavailable)
		case <-done:
		}
	}
}

func (d *nodeRuntime) endReconciliation(key string) {
	d.mu.Lock()
	current := d.inflight[key]
	delete(d.inflight, key)
	if current != nil {
		close(current.done)
	}
	d.mu.Unlock()
}

func (d *nodeRuntime) reconciliationInFlight(key string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.inflight[key] != nil
}

type httpNomadAllocationSource struct {
	allocationURL string
	visibilityURL string
	namespace     string
	jobID         string
	tokenFile     string
	http          *http.Client
}

func newNomadAllocationSource(config NomadAllocationConfig) (nomadAllocationSource, error) {
	address := strings.TrimSpace(config.Address)
	nodeID := strings.TrimSpace(config.NodeID)
	namespace := strings.TrimSpace(config.Namespace)
	jobID := strings.TrimSpace(config.JobID)
	if address == "" && nodeID == "" && namespace == "" && jobID == "" {
		return nil, nil
	}
	if address == "" || nodeID == "" || namespace == "" || jobID == "" {
		return nil, fmt.Errorf("nomad address, node ID, namespace, and job ID must be configured together")
	}
	parsed, err := url.Parse(address)
	if err != nil || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" ||
		(parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
		return nil, fmt.Errorf("nomad address must be an HTTP(S) origin")
	}
	basePath := strings.TrimSuffix(parsed.Path, "/")
	parsed.Path = basePath + "/v1/node/" + url.PathEscape(nodeID) + "/allocations"
	allocationURL := parsed.String()
	parsed.Path = basePath + "/v1/job/" + url.PathEscape(jobID)
	query := parsed.Query()
	query.Set("namespace", namespace)
	parsed.RawQuery = query.Encode()
	visibilityURL := parsed.String()
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	if parsed.Scheme == "https" {
		tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12}
		if strings.TrimSpace(config.CAFile) != "" {
			payload, err := os.ReadFile(strings.TrimSpace(config.CAFile))
			if err != nil {
				return nil, fmt.Errorf("read Nomad CA: %w", err)
			}
			roots := x509.NewCertPool()
			if !roots.AppendCertsFromPEM(payload) {
				return nil, fmt.Errorf("nomad CA contains no certificates")
			}
			tlsConfig.RootCAs = roots
		}
		if strings.TrimSpace(config.CertFile) != "" || strings.TrimSpace(config.KeyFile) != "" {
			if strings.TrimSpace(config.CertFile) == "" || strings.TrimSpace(config.KeyFile) == "" {
				return nil, fmt.Errorf("nomad client certificate and key must be configured together")
			}
			certificate, err := tls.LoadX509KeyPair(config.CertFile, config.KeyFile)
			if err != nil {
				return nil, fmt.Errorf("load Nomad client identity: %w", err)
			}
			tlsConfig.Certificates = []tls.Certificate{certificate}
		}
		transport.TLSClientConfig = tlsConfig
	}
	return &httpNomadAllocationSource{
		allocationURL: allocationURL, visibilityURL: visibilityURL,
		namespace: namespace, jobID: jobID, tokenFile: strings.TrimSpace(config.TokenFile),
		http: &http.Client{Timeout: 2 * time.Second, Transport: transport},
	}, nil
}

func (s *httpNomadAllocationSource) ActiveAllocations(ctx context.Context) (map[string]bool, error) {
	token := ""
	if s.tokenFile != "" {
		payload, err := os.ReadFile(s.tokenFile)
		if err != nil {
			return nil, fmt.Errorf("read Nomad token: %w", err)
		}
		token = strings.TrimSpace(string(payload))
	}
	// Nomad intentionally returns an ACL-filtered 200 response from the node
	// allocation list. Prove read-job access to the exact workload namespace
	// before an empty list is allowed to establish physical absence.
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, s.visibilityURL, nil)
	if err != nil {
		return nil, err
	}
	if token != "" {
		request.Header.Set("X-Nomad-Token", token)
	}
	response, err := s.http.Do(request)
	if err != nil {
		return nil, fmt.Errorf("verify Nomad allocation catalog visibility: %w", err)
	}
	if response.StatusCode/100 != 2 {
		payload, _ := io.ReadAll(io.LimitReader(response.Body, 4<<10))
		response.Body.Close()
		return nil, fmt.Errorf("verify Nomad allocation catalog visibility: HTTP %d: %s", response.StatusCode, strings.TrimSpace(string(payload)))
	}
	var job struct {
		ID        string `json:"ID"`
		Namespace string `json:"Namespace"`
	}
	if err := json.NewDecoder(io.LimitReader(response.Body, nomadAllocationResponseMaxBytes)).Decode(&job); err != nil {
		response.Body.Close()
		return nil, fmt.Errorf("decode Nomad allocation catalog visibility anchor: %w", err)
	}
	response.Body.Close()
	if job.ID != s.jobID || job.Namespace != s.namespace {
		return nil, fmt.Errorf("Nomad allocation catalog visibility anchor does not match configured job")
	}

	request, err = http.NewRequestWithContext(ctx, http.MethodGet, s.allocationURL, nil)
	if err != nil {
		return nil, err
	}
	if token != "" {
		request.Header.Set("X-Nomad-Token", token)
	}
	response, err = s.http.Do(request)
	if err != nil {
		return nil, fmt.Errorf("list Nomad node allocations: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode/100 != 2 {
		payload, _ := io.ReadAll(io.LimitReader(response.Body, 4<<10))
		return nil, fmt.Errorf("list Nomad node allocations: HTTP %d: %s", response.StatusCode, strings.TrimSpace(string(payload)))
	}
	var records []struct {
		ID string `json:"ID"`
	}
	decoder := json.NewDecoder(io.LimitReader(response.Body, nomadAllocationResponseMaxBytes))
	if err := decoder.Decode(&records); err != nil {
		return nil, fmt.Errorf("decode Nomad node allocations: %w", err)
	}
	active := make(map[string]bool, len(records))
	for _, record := range records {
		if id := strings.TrimSpace(record.ID); id != "" {
			// Presence, not scheduler status, is the purge fence. A normal stop
			// must retain its allocation long enough for planned publication;
			// treating DesiredStatus=stop as an orphan would discard writes.
			active[id] = true
		}
	}
	return active, nil
}

func rootFSSessionNeedsReconciliation(session rootfssession.RecoverySession, now time.Time, forced bool) bool {
	if session.ExternalCrash && session.BranchRemoved {
		// The regional authority and physical cleanup were already verified.
		// Keep the compact local proof for the bounded response-replay window,
		// but do not poll the authority once per second for every retained
		// terminal session. Reconcile once more at expiry to verify and forget
		// the proof.
		return !now.Before(session.CrashRequestedAt.Add(rootfssession.ExternalTerminalProofRetention))
	}
	if forced || session.Kind == rootfssession.RecoveryPlannedRetire {
		return true
	}
	if session.Consumer != nil {
		deadline, err := session.Consumer.Validate()
		// Live is process-local and cannot establish orphanhood when the A/B
		// ctld pair shares one durable session journal. The standby observes the
		// active owner's session as not live even while the task driver keeps its
		// durable consumer lease renewed. Reconcile early only through a forced
		// lease-loss/allocation-absence path; periodic scans wait for the durable
		// consumer lease to expire.
		return err != nil || !now.Before(deadline)
	}
	return now.Sub(session.CreatedAt) >= rootFSSessionAttachGrace
}

func crashOperationID(stage rootfshandoff.StageRequest) string {
	payload := fmt.Sprintf("%s\x00%s\x00%d", stage.Parent, stage.Identity.WriterGrantID, stage.Identity.WriterEpoch)
	sum := sha256.Sum256([]byte(payload))
	return "nomad-crash-" + hex.EncodeToString(sum[:16])
}

func (d *nodeRuntime) reconcile(ctx context.Context, session rootfssession.RecoverySession) error {
	if session.ExternalCrash {
		_, err := d.runtime.ReclaimExternallyRetired(ctx, session.Stage)
		return err
	}
	observation, err := d.fenceHostRuntime(ctx, session)
	if err != nil {
		return err
	}
	if session.Kind == rootfssession.RecoveryPlannedRetire {
		operationID := session.RetireOperationID
		if operationID == "" {
			return fmt.Errorf("planned RootFS recovery lacks its operation ID")
		}
		_, err := d.runtime.Retire(ctx, session.Stage, operationID)
		return err
	}
	_, err = d.runtime.CrashFence(ctx, session.Stage, crashOperationID(session.Stage), observation)
	return err
}

func (d *nodeRuntime) fenceHostRuntime(
	ctx context.Context,
	session rootfssession.RecoverySession,
) (CrashTaskObservation, error) {
	consumer := session.Consumer
	if consumer == nil {
		namespace, err := os.Readlink("/proc/self/ns/mnt")
		if err != nil {
			return CrashTaskObservation{}, fmt.Errorf("read ctld Nomad runtime mount namespace: %w", err)
		}
		return CrashTaskObservation{
			ActiveKey: session.Stage.Identity.ClaimID, HostMountNamespaceID: namespace,
			ContainerAbsent: true, TaskAbsent: true, FrontendSnapshotAbsent: true, StableMountAbsent: true,
		}, nil
	}
	namespace, err := os.Readlink("/proc/self/ns/mnt")
	if err != nil {
		return CrashTaskObservation{}, fmt.Errorf("read ctld Nomad runtime mount namespace: %w", err)
	}
	if namespace != consumer.HostMountNamespace {
		return CrashTaskObservation{}, fmt.Errorf(
			"consumer mount namespace %s differs from ctld Nomad runtime %s: %w",
			consumer.HostMountNamespace, namespace, errdefs.ErrFailedPrecondition,
		)
	}
	if err := validateConsumerMountPath(consumer.StableMount, d.config.RootFSConsumerMountRoot); err != nil {
		return CrashTaskObservation{}, err
	}
	// A naturally stopped container may reject kill. Forced delete plus the
	// subsequent state lookup, not kill's exit status, is the absence proof.
	_ = d.runner.Kill(ctx, consumer.ContainerID, "KILL")
	if err := d.runner.Delete(ctx, consumer.ContainerID, true); err != nil && !errdefs.IsNotFound(err) {
		return CrashTaskObservation{}, fmt.Errorf("delete orphan gVisor container: %w", err)
	}
	if _, err := d.runner.State(ctx, consumer.ContainerID); err == nil {
		return CrashTaskObservation{}, fmt.Errorf("gVisor container %s remains present: %w", consumer.ContainerID, errdefs.ErrFailedPrecondition)
	} else if !errdefs.IsNotFound(err) {
		return CrashTaskObservation{}, fmt.Errorf("attest orphan gVisor container absence: %w", err)
	}
	if err := d.mounter.Unmount(consumer.StableMount); err != nil {
		return CrashTaskObservation{}, err
	}
	attached, err := hostMountAttached(consumer.StableMount)
	if err != nil {
		return CrashTaskObservation{}, err
	}
	if attached {
		return CrashTaskObservation{}, fmt.Errorf("stable task root %s remains mounted: %w", consumer.StableMount, errdefs.ErrFailedPrecondition)
	}
	return CrashTaskObservation{
		ActiveKey: consumer.ActiveKey, ContainerID: consumer.ContainerID,
		HostMountNamespaceID: consumer.HostMountNamespace, ContainerAbsent: true, TaskAbsent: true,
		FrontendSnapshotAbsent: true, StableMountAbsent: true,
	}, nil
}

func validateConsumerMountPath(path, root string) error {
	path = filepath.Clean(strings.TrimSpace(path))
	root = filepath.Clean(strings.TrimSpace(root))
	if !filepath.IsAbs(path) || !filepath.IsAbs(root) || path == root || root == "/" {
		return fmt.Errorf("consumer mount and root are invalid")
	}
	relative, err := filepath.Rel(root, path)
	if err != nil || relative == ".." || filepath.IsAbs(relative) || startsWithDotDot(relative) {
		return fmt.Errorf("consumer mount %s is outside %s", path, root)
	}
	return nil
}

func hostMountAttached(path string) (bool, error) {
	file, err := os.Open("/proc/self/mountinfo")
	if err != nil {
		return false, fmt.Errorf("open host mountinfo: %w", err)
	}
	defer file.Close()
	wanted := filepath.Clean(path)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) < 5 {
			continue
		}
		mountpoint := strings.NewReplacer(
			`\040`, " ", `\011`, "\t", `\012`, "\n", `\134`, `\`,
		).Replace(fields[4])
		if filepath.Clean(mountpoint) == wanted {
			return true, nil
		}
	}
	if err := scanner.Err(); err != nil {
		return false, fmt.Errorf("scan host mountinfo: %w", err)
	}
	return false, nil
}
