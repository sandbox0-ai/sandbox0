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
	"bufio"
	"context"
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
	"time"

	"github.com/containerd/errdefs"
	"github.com/hashicorp/go-hclog"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	rootfssession "github.com/sandbox0-ai/sandbox0/pkg/rootfssession"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
)

const (
	rootFSSessionReconcileInterval  = time.Second
	rootFSSessionAttachGrace        = 2 * time.Minute
	rootFSSessionReconcileTimeout   = 3 * time.Minute
	nomadAllocationResponseMaxBytes = 64 << 20
)

type rootFSSessionDaemon struct {
	runtime rootFSSessionDaemonRuntime
	runner  Runsc
	mounter Mounter
	config  PluginConfig
	logger  hclog.Logger

	mu          sync.Mutex
	wg          sync.WaitGroup
	inflight    map[string]bool
	trigger     chan string
	allocations nomadAllocationSource
	network     NetworkRuntime
	clusterID   string
	nodeID      string
}

type rootFSSessionDaemonRuntime interface {
	RootFSRuntime
	RecoverySessions() ([]rootfssession.RecoverySession, error)
	FenceLocalRootFSWriter(context.Context, rootfshandoff.StageRequest, string, crashTaskObservation) (rootfshandoff.CrashFenceProof, error)
	ReclaimExternallyRetired(context.Context, rootfshandoff.StageRequest) (bool, error)
}

type nomadAllocationSource interface {
	ActiveAllocations(context.Context) (map[string]bool, error)
}

// NomadAllocationConfig identifies the server-side allocation catalog used as
// a reconciliation trigger when the local task-driver process misses Destroy.
type NomadAllocationConfig struct {
	ClusterID string
	Address   string
	NodeID    string
	TokenFile string
	CAFile    string
	CertFile  string
	KeyFile   string
}

// RunRootFSSessionDaemon runs the node-scoped owner for writer leases,
// NBD/XFS/Overlay sessions, and terminal reconciliation. The Nomad task-driver
// process talks to it only over the root-owned Unix socket.
func RunRootFSSessionDaemon(
	ctx context.Context,
	config PluginConfig,
	socketPath string,
	nomadConfig NomadAllocationConfig,
	logger hclog.Logger,
) error {
	config.RootFSSessiondSocket = ""
	if strings.TrimSpace(config.RootFSConsumerMountRoot) == "" {
		return fmt.Errorf("rootfs_consumer_mount_root is required for the session daemon")
	}
	if strings.TrimSpace(config.RootFSConsumerNetNSRoot) == "" {
		return fmt.Errorf("rootfs_consumer_netns_root is required for the session daemon")
	}
	if strings.TrimSpace(config.RootFSAuthorityURL) == "" {
		return fmt.Errorf("rootfs_authority_url is required for the session daemon")
	}
	if strings.TrimSpace(nomadConfig.ClusterID) == "" {
		return fmt.Errorf("cluster_id is required for the session daemon")
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
		return fmt.Errorf("Nomad allocation authority is required for the session daemon")
	}
	runtime, err := newEmbeddedRootFSRuntime(&config, logger.Named("runtime"))
	if err != nil {
		return err
	}
	defer runtime.Close()
	daemon := &rootFSSessionDaemon{
		runtime: runtime, runner: NewCommandRunsc(config), mounter: systemMounter{},
		config: config, logger: logger, inflight: make(map[string]bool), trigger: make(chan string, 128),
		allocations: allocations, network: commandNetworkRuntime{},
		clusterID: strings.TrimSpace(nomadConfig.ClusterID), nodeID: strings.TrimSpace(nomadConfig.NodeID),
	}
	daemonCtx, cancelDaemon := context.WithCancel(ctx)
	defer cancelDaemon()
	daemon.wg.Add(1)
	go func() {
		defer daemon.wg.Done()
		daemon.reconcileLoop(daemonCtx)
	}()
	err = serveRootFSSessionRuntime(daemonCtx, socketPath, runtime, daemon.writerLeaseLost, daemon.health, daemon)
	cancelDaemon()
	daemon.wg.Wait()
	return err
}

// CleanupRuntimeSlot removes one exact claimed runtime without calling the
// Nomad task driver or completing regional writer authority. The local RootFS
// crash proof remains durable, making a lost response byte-stable on retry.
func (d *rootFSSessionDaemon) CleanupRuntimeSlot(
	ctx context.Context,
	request protocol.NodeCleanupControlRequest,
) (protocol.NodeCleanupControlProof, error) {
	if err := request.Validate(); err != nil {
		return protocol.NodeCleanupControlProof{}, fmt.Errorf("validate node cleanup request: %w: %w", err, errdefs.ErrInvalidArgument)
	}
	if d.clusterID == "" || d.nodeID == "" || request.ClusterID != d.clusterID || request.NodeID != d.nodeID {
		return protocol.NodeCleanupControlProof{}, fmt.Errorf("node cleanup target does not match this daemon: %w", errdefs.ErrPermissionDenied)
	}
	if request.WriterGrantID == "" {
		return protocol.NodeCleanupControlProof{}, fmt.Errorf("grantless runtime cleanup requires the node slot journal: %w", errdefs.ErrFailedPrecondition)
	}
	sessions, err := d.runtime.RecoverySessions()
	if err != nil {
		return protocol.NodeCleanupControlProof{}, fmt.Errorf("list durable RootFS sessions: %w", err)
	}
	var matched *rootfssession.RecoverySession
	for index := range sessions {
		if sessions[index].Stage.Identity.WriterGrantID != request.WriterGrantID {
			continue
		}
		if matched != nil {
			return protocol.NodeCleanupControlProof{}, fmt.Errorf("writer grant has multiple local sessions: %w", errdefs.ErrFailedPrecondition)
		}
		candidate := sessions[index]
		matched = &candidate
	}
	if matched == nil {
		return protocol.NodeCleanupControlProof{}, fmt.Errorf("RootFS writer session is absent: %w", errdefs.ErrNotFound)
	}
	if !d.beginReconciliation(matched.Stage.Parent) {
		return protocol.NodeCleanupControlProof{}, fmt.Errorf("RootFS session cleanup is already in progress: %w", errdefs.ErrUnavailable)
	}
	defer d.endReconciliation(matched.Stage.Parent)
	if err := validateRuntimeSlotCleanupSession(*matched, request); err != nil {
		return protocol.NodeCleanupControlProof{}, err
	}
	if matched.CrashOperationID != "" &&
		(!matched.ExternalCrash || matched.CrashOperationID != request.WriterOperationID) {
		return protocol.NodeCleanupControlProof{}, fmt.Errorf("RootFS crash fence belongs to another authority operation: %w", errdefs.ErrFailedPrecondition)
	}
	netnsPath, err := d.runtimeSlotNetworkPath(matched.Consumer, request.NetNSIdentity)
	if err != nil {
		return protocol.NodeCleanupControlProof{}, err
	}
	observation, err := d.fenceHostRuntime(ctx, *matched)
	if err != nil {
		return protocol.NodeCleanupControlProof{}, err
	}
	crashProof, err := d.runtime.FenceLocalRootFSWriter(ctx, matched.Stage, request.WriterOperationID, observation)
	if err != nil {
		return protocol.NodeCleanupControlProof{}, err
	}
	crashDigest, err := crashProof.Digest()
	if err != nil {
		return protocol.NodeCleanupControlProof{}, fmt.Errorf("digest local RootFS crash proof: %w", err)
	}
	if err := d.cleanupRuntimeSlotNetwork(ctx, matched.Consumer, request.NetNSIdentity, netnsPath); err != nil {
		return protocol.NodeCleanupControlProof{}, err
	}
	proof := protocol.NodeCleanupControlProof{
		Version: protocol.NodeCleanupProofVersion, OperationID: request.OperationID,
		WriterOperationID: request.WriterOperationID, SlotID: request.SlotID,
		ClusterID: request.ClusterID, AllocationID: request.AllocationID,
		NodeID: request.NodeID, NodeUID: request.NodeUID, NodeBootID: request.NodeBootID,
		NetNSIdentity: request.NetNSIdentity, RunscContainerID: request.RunscContainerID,
		WriterGrantID: request.WriterGrantID, WriterFenceDigest: request.WriterFenceDigest,
		RootFSCrashOperationID: crashProof.OperationID,
		RootFSCrashProofDigest: hex.EncodeToString(crashDigest[:]),
		RunscAbsent:            true, StableMountAbsent: true, RootFSWriterAbsent: true, NetworkPolicyAbsent: true,
	}
	proof.ProofDigest, err = proof.Digest()
	if err != nil {
		return protocol.NodeCleanupControlProof{}, err
	}
	if err := proof.Validate(); err != nil {
		return protocol.NodeCleanupControlProof{}, fmt.Errorf("validate node cleanup proof: %w", err)
	}
	return proof, nil
}

func validateRuntimeSlotCleanupSession(
	session rootfssession.RecoverySession,
	request protocol.NodeCleanupControlRequest,
) error {
	stage := session.Stage
	consumer := session.Consumer
	if session.Kind == rootfssession.RecoveryUnavailable || consumer == nil ||
		stage.Identity.SlotNonce != request.SlotID || stage.Identity.PodUID != request.AllocationID ||
		stage.Identity.NodeUID != request.NodeUID || stage.Identity.BootID != request.NodeBootID ||
		stage.Identity.WriterGrantID != request.WriterGrantID || stage.Identity.ContainerName != protocol.NomadTaskName ||
		stage.ExpectedPolicyToken.NetNSIdentity != request.NetNSIdentity ||
		consumer.ActiveKey != request.SlotID || consumer.ContainerID != request.RunscContainerID ||
		consumer.NetNSIdentity != request.NetNSIdentity || consumer.NetworkChain != networkChainName(consumer.ContainerID) ||
		consumer.NetNSPath == "" {
		return fmt.Errorf("RootFS session does not match the runtime slot incarnation: %w", errdefs.ErrFailedPrecondition)
	}
	return nil
}

func (d *rootFSSessionDaemon) cleanupRuntimeSlotNetwork(
	ctx context.Context,
	consumer *rootfssession.ConsumerRegistration,
	expectedIdentity string,
	validatedPath string,
) error {
	if consumer == nil || consumer.NetNSPath == "" || consumer.NetNSIdentity != expectedIdentity || d.network == nil {
		return fmt.Errorf("runtime slot network cleanup identity is unavailable: %w", errdefs.ErrFailedPrecondition)
	}
	currentPath, err := d.runtimeSlotNetworkPath(consumer, expectedIdentity)
	if err != nil {
		return err
	}
	if currentPath == "" {
		return nil
	}
	if currentPath != validatedPath {
		return fmt.Errorf("runtime slot network namespace path changed: %w", errdefs.ErrFailedPrecondition)
	}
	if err := d.network.Cleanup(ctx, currentPath, consumer.NetworkChain); err != nil {
		return fmt.Errorf("cleanup runtime slot network policy: %w", err)
	}
	return nil
}

func (d *rootFSSessionDaemon) runtimeSlotNetworkPath(
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

func (d *rootFSSessionDaemon) health(ctx context.Context) error {
	if _, err := d.runtime.RecoverySessions(); err != nil {
		return fmt.Errorf("read durable RootFS recovery journal: %w: %w", err, errdefs.ErrUnavailable)
	}
	if d.allocations != nil {
		if _, err := d.allocations.ActiveAllocations(ctx); err != nil {
			return fmt.Errorf("read Nomad allocation authority: %w: %w", err, errdefs.ErrUnavailable)
		}
	}
	return nil
}

func (d *rootFSSessionDaemon) writerLeaseLost(stage rootfshandoff.StageRequest, cause error) {
	d.logger.Error("RootFS writer authority lease lost", "parent", stage.Parent, "error", cause)
	select {
	case d.trigger <- stage.Parent:
	default:
		d.logger.Error("RootFS terminal trigger queue is full", "parent", stage.Parent)
	}
}

func (d *rootFSSessionDaemon) reconcileLoop(ctx context.Context) {
	ticker := time.NewTicker(rootFSSessionReconcileInterval)
	defer ticker.Stop()
	d.scan(ctx, "")
	for {
		select {
		case <-ctx.Done():
			return
		case parent := <-d.trigger:
			d.scan(ctx, parent)
		case <-ticker.C:
			d.scan(ctx, "")
		}
	}
}

func (d *rootFSSessionDaemon) scan(ctx context.Context, onlyParent string) {
	sessions, err := d.runtime.RecoverySessions()
	if err != nil {
		d.logger.Error("list durable RootFS recovery sessions", "error", err)
		return
	}
	now := time.Now()
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
		allocationPurged := activeAllocations != nil && !activeAllocations[session.Stage.Identity.PodUID] &&
			now.Sub(session.CreatedAt) >= rootFSSessionReconcileInterval
		if !rootFSSessionNeedsReconciliation(session, now, onlyParent != "" || allocationPurged) {
			continue
		}
		parent := session.Stage.Parent
		if !d.beginReconciliation(parent) {
			continue
		}
		d.mu.Lock()
		d.wg.Add(1)
		d.mu.Unlock()
		go func(session rootfssession.RecoverySession) {
			defer func() {
				d.endReconciliation(session.Stage.Parent)
				d.wg.Done()
			}()
			reconcileCtx, cancel := context.WithTimeout(ctx, rootFSSessionReconcileTimeout)
			defer cancel()
			if err := d.reconcile(reconcileCtx, session); err != nil && !errors.Is(err, context.Canceled) {
				d.logger.Error("reconcile orphan RootFS writer", "parent", session.Stage.Parent, "error", err)
			}
		}(session)
	}
}

func (d *rootFSSessionDaemon) beginReconciliation(parent string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.inflight == nil {
		d.inflight = make(map[string]bool)
	}
	if d.inflight[parent] {
		return false
	}
	d.inflight[parent] = true
	return true
}

func (d *rootFSSessionDaemon) endReconciliation(parent string) {
	d.mu.Lock()
	delete(d.inflight, parent)
	d.mu.Unlock()
}

type httpNomadAllocationSource struct {
	url       string
	tokenFile string
	http      *http.Client
}

func newNomadAllocationSource(config NomadAllocationConfig) (nomadAllocationSource, error) {
	address := strings.TrimSpace(config.Address)
	nodeID := strings.TrimSpace(config.NodeID)
	if address == "" && nodeID == "" {
		return nil, nil
	}
	if address == "" || nodeID == "" {
		return nil, fmt.Errorf("Nomad address and node ID must be configured together")
	}
	parsed, err := url.Parse(address)
	if err != nil || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" ||
		(parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
		return nil, fmt.Errorf("Nomad address must be an HTTP(S) origin")
	}
	parsed.Path = strings.TrimSuffix(parsed.Path, "/") + "/v1/node/" + url.PathEscape(nodeID) + "/allocations"
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
				return nil, fmt.Errorf("Nomad CA contains no certificates")
			}
			tlsConfig.RootCAs = roots
		}
		if strings.TrimSpace(config.CertFile) != "" || strings.TrimSpace(config.KeyFile) != "" {
			if strings.TrimSpace(config.CertFile) == "" || strings.TrimSpace(config.KeyFile) == "" {
				return nil, fmt.Errorf("Nomad client certificate and key must be configured together")
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
		url: parsed.String(), tokenFile: strings.TrimSpace(config.TokenFile),
		http: &http.Client{Timeout: 2 * time.Second, Transport: transport},
	}, nil
}

func (s *httpNomadAllocationSource) ActiveAllocations(ctx context.Context) (map[string]bool, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, s.url, nil)
	if err != nil {
		return nil, err
	}
	if s.tokenFile != "" {
		token, err := os.ReadFile(s.tokenFile)
		if err != nil {
			return nil, fmt.Errorf("read Nomad token: %w", err)
		}
		request.Header.Set("X-Nomad-Token", strings.TrimSpace(string(token)))
	}
	response, err := s.http.Do(request)
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
	if forced || session.Kind == rootfssession.RecoveryPlannedRetire {
		return true
	}
	if session.Consumer != nil {
		deadline, err := session.Consumer.Validate()
		return err != nil || !session.Live || !now.Before(deadline)
	}
	return now.Sub(session.CreatedAt) >= rootFSSessionAttachGrace
}

func (d *rootFSSessionDaemon) reconcile(ctx context.Context, session rootfssession.RecoverySession) error {
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

func (d *rootFSSessionDaemon) fenceHostRuntime(
	ctx context.Context,
	session rootfssession.RecoverySession,
) (crashTaskObservation, error) {
	consumer := session.Consumer
	if consumer == nil {
		namespace, err := os.Readlink("/proc/self/ns/mnt")
		if err != nil {
			return crashTaskObservation{}, fmt.Errorf("read session daemon mount namespace: %w", err)
		}
		return crashTaskObservation{
			ActiveKey: session.Stage.Identity.ClaimID, HostMountNamespaceID: namespace,
			ContainerAbsent: true, TaskAbsent: true, FrontendSnapshotAbsent: true, StableMountAbsent: true,
		}, nil
	}
	namespace, err := os.Readlink("/proc/self/ns/mnt")
	if err != nil {
		return crashTaskObservation{}, fmt.Errorf("read session daemon mount namespace: %w", err)
	}
	if namespace != consumer.HostMountNamespace {
		return crashTaskObservation{}, fmt.Errorf(
			"consumer mount namespace %s differs from session daemon %s: %w",
			consumer.HostMountNamespace, namespace, errdefs.ErrFailedPrecondition,
		)
	}
	if err := validateConsumerMountPath(consumer.StableMount, d.config.RootFSConsumerMountRoot); err != nil {
		return crashTaskObservation{}, err
	}
	// A naturally stopped container may reject kill. Forced delete plus the
	// subsequent state lookup, not kill's exit status, is the absence proof.
	_ = d.runner.Kill(ctx, consumer.ContainerID, "KILL")
	if err := d.runner.Delete(ctx, consumer.ContainerID, true); err != nil && !errdefs.IsNotFound(err) {
		return crashTaskObservation{}, fmt.Errorf("delete orphan gVisor container: %w", err)
	}
	if _, err := d.runner.State(ctx, consumer.ContainerID); err == nil {
		return crashTaskObservation{}, fmt.Errorf("gVisor container %s remains present: %w", consumer.ContainerID, errdefs.ErrFailedPrecondition)
	} else if !errdefs.IsNotFound(err) {
		return crashTaskObservation{}, fmt.Errorf("attest orphan gVisor container absence: %w", err)
	}
	if err := d.mounter.Unmount(consumer.StableMount); err != nil {
		return crashTaskObservation{}, err
	}
	attached, err := hostMountAttached(consumer.StableMount)
	if err != nil {
		return crashTaskObservation{}, err
	}
	if attached {
		return crashTaskObservation{}, fmt.Errorf("stable task root %s remains mounted: %w", consumer.StableMount, errdefs.ErrFailedPrecondition)
	}
	return crashTaskObservation{
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
