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
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/containerd/errdefs"
	managerauthority "github.com/sandbox0-ai/sandbox0/manager/pkg/rootfswriterauthority"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/pkg/objectstore"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsrebase"
	rootfssession "github.com/sandbox0-ai/sandbox0/pkg/rootfssession"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/rootfswriterauthority"
	slotprotocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
)

// rootfsRuntime owns the node-local NBD/XFS/Overlay session inside the
// node-scoped ctld Nomad runtime. The task driver uses Runtime over Unix RPC.
type rootfsRuntime struct {
	sessions          *rootfssession.Manager
	authority         rootFSWriterAuthority
	info              RuntimeInfo
	logger            logger
	consumerMountRoot string
	consumerNetNSRoot string
	renewalMu         sync.Mutex
	renewals          map[string]*rootfsRenewal
}

// RuntimeInfo returns the immutable configuration owned by this ctld runtime.
func (r *rootfsRuntime) RuntimeInfo() (RuntimeInfo, error) {
	if r == nil {
		return RuntimeInfo{}, fmt.Errorf("RootFS runtime is unavailable: %w", errdefs.ErrUnavailable)
	}
	if err := r.info.Validate(); err != nil {
		return RuntimeInfo{}, fmt.Errorf("validate ctld Nomad runtime info: %w", err)
	}
	return r.info, nil
}

type rootFSWriterAuthority interface {
	ConsumeWriterGrant(context.Context, rootfshandoff.StageRequest) (protocol.LeaseObservation, error)
	RenewWriterGrant(context.Context, rootfshandoff.StageRequest) (protocol.LeaseObservation, error)
	RequestWriterPressurePause(context.Context, rootfshandoff.StageRequest, rootfsblock.DirtyTailPressure) (string, error)
	PublishWriterGrant(context.Context, rootfshandoff.StageRequest, managerauthority.PublishGenerationRequest) error
	PublishRunningFork(context.Context, rootfshandoff.StageRequest, rootfshandoff.RunningForkCheckpointRequest, rootfshandoff.RunningForkCheckpointResult) error
	BeginCrashAbandonWriterGrant(context.Context, rootfshandoff.StageRequest, string) error
	CompleteCrashAbandonWriterGrant(context.Context, rootfshandoff.StageRequest, string, rootfshandoff.CrashFenceProof) error
	CancelUnconsumedWriterGrant(context.Context, rootfshandoff.StageRequest) error
	VerifyTerminalWriterGrant(context.Context, rootfshandoff.StageRequest) error
}

// DirtyTailPressureSignal wakes the ctld node runtime as soon as a normal write is
// blocked before protected retirement capacity.
func (r *rootfsRuntime) DirtyTailPressureSignal() <-chan struct{} {
	if r == nil || r.sessions == nil {
		return nil
	}
	return r.sessions.DirtyTailPressureSignal()
}

func (r *rootfsRuntime) DirtyTailPressureSessions() ([]rootfssession.DirtyTailPressureSession, error) {
	if r == nil || r.sessions == nil {
		return nil, fmt.Errorf("RootFS session manager is unavailable: %w", errdefs.ErrUnavailable)
	}
	return r.sessions.DirtyTailPressureSessions()
}

// PlanDirtyTailPressure obtains the regional operation before promoting the
// pressure marker already persisted by DirtyTailPressureSessions to the same
// exact local retirement operation. A lost response is safe because both
// sides derive and retry one deterministic operation.
func (r *rootfsRuntime) PlanDirtyTailPressure(
	ctx context.Context,
	pressure rootfssession.DirtyTailPressureSession,
) (string, error) {
	if r == nil || r.sessions == nil || r.authority == nil {
		return "", fmt.Errorf("RootFS pressure planning authority is unavailable: %w", errdefs.ErrUnavailable)
	}
	stage := pressure.Stage.WithoutWriterGrantToken()
	operationID, err := r.authority.RequestWriterPressurePause(ctx, stage, pressure.Pressure)
	if err != nil {
		return "", err
	}
	expected := rootfshandoff.PlannedRetireOperationID(
		stage.Parent, stage.Identity.WriterGrantID, stage.Identity.WriterEpoch,
	)
	if operationID != expected {
		return "", fmt.Errorf("regional pressure operation does not match writer binding: %w", errdefs.ErrFailedPrecondition)
	}
	if err := r.sessions.BeginRetire(stage.Parent, stage.Identity, operationID); err != nil {
		return "", fmt.Errorf("persist local pressure retirement: %w", err)
	}
	return operationID, nil
}

// Runtime is the driver-facing RootFS attachment and retire boundary.
type Runtime interface {
	Ensure(context.Context, rootfshandoff.StageRequest, func(error)) (rootfssession.Mount, error)
	RegisterConsumer(context.Context, rootfshandoff.StageRequest, ConsumerRequest) (ConsumerLease, error)
	RenewConsumer(context.Context, rootfshandoff.StageRequest, ConsumerLease) (ConsumerLease, error)
	CaptureRunningFork(context.Context, rootfshandoff.StageRequest, rootfshandoff.RunningForkCheckpointRequest) (rootfshandoff.RunningForkCheckpointResult, error)
	Retire(context.Context, rootfshandoff.StageRequest, string) (rootfssession.RetireResult, error)
	CrashFence(context.Context, rootfshandoff.StageRequest, string, CrashTaskObservation) (rootfshandoff.CrashFenceProof, error)
}

// ExecutePausedRebase runs one exact offline worker operation. Regional
// publication and result acknowledgement are orchestrated by the caller.
func (r *rootfsRuntime) ExecutePausedRebase(
	ctx context.Context,
	request rootfsrebase.WorkerRequest,
) (rootfsrebase.WorkerResult, error) {
	if r == nil || r.sessions == nil {
		return rootfsrebase.WorkerResult{}, fmt.Errorf("RootFS session manager is unavailable: %w", errdefs.ErrUnavailable)
	}
	return r.sessions.ExecuteRebase(ctx, request)
}

// RejectPausedRebase fences execution or reports an already-produced output
// until the regional authority records its permanent rejection.
func (r *rootfsRuntime) RejectPausedRebase(
	ctx context.Context,
	request rootfsrebase.WorkerRequest,
) (rootfsrebase.WorkerRejection, error) {
	if r == nil || r.sessions == nil {
		return rootfsrebase.WorkerRejection{}, fmt.Errorf("RootFS session manager is unavailable: %w", errdefs.ErrUnavailable)
	}
	return r.sessions.RejectRebase(ctx, request)
}

// AcknowledgePausedRebase discards one exact cached worker output only after
// the regional transaction has reached a permanent outcome.
func (r *rootfsRuntime) AcknowledgePausedRebase(
	request rootfsrebase.WorkerRequest,
	proofDigest string,
) error {
	if r == nil || r.sessions == nil {
		return fmt.Errorf("RootFS session manager is unavailable: %w", errdefs.ErrUnavailable)
	}
	return r.sessions.AcknowledgeRebase(request, proofDigest)
}

func (r *rootfsRuntime) CaptureRunningFork(
	ctx context.Context,
	request rootfshandoff.StageRequest,
	fork rootfshandoff.RunningForkCheckpointRequest,
) (rootfshandoff.RunningForkCheckpointResult, error) {
	if r.authority == nil {
		return rootfshandoff.RunningForkCheckpointResult{}, fmt.Errorf("RootFS writer authority is not configured: %w", errdefs.ErrUnavailable)
	}
	durable := request.WithoutWriterGrantToken()
	checkpoint, err := r.sessions.CaptureRunningFork(ctx, durable, fork)
	if err != nil {
		return rootfshandoff.RunningForkCheckpointResult{}, err
	}
	if err := r.authority.PublishRunningFork(ctx, durable, fork, checkpoint); err != nil {
		return rootfshandoff.RunningForkCheckpointResult{}, releaseRejectedRunningFork(
			err,
			func() error {
				return r.sessions.AcknowledgeRunningFork(
					durable, fork.OperationID, checkpoint.ProofDigest,
				)
			},
		)
	}
	if err := r.sessions.AcknowledgeRunningFork(durable, fork.OperationID, checkpoint.ProofDigest); err != nil {
		return rootfshandoff.RunningForkCheckpointResult{}, fmt.Errorf("acknowledge regional running fork: %w", err)
	}
	return checkpoint, nil
}

func releaseRejectedRunningFork(publishErr error, acknowledge func() error) error {
	if publishErr == nil || !errdefs.IsFailedPrecondition(publishErr) || acknowledge == nil {
		return publishErr
	}
	if err := acknowledge(); err != nil {
		return errors.Join(
			publishErr,
			fmt.Errorf("release permanently rejected running fork: %w", err),
		)
	}
	return publishErr
}

// ConsumerRequest binds the durable block writer to the exact host
// runtime artifacts that must be absent before crash abandonment.
type ConsumerRequest struct {
	ActiveKey          string `json:"active_key"`
	ContainerID        string `json:"container_id"`
	StableMount        string `json:"stable_mount"`
	HostMountNamespace string `json:"host_mount_namespace"`
	NetNSPath          string `json:"netns_path,omitempty"`
	NetNSIdentity      string `json:"netns_identity,omitempty"`
	NetworkChain       string `json:"network_chain,omitempty"`
}

// ConsumerLease is issued by the node session owner. A restarted plugin
// re-registers and rotates LeaseID so stale instances cannot keep an orphan
// writer alive.
type ConsumerLease struct {
	LeaseID   string    `json:"lease_id"`
	ExpiresAt time.Time `json:"expires_at"`
}

const rootFSConsumerLeaseTTL = 30 * time.Second

type CrashTaskObservation struct {
	ActiveKey              string
	ContainerID            string
	HostMountNamespaceID   string
	ContainerAbsent        bool
	TaskAbsent             bool
	FrontendSnapshotAbsent bool
	StableMountAbsent      bool
}

type rootfsRenewal struct {
	cancel context.CancelFunc
}

// ConsumedAttachError reports an attach failure after the regional writer
// grant was consumed. Callers must crash-fence rather than roll back the claim.
type ConsumedAttachError struct {
	Err error
}

func (e *ConsumedAttachError) Error() string {
	if e == nil || e.Err == nil {
		return "RootFS attach failed after writer grant consumption"
	}
	return e.Err.Error()
}

func (e *ConsumedAttachError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

func newRuntime(ctx context.Context, config *Config, logger logger) (*rootfsRuntime, error) {
	if config == nil {
		return nil, fmt.Errorf("Nomad RootFS runtime configuration is required")
	}
	store, err := objectstore.Create(objectstore.Config{
		Type: config.RootFSObjectType, Bucket: config.RootFSObjectBucket,
		Region: config.RootFSObjectRegion, Endpoint: config.RootFSObjectEndpoint,
		AccessKey: config.RootFSObjectAccessKey, SecretKey: config.RootFSObjectSecretKey,
		SessionToken: config.RootFSObjectSessionToken,
	})
	if err != nil {
		return nil, fmt.Errorf("create RootFS object store: %w", err)
	}
	if config.RootFSObjectEncryptionEnabled {
		keyPEM, err := objectstore.LoadEncryptionKey(config.RootFSObjectEncryptionKeyPath)
		if err != nil {
			return nil, fmt.Errorf("load RootFS object encryption key: %w", err)
		}
		keyEncryptor, err := objectstore.NewKeyEncryptor(keyPEM, config.RootFSObjectEncryptionPassphrase)
		if err != nil {
			return nil, fmt.Errorf("create RootFS object key encryptor: %w", err)
		}
		store = objectstore.Encrypting(store, objectstore.EncryptionConfig{
			Enabled:      true,
			Algorithm:    config.RootFSObjectEncryptionAlgorithm,
			KeyEncryptor: keyEncryptor,
		})
	}
	conditional, ok := store.(objectstore.ConditionalStore)
	if !ok {
		return nil, fmt.Errorf("RootFS object store %s does not support conditional create", store)
	}
	if err := store.Create(); err != nil && !strings.Contains(strings.ToLower(err.Error()), "alreadyownedbyyou") {
		return nil, fmt.Errorf("create RootFS bucket: %w", err)
	}
	hostRuntime, err := rootfssession.NewLinuxRuntime(rootfssession.LinuxRuntimeConfig{
		DevicePaths: config.RootFSNBDDevices,
	})
	if err != nil {
		return nil, fmt.Errorf("create RootFS host runtime: %w", err)
	}
	sessions, err := rootfssession.New(rootfssession.Config{
		StatePath: config.RootFSStatePath, BranchRoot: config.RootFSBranchRoot,
		MountRoot: config.RootFSMountRoot, MaxDirtyTailBytes: config.RootFSMaxDirtyTailBytes,
		MaxNodeDirtyTailBytes:           config.RootFSMaxNodeDirtyTailBytes,
		DirtyTailRetirementReserveBytes: config.RootFSDirtyTailRetirementReserveBytes,
		Source:                          conditional,
		Publisher:                       rootfsblock.ObjectStorePublisher{Store: conditional}, Runtime: hostRuntime,
	})
	if err != nil {
		return nil, fmt.Errorf("create RootFS session manager: %w", err)
	}
	reconcileCtx, reconcileCancel := context.WithTimeout(ctx, 30*time.Second)
	err = sessions.ReconcileFreezes(reconcileCtx)
	if err == nil {
		err = sessions.ReconcileRunningForkCaptures(reconcileCtx)
	}
	if err == nil {
		err = sessions.ReconcileRebases(reconcileCtx)
	}
	if err == nil {
		err = sessions.ReconcileReleases(reconcileCtx)
	}
	reconcileCancel()
	if err != nil {
		_ = sessions.Close()
		return nil, fmt.Errorf("reconcile RootFS session journal: %w", err)
	}
	dirtyTailUsage := sessions.NodeDirtyTailUsage()
	logger.Info("RootFS node dirty tail budget initialized",
		"used_bytes", dirtyTailUsage.UsedBytes,
		"reserved_bytes", dirtyTailUsage.ReservedBytes,
		"max_bytes", dirtyTailUsage.MaxBytes,
		"journal_owners", dirtyTailUsage.Owners,
	)
	var authority rootFSWriterAuthority
	if config.RootFSAuthorityURL != "" {
		client, clientErr := managerauthority.NewManagerClient(managerauthority.ManagerClientConfig{
			BaseURL: config.RootFSAuthorityURL, CAFile: config.RootFSAuthorityCAFile,
			ClientCertFile: config.RootFSAuthorityClientCertFile,
			ClientKeyFile:  config.RootFSAuthorityClientKeyFile,
			TokenFile:      config.RootFSAuthorityTokenFile, Timeout: 2 * time.Second,
		})
		if clientErr != nil {
			_ = sessions.Close()
			return nil, fmt.Errorf("create RootFS writer authority client: %w", clientErr)
		}
		authority = client
	}
	return &rootfsRuntime{
		sessions: sessions, authority: authority, info: runtimeInfoFromConfig(*config), logger: logger,
		consumerMountRoot: strings.TrimSpace(config.RootFSConsumerMountRoot),
		consumerNetNSRoot: strings.TrimSpace(config.RootFSConsumerNetNSRoot),
		renewals:          make(map[string]*rootfsRenewal),
	}, nil
}

func (r *rootfsRuntime) Ensure(
	ctx context.Context,
	request rootfshandoff.StageRequest,
	onLeaseLost func(error),
) (rootfssession.Mount, error) {
	if err := request.Validate(); err != nil {
		return rootfssession.Mount{}, err
	}
	if err := r.sessions.Reserve(request); err != nil {
		return rootfssession.Mount{}, fmt.Errorf("reserve durable RootFS session: %w", err)
	}
	if r.authority != nil {
		if onLeaseLost == nil {
			return rootfssession.Mount{}, fmt.Errorf("writer lease loss handler is required")
		}
		observation, err := r.authority.ConsumeWriterGrant(ctx, request)
		if err != nil {
			return rootfssession.Mount{}, fmt.Errorf("consume regional writer grant: %w", err)
		}
		r.startRenewal(request, observation, onLeaseLost)
	}
	mount, err := r.sessions.Ensure(ctx, request)
	if err != nil && r.authority != nil {
		return rootfssession.Mount{}, &ConsumedAttachError{Err: err}
	}
	return mount, err
}

func (r *rootfsRuntime) RegisterConsumer(
	_ context.Context,
	request rootfshandoff.StageRequest,
	consumer ConsumerRequest,
) (ConsumerLease, error) {
	hostMountNamespace, err := os.Readlink("/proc/self/ns/mnt")
	if err != nil {
		return ConsumerLease{}, fmt.Errorf("read RootFS owner mount namespace: %w", err)
	}
	if strings.TrimSpace(consumer.HostMountNamespace) != hostMountNamespace {
		return ConsumerLease{}, fmt.Errorf("RootFS consumer is outside the session owner mount namespace: %w", errdefs.ErrFailedPrecondition)
	}
	if r.consumerMountRoot != "" {
		resolvedMount, err := validateRootfsPath(consumer.StableMount, r.consumerMountRoot)
		if err != nil {
			return ConsumerLease{}, fmt.Errorf("validate RootFS consumer mount: %w", err)
		}
		consumer.StableMount = resolvedMount
	}
	if consumer.NetNSPath != "" || consumer.NetNSIdentity != "" || consumer.NetworkChain != "" {
		if !filepath.IsAbs(consumer.NetNSPath) || filepath.Clean(consumer.NetNSPath) == string(filepath.Separator) ||
			strings.TrimSpace(consumer.NetNSIdentity) == "" || strings.TrimSpace(consumer.NetworkChain) == "" {
			return ConsumerLease{}, fmt.Errorf("RootFS consumer network identity is incomplete: %w", errdefs.ErrInvalidArgument)
		}
		if r.consumerNetNSRoot == "" {
			return ConsumerLease{}, fmt.Errorf("RootFS consumer network namespace root is not configured: %w", errdefs.ErrFailedPrecondition)
		}
		resolvedNetNS, err := validateExistingPath(consumer.NetNSPath, r.consumerNetNSRoot)
		if err != nil {
			return ConsumerLease{}, fmt.Errorf("validate RootFS consumer network namespace: %w: %w", err, errdefs.ErrFailedPrecondition)
		}
		consumer.NetNSPath = resolvedNetNS
		observedIdentity, err := runtimePathIdentity(consumer.NetNSPath, "netns-v1", "Nomad network namespace")
		if err != nil {
			return ConsumerLease{}, err
		}
		if observedIdentity != consumer.NetNSIdentity ||
			request.ExpectedPolicyToken.NetNSIdentity != consumer.NetNSIdentity ||
			slotprotocol.NomadNetworkChainName(consumer.ContainerID) != consumer.NetworkChain {
			return ConsumerLease{}, fmt.Errorf("RootFS consumer network identity changed: %w", errdefs.ErrFailedPrecondition)
		}
	}
	leaseIDBytes := make([]byte, 32)
	if _, err := rand.Read(leaseIDBytes); err != nil {
		return ConsumerLease{}, fmt.Errorf("generate RootFS consumer lease: %w", err)
	}
	lease := ConsumerLease{
		LeaseID: hex.EncodeToString(leaseIDBytes), ExpiresAt: time.Now().Add(rootFSConsumerLeaseTTL).UTC(),
	}
	err = r.sessions.RegisterConsumer(request.Parent, request.Identity, rootfssession.ConsumerRegistration{
		LeaseID: lease.LeaseID, ActiveKey: consumer.ActiveKey, ContainerID: consumer.ContainerID,
		StableMount: consumer.StableMount, HostMountNamespace: consumer.HostMountNamespace,
		NetNSPath: consumer.NetNSPath, NetNSIdentity: consumer.NetNSIdentity, NetworkChain: consumer.NetworkChain,
		LeaseExpiresAt: lease.ExpiresAt.Format(time.RFC3339Nano),
	})
	if err != nil {
		return ConsumerLease{}, err
	}
	return lease, nil
}

func (r *rootfsRuntime) RenewConsumer(
	_ context.Context,
	request rootfshandoff.StageRequest,
	lease ConsumerLease,
) (ConsumerLease, error) {
	lease.ExpiresAt = time.Now().Add(rootFSConsumerLeaseTTL).UTC()
	if err := r.sessions.RenewConsumer(request.Parent, request.Identity, lease.LeaseID, lease.ExpiresAt); err != nil {
		return ConsumerLease{}, err
	}
	return lease, nil
}

func (r *rootfsRuntime) RecoverySessions() ([]rootfssession.RecoverySession, error) {
	return r.sessions.RecoverySessions()
}

func (r *rootfsRuntime) Retire(ctx context.Context, request rootfshandoff.StageRequest, operationID string) (rootfssession.RetireResult, error) {
	if strings.TrimSpace(operationID) == "" {
		return rootfssession.RetireResult{}, fmt.Errorf("retire operation ID is required")
	}
	// Sealing a metadata-heavy branch and publishing its immutable objects can
	// take longer than one lease TTL. Keep the exact writer lease alive until
	// the regional terminal CAS has completed. A failed cleanup remains
	// retryable with the same operation, so it must keep renewing until either a
	// retry publishes the head or crash recovery fences the writer.
	if err := r.sessions.BeginRetire(request.Parent, request.Identity, operationID); err != nil {
		if errdefs.IsNotFound(err) && r.authority != nil {
			if verifyErr := r.authority.VerifyTerminalWriterGrant(ctx, request); verifyErr == nil {
				r.stopRenewal(request.Parent)
				return rootfssession.RetireResult{}, nil
			} else {
				return rootfssession.RetireResult{}, errors.Join(
					err, fmt.Errorf("verify forgotten regional writer retirement: %w", verifyErr),
				)
			}
		}
		return rootfssession.RetireResult{}, err
	}
	if err := r.releaseRetiringSession(ctx, request); err != nil {
		return rootfssession.RetireResult{}, err
	}
	result, err := r.sessions.RetireResult(request.Parent, request.Identity, operationID)
	if err != nil {
		return rootfssession.RetireResult{}, err
	}
	if result.DurabilityState == "" || result.Descriptor == nil {
		return rootfssession.RetireResult{}, fmt.Errorf("RootFS retire result is not durable")
	}
	if r.authority != nil {
		sealedID := "generation-" + strings.TrimPrefix(result.CurrentBlockHead, "sha256:")
		generation := sandboxstore.RootFSGeneration{
			ID: sealedID, FilesystemID: request.Identity.RootFSID,
			ParentGenerationID: request.InitialGeneration,
			SourceOCIDigest:    request.Generation.SourceOCIDigest,
			BaseArtifactDigest: request.Generation.BaseArtifactDigest,
			BaseBlockRoot:      request.Generation.BaseBlockRoot,
			CurrentBlockHead:   result.CurrentBlockHead,
			WriterEpoch:        request.Identity.WriterEpoch,
			FormatGeneration:   request.Generation.FormatGeneration,
			DurabilityState:    result.DurabilityState,
			LocatorVersion:     request.Generation.LocatorVersion + 1,
			Descriptor:         result.Descriptor,
		}
		if err := r.authority.PublishWriterGrant(ctx, request, managerauthority.PublishGenerationRequest{
			OperationID: operationID, ProofDigest: hex.EncodeToString(result.DetachProof),
			ExpectedOldGenerationID: request.InitialGeneration, Generation: generation,
		}); err != nil {
			return result, fmt.Errorf("publish regional writer retirement: %w", err)
		}
		r.stopRenewal(request.Parent)
		if err := r.sessions.ReclaimTerminalArtifacts(request.Parent, request.Identity); err != nil {
			return result, fmt.Errorf("reclaim published RootFS artifacts: %w", err)
		}
		if err := r.authority.VerifyTerminalWriterGrant(ctx, request); err != nil {
			return result, fmt.Errorf("verify published regional writer retirement: %w", err)
		}
		if err := r.sessions.ForgetVerifiedTerminal(request.Parent, request.Identity); err != nil {
			return result, fmt.Errorf("forget published RootFS session: %w", err)
		}
		return result, nil
	}
	r.stopRenewal(request.Parent)
	return result, nil
}

func (r *rootfsRuntime) releaseRetiringSession(ctx context.Context, request rootfshandoff.StageRequest) error {
	delay := 100 * time.Millisecond
	for {
		err := r.sessions.Release(ctx, request.Identity)
		if err == nil {
			return nil
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return errors.Join(err, fmt.Errorf("retry retiring RootFS session: %w", ctx.Err()))
		case <-timer.C:
		}
		if delay < time.Second {
			delay *= 2
			if delay > time.Second {
				delay = time.Second
			}
		}
	}
}

// FenceLocalRootFSWriter detaches and attests one exact node-local writer after
// a separate regional controller has already fenced renewal authority. It
// deliberately does not call or complete the regional writer authority.
func (r *rootfsRuntime) FenceLocalRootFSWriter(
	ctx context.Context,
	request rootfshandoff.StageRequest,
	operationID string,
	task CrashTaskObservation,
) (rootfshandoff.CrashFenceProof, error) {
	return r.fenceLocalRootFSWriter(ctx, request, operationID, task, true)
}

func (r *rootfsRuntime) fenceLocalRootFSWriter(
	ctx context.Context,
	request rootfshandoff.StageRequest,
	operationID string,
	task CrashTaskObservation,
	external bool,
) (rootfshandoff.CrashFenceProof, error) {
	operationID = strings.TrimSpace(operationID)
	if operationID == "" || request.Generation == nil {
		return rootfshandoff.CrashFenceProof{}, fmt.Errorf("local crash operation and generation are required")
	}
	if err := request.ValidateDurableBinding(); err != nil {
		return rootfshandoff.CrashFenceProof{}, err
	}
	r.stopRenewal(request.Parent)
	if err := r.sessions.Release(ctx, request.Identity); err != nil {
		return rootfshandoff.CrashFenceProof{}, fmt.Errorf("release crashed RootFS session: %w", err)
	}
	var (
		session rootfshandoff.CrashFenceSessionObservation
		err     error
	)
	if external {
		session, err = r.sessions.CrashFenceExternal(request, operationID)
	} else {
		session, err = r.sessions.CrashFence(request, operationID)
	}
	if err != nil {
		return rootfshandoff.CrashFenceProof{}, fmt.Errorf("attest crashed RootFS session: %w", err)
	}
	binding, err := request.BindingDigest()
	if err != nil {
		return rootfshandoff.CrashFenceProof{}, err
	}
	proof := rootfshandoff.CrashFenceProof{
		Version: rootfshandoff.CrashFenceProofVersion, OperationID: operationID,
		Parent: request.Parent, ClaimID: request.Identity.ClaimID,
		WriterGrantID: request.Identity.WriterGrantID, WriterEpoch: request.Identity.WriterEpoch,
		BindingVersion: request.BindingVersion, BindingDigest: hex.EncodeToString(binding[:]),
		RootFSID: request.Identity.RootFSID, InitialGeneration: request.InitialGeneration,
		InitialBlockHead: request.Generation.CurrentBlockHead,
		HeadAction:       rootfshandoff.CrashFenceHeadKeepInitial,
		NodeUID:          request.Identity.NodeUID, BootID: request.Identity.BootID,
		RuntimeGeneration:    request.Identity.RuntimeGeneration,
		HostMountNamespaceID: task.HostMountNamespaceID,
		PodUID:               request.Identity.PodUID, PodSandboxID: request.Identity.PodSandboxID,
		ContainerName: request.Identity.ContainerName, SlotNonce: request.Identity.SlotNonce,
		ActiveKey: task.ActiveKey, ConsumerBound: task.ContainerID != "", ContainerID: task.ContainerID,
		ContainerAbsent: task.ContainerAbsent, TaskAbsent: task.TaskAbsent,
		FrontendSnapshotAbsent: task.FrontendSnapshotAbsent, StableMountAbsent: task.StableMountAbsent,
		SnapshotterState: rootfshandoff.StateTombstoned, Session: session, ObservedAt: session.ObservedAt,
	}
	if err := proof.Validate(); err != nil {
		return rootfshandoff.CrashFenceProof{}, fmt.Errorf("validate Nomad crash fence proof: %w", err)
	}
	return proof, nil
}

// CrashFence abandons an unsealed writer only after the regional lease fence
// is authoritative and the node has proved every local runtime owner absent.
func (r *rootfsRuntime) CrashFence(
	ctx context.Context,
	request rootfshandoff.StageRequest,
	operationID string,
	task CrashTaskObservation,
) (rootfshandoff.CrashFenceProof, error) {
	operationID = strings.TrimSpace(operationID)
	if operationID == "" || request.Generation == nil {
		return rootfshandoff.CrashFenceProof{}, fmt.Errorf("crash operation and generation are required")
	}
	if err := request.ValidateDurableBinding(); err != nil {
		return rootfshandoff.CrashFenceProof{}, err
	}
	if r.authority == nil {
		return rootfshandoff.CrashFenceProof{}, fmt.Errorf("regional writer authority is required for crash fencing")
	}
	r.stopRenewal(request.Parent)
	if err := r.authority.VerifyTerminalWriterGrant(ctx, request); err == nil {
		if err := r.finalizeVerifiedTerminal(request); err != nil {
			return rootfshandoff.CrashFenceProof{}, fmt.Errorf("finalize verified terminal RootFS session: %w", err)
		}
		return rootfshandoff.CrashFenceProof{}, nil
	} else if !errdefs.IsFailedPrecondition(err) {
		return rootfshandoff.CrashFenceProof{}, fmt.Errorf("verify previous regional crash retirement: %w", err)
	}
	if err := r.authority.CancelUnconsumedWriterGrant(ctx, request); err == nil {
		if err := r.sessions.Release(ctx, request.Identity); err != nil {
			return rootfshandoff.CrashFenceProof{}, fmt.Errorf("release canceled preconsume RootFS session: %w", err)
		}
		if _, err := r.sessions.CrashFence(request, operationID); err != nil {
			return rootfshandoff.CrashFenceProof{}, fmt.Errorf("attest canceled preconsume RootFS session: %w", err)
		}
		if err := r.sessions.ReclaimTerminalArtifacts(request.Parent, request.Identity); err != nil {
			return rootfshandoff.CrashFenceProof{}, fmt.Errorf("reclaim canceled preconsume RootFS artifacts: %w", err)
		}
		if err := r.authority.VerifyTerminalWriterGrant(ctx, request); err != nil {
			return rootfshandoff.CrashFenceProof{}, fmt.Errorf("verify canceled preconsume writer grant: %w", err)
		}
		if err := r.sessions.ForgetVerifiedTerminal(request.Parent, request.Identity); err != nil {
			return rootfshandoff.CrashFenceProof{}, fmt.Errorf("forget canceled preconsume RootFS session: %w", err)
		}
		return rootfshandoff.CrashFenceProof{}, nil
	} else if !errdefs.IsFailedPrecondition(err) {
		return rootfshandoff.CrashFenceProof{}, fmt.Errorf("cancel unconsumed regional writer grant: %w", err)
	}
	if err := r.awaitRegionalCrashFence(ctx, request, operationID); err != nil {
		return rootfshandoff.CrashFenceProof{}, err
	}
	proof, err := r.fenceLocalRootFSWriter(ctx, request, operationID, task, false)
	if err != nil {
		return rootfshandoff.CrashFenceProof{}, err
	}
	if err := r.authority.CompleteCrashAbandonWriterGrant(ctx, request, operationID, proof); err != nil {
		return rootfshandoff.CrashFenceProof{}, fmt.Errorf("complete regional writer crash abandon: %w", err)
	}
	if err := r.sessions.ReclaimTerminalArtifacts(request.Parent, request.Identity); err != nil {
		return rootfshandoff.CrashFenceProof{}, fmt.Errorf("reclaim crash-abandoned RootFS artifacts: %w", err)
	}
	if err := r.authority.VerifyTerminalWriterGrant(ctx, request); err != nil {
		return rootfshandoff.CrashFenceProof{}, fmt.Errorf("verify crash-abandoned regional writer retirement: %w", err)
	}
	if err := r.sessions.ForgetVerifiedTerminal(request.Parent, request.Identity); err != nil {
		return rootfshandoff.CrashFenceProof{}, fmt.Errorf("forget crash-abandoned RootFS session: %w", err)
	}
	return proof, nil
}

func (r *rootfsRuntime) finalizeVerifiedTerminal(request rootfshandoff.StageRequest) error {
	sessions, err := r.sessions.RecoverySessions()
	if err != nil {
		return fmt.Errorf("inspect terminal RootFS session ownership: %w", err)
	}
	keepProof := false
	for _, session := range sessions {
		if session.Stage.Parent != request.Parent {
			continue
		}
		if session.Stage.Identity.RootFSID != request.Identity.RootFSID ||
			session.Stage.Identity.WriterEpoch != request.Identity.WriterEpoch {
			return fmt.Errorf("terminal RootFS session belongs to another writer: %w", errdefs.ErrFailedPrecondition)
		}
		keepProof = session.ExternalCrash
		break
	}
	if err := r.sessions.ReclaimTerminalArtifacts(request.Parent, request.Identity); err != nil {
		if errdefs.IsNotFound(err) {
			return nil
		}
		return err
	}
	if keepProof {
		return nil
	}
	return r.sessions.ForgetVerifiedTerminal(request.Parent, request.Identity)
}

// ReclaimVerifiedTerminal removes a compact internal crash session only after
// the regional authority proves that its exact durable writer binding is
// terminal. This lets plugin-independent slot cleanup finish a response-loss
// window without replacing the original crash operation.
func (r *rootfsRuntime) ReclaimVerifiedTerminal(
	ctx context.Context,
	request rootfshandoff.StageRequest,
) error {
	if r.authority == nil {
		return fmt.Errorf("regional writer authority is required for terminal reclaim")
	}
	if err := r.authority.VerifyTerminalWriterGrant(ctx, request); err != nil {
		return fmt.Errorf("verify terminal writer grant: %w", err)
	}
	return r.finalizeVerifiedTerminal(request)
}

// ReclaimExternallyRetired removes large node-local artifacts once the
// regional controller has made an external crash fence terminal. The compact
// journal record remains for a bounded retry window after those artifacts are
// gone.
func (r *rootfsRuntime) ReclaimExternallyRetired(
	ctx context.Context,
	request rootfshandoff.StageRequest,
) (bool, error) {
	if r.authority == nil {
		return false, fmt.Errorf("regional writer authority is required for external retirement")
	}
	if err := r.authority.VerifyTerminalWriterGrant(ctx, request); err != nil {
		if errdefs.IsFailedPrecondition(err) {
			return false, nil
		}
		return false, fmt.Errorf("verify externally retired writer grant: %w", err)
	}
	sessions, err := r.sessions.RecoverySessions()
	if err != nil {
		return false, fmt.Errorf("inspect external RootFS crash fence: %w", err)
	}
	var matched *rootfssession.RecoverySession
	for index := range sessions {
		if sessions[index].Stage.Parent == request.Parent {
			candidate := sessions[index]
			matched = &candidate
			break
		}
	}
	if matched != nil && (!matched.ExternalCrash || matched.Stage.Identity.RootFSID != request.Identity.RootFSID ||
		matched.Stage.Identity.WriterEpoch != request.Identity.WriterEpoch) {
		return false, fmt.Errorf("external RootFS crash fence belongs to another writer: %w", errdefs.ErrFailedPrecondition)
	}
	if err := r.sessions.ReclaimTerminalArtifacts(request.Parent, request.Identity); err != nil && !errdefs.IsNotFound(err) {
		return false, fmt.Errorf("reclaim externally retired RootFS artifacts: %w", err)
	}
	if matched != nil && !matched.CrashRequestedAt.IsZero() &&
		!time.Now().Before(matched.CrashRequestedAt.Add(rootfssession.ExternalTerminalProofRetention)) {
		if err := r.sessions.ForgetVerifiedTerminal(request.Parent, request.Identity); err != nil {
			return false, fmt.Errorf("forget expired external RootFS proof: %w", err)
		}
	}
	return true, nil
}

func (r *rootfsRuntime) awaitRegionalCrashFence(
	ctx context.Context,
	request rootfshandoff.StageRequest,
	operationID string,
) error {
	for {
		err := r.authority.BeginCrashAbandonWriterGrant(ctx, request, operationID)
		if err == nil {
			return nil
		}
		if !errdefs.IsFailedPrecondition(err) {
			return fmt.Errorf("begin regional writer crash abandon: %w", err)
		}
		timer := time.NewTimer(time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return fmt.Errorf("wait for regional writer crash fence: %w", ctx.Err())
		case <-timer.C:
		}
	}
}

func (r *rootfsRuntime) Close() error {
	if r == nil || r.sessions == nil {
		return nil
	}
	r.stopAllRenewals()
	return r.sessions.Close()
}

func (r *rootfsRuntime) startRenewal(
	request rootfshandoff.StageRequest,
	observation protocol.LeaseObservation,
	onLeaseLost func(error),
) {
	ctx, cancel := context.WithCancel(context.Background())
	r.renewalMu.Lock()
	if old := r.renewals[request.Parent]; old != nil {
		old.cancel()
	}
	renewal := &rootfsRenewal{cancel: cancel}
	r.renewals[request.Parent] = renewal
	r.renewalMu.Unlock()
	go func() {
		defer func() {
			r.renewalMu.Lock()
			if r.renewals[request.Parent] == renewal {
				delete(r.renewals, request.Parent)
			}
			r.renewalMu.Unlock()
		}()
		err := runWriterLeaseRenewal(ctx, request, observation, func(
			ctx context.Context,
			request rootfshandoff.StageRequest,
		) (protocol.LeaseObservation, error) {
			return r.authority.RenewWriterGrant(ctx, request)
		})
		if err != nil {
			r.logger.Error("RootFS writer lease lost", "parent", request.Parent, "error", err)
			if onLeaseLost != nil {
				onLeaseLost(err)
			}
		}
	}()
}

type writerLeaseRenewFunc func(
	context.Context,
	rootfshandoff.StageRequest,
) (protocol.LeaseObservation, error)

// runWriterLeaseRenewal converts authority clock observations to monotonic
// local durations and fails closed when the last observed lease expires. A
// node must not leave the attached block writer running after it can no longer
// prove that the regional authority still recognizes that writer.
func runWriterLeaseRenewal(
	ctx context.Context,
	request rootfshandoff.StageRequest,
	observation protocol.LeaseObservation,
	renew writerLeaseRenewFunc,
) error {
	if renew == nil {
		return fmt.Errorf("writer lease renew function is required")
	}
	renewAt, expiresAt, err := localWriterLeaseSchedule(observation)
	if err != nil {
		return err
	}
	var lastErr error
	for {
		deadline := renewAt
		if expiresAt.Before(deadline) {
			deadline = expiresAt
		}
		timer := time.NewTimer(time.Until(deadline))
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-timer.C:
		}
		if !time.Now().Before(expiresAt) {
			return errors.Join(lastErr, errors.New("RootFS writer lease expired"))
		}

		renewCtx, cancel := context.WithDeadline(ctx, expiresAt)
		next, renewErr := renew(renewCtx, request)
		cancel()
		if renewErr == nil {
			renewAt, expiresAt, err = localWriterLeaseSchedule(next)
			if err != nil {
				return fmt.Errorf("invalid renewed writer lease: %w", err)
			}
			lastErr = nil
			continue
		}
		if ctx.Err() != nil {
			return nil
		}
		lastErr = renewErr
		if writerLeaseRenewalIsTerminal(renewErr) {
			return fmt.Errorf("writer authority rejected lease renewal: %w", renewErr)
		}
		if !time.Now().Before(expiresAt) {
			return errors.Join(renewErr, errors.New("RootFS writer lease expired"))
		}
		renewAt = time.Now().Add(time.Second)
		if expiresAt.Before(renewAt) {
			renewAt = expiresAt
		}
	}
}

func localWriterLeaseSchedule(observation protocol.LeaseObservation) (time.Time, time.Time, error) {
	if err := observation.Validate(); err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("validate writer lease observation: %w", err)
	}
	now := time.Now()
	renewAfter := observation.RenewAfter.Sub(observation.ServerTime)
	remaining := observation.LeaseExpiresAt.Sub(observation.ServerTime)
	return now.Add(renewAfter), now.Add(remaining), nil
}

func writerLeaseRenewalIsTerminal(err error) bool {
	return errdefs.IsInvalidArgument(err) || errdefs.IsNotFound(err) ||
		errdefs.IsPermissionDenied(err) || errdefs.IsFailedPrecondition(err)
}

func (r *rootfsRuntime) stopRenewal(parent string) {
	r.renewalMu.Lock()
	renewal := r.renewals[parent]
	delete(r.renewals, parent)
	r.renewalMu.Unlock()
	if renewal != nil {
		renewal.cancel()
	}
}

func (r *rootfsRuntime) stopAllRenewals() {
	r.renewalMu.Lock()
	renewals := make([]*rootfsRenewal, 0, len(r.renewals))
	for parent, renewal := range r.renewals {
		renewals = append(renewals, renewal)
		delete(r.renewals, parent)
	}
	r.renewalMu.Unlock()
	for _, renewal := range renewals {
		renewal.cancel()
	}
}

func validateRootFSConfig(config *Config) error {
	if config == nil {
		return fmt.Errorf("Nomad RootFS runtime configuration is required")
	}
	if config.RootFSMaxDirtyTailBytes < 0 {
		return fmt.Errorf("rootfs_max_dirty_tail_bytes must be non-negative")
	}
	if config.RootFSMaxNodeDirtyTailBytes < 0 {
		return fmt.Errorf("rootfs_max_node_dirty_tail_bytes must be non-negative")
	}
	if config.RootFSDirtyTailRetirementReserveBytes < 0 {
		return fmt.Errorf("rootfs_dirty_tail_retirement_reserve_bytes must be non-negative")
	}
	if config.RootFSMaxNodeDirtyTailBytes > 0 &&
		config.RootFSDirtyTailRetirementReserveBytes > config.RootFSMaxNodeDirtyTailBytes {
		return fmt.Errorf("rootfs_dirty_tail_retirement_reserve_bytes must not exceed rootfs_max_node_dirty_tail_bytes")
	}
	if config.RootFSConsumerNetNSRoot != "" &&
		(!filepath.IsAbs(config.RootFSConsumerNetNSRoot) || filepath.Clean(config.RootFSConsumerNetNSRoot) == "/") {
		return fmt.Errorf("rootfs_consumer_netns_root must be a non-root absolute path")
	}
	for name, value := range map[string]string{
		"rootfs_state_path":  config.RootFSStatePath,
		"rootfs_branch_root": config.RootFSBranchRoot,
		"rootfs_mount_root":  config.RootFSMountRoot,
	} {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("%s is required when rootfs_enabled is true", name)
		}
		if !filepath.IsAbs(value) || filepath.Clean(value) == "/" {
			return fmt.Errorf("%s must be a non-root absolute path", name)
		}
	}
	if strings.TrimSpace(config.RootFSObjectBucket) == "" {
		return fmt.Errorf("rootfs_object_bucket is required when rootfs_enabled is true")
	}
	if len(config.RootFSNBDDevices) == 0 {
		return fmt.Errorf("at least one rootfs_nbd_device is required")
	}
	if config.RootFSAuthorityURL != "" {
		for name, value := range map[string]string{
			"rootfs_authority_ca_file":          config.RootFSAuthorityCAFile,
			"rootfs_authority_client_cert_file": config.RootFSAuthorityClientCertFile,
			"rootfs_authority_client_key_file":  config.RootFSAuthorityClientKeyFile,
			"rootfs_authority_token_file":       config.RootFSAuthorityTokenFile,
		} {
			if value == "" || !filepath.IsAbs(value) {
				return fmt.Errorf("%s must be an absolute path when rootfs_authority_url is set", name)
			}
		}
	}
	return nil
}
