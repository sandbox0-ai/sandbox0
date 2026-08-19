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
	"encoding/hex"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/containerd/errdefs"
	"github.com/hashicorp/go-hclog"
	managerauthority "github.com/sandbox0-ai/sandbox0/manager/pkg/rootfswriterauthority"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/pkg/objectstore"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	rootfssession "github.com/sandbox0-ai/sandbox0/pkg/rootfssession"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/rootfswriterauthority"
)

// rootfsRuntime owns the node-local NBD/XFS/Overlay session for this PoC.
// A production deployment moves this ownership to ctld and gives the task
// driver only an already-authorized mount.
type rootfsRuntime struct {
	sessions  *rootfssession.Manager
	authority *managerauthority.ManagerClient
	logger    hclog.Logger
	renewalMu sync.Mutex
	renewals  map[string]*rootfsRenewal
}

// RootFSRuntime is the driver-facing RootFS attachment and retire boundary.
type RootFSRuntime interface {
	Ensure(context.Context, rootfshandoff.StageRequest) (rootfssession.Mount, error)
	Retire(context.Context, rootfshandoff.StageRequest, string) (rootfssession.RetireResult, error)
	CrashFence(context.Context, rootfshandoff.StageRequest, string, crashTaskObservation) (rootfshandoff.CrashFenceProof, error)
}

type crashTaskObservation struct {
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

type consumedRootFSAttachError struct {
	err error
}

func (e *consumedRootFSAttachError) Error() string { return e.err.Error() }
func (e *consumedRootFSAttachError) Unwrap() error { return e.err }

func newRootFSRuntime(config *PluginConfig, logger hclog.Logger) (*rootfsRuntime, error) {
	if config == nil || !config.RootFSEnabled {
		return nil, nil
	}
	store, err := objectstore.Create(objectstore.Config{
		Type: config.RootFSObjectType, Bucket: config.RootFSObjectBucket,
		Region: config.RootFSObjectRegion, Endpoint: config.RootFSObjectEndpoint,
		AccessKey: config.RootFSObjectAccessKey, SecretKey: config.RootFSObjectSecretKey,
	})
	if err != nil {
		return nil, fmt.Errorf("create RootFS object store: %w", err)
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
		MountRoot: config.RootFSMountRoot, Source: conditional,
		Publisher: rootfsblock.ObjectStorePublisher{Store: conditional}, Runtime: hostRuntime,
	})
	if err != nil {
		return nil, fmt.Errorf("create RootFS session manager: %w", err)
	}
	reconcileCtx, reconcileCancel := context.WithTimeout(context.Background(), 30*time.Second)
	err = sessions.ReconcileReleases(reconcileCtx)
	reconcileCancel()
	if err != nil {
		_ = sessions.Close()
		return nil, fmt.Errorf("reconcile RootFS session journal: %w", err)
	}
	var authority *managerauthority.ManagerClient
	if config.RootFSAuthorityURL != "" {
		authority, err = managerauthority.NewManagerClient(managerauthority.ManagerClientConfig{
			BaseURL: config.RootFSAuthorityURL, CAFile: config.RootFSAuthorityCAFile,
			ClientCertFile: config.RootFSAuthorityClientCertFile,
			ClientKeyFile:  config.RootFSAuthorityClientKeyFile,
			TokenFile:      config.RootFSAuthorityTokenFile, Timeout: 2 * time.Second,
		})
		if err != nil {
			_ = sessions.Close()
			return nil, fmt.Errorf("create RootFS writer authority client: %w", err)
		}
	}
	return &rootfsRuntime{
		sessions: sessions, authority: authority, logger: logger,
		renewals: make(map[string]*rootfsRenewal),
	}, nil
}

func (r *rootfsRuntime) Ensure(ctx context.Context, request rootfshandoff.StageRequest) (rootfssession.Mount, error) {
	if err := request.Validate(); err != nil {
		return rootfssession.Mount{}, err
	}
	if r.authority != nil {
		observation, err := r.authority.ConsumeWriterGrant(ctx, request)
		if err != nil {
			return rootfssession.Mount{}, fmt.Errorf("consume regional writer grant: %w", err)
		}
		r.startRenewal(request, observation)
	}
	mount, err := r.sessions.Ensure(ctx, request)
	if err != nil && r.authority != nil {
		return rootfssession.Mount{}, &consumedRootFSAttachError{err: err}
	}
	return mount, err
}

func (r *rootfsRuntime) Retire(ctx context.Context, request rootfshandoff.StageRequest, operationID string) (rootfssession.RetireResult, error) {
	if strings.TrimSpace(operationID) == "" {
		return rootfssession.RetireResult{}, fmt.Errorf("retire operation ID is required")
	}
	r.stopRenewal(request.Parent)
	if err := r.sessions.BeginRetire(request.Parent, request.Identity, operationID); err != nil {
		return rootfssession.RetireResult{}, err
	}
	if err := r.sessions.Release(ctx, request.Identity); err != nil {
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
	}
	return result, nil
}

// CrashFence abandons an unsealed writer only after the regional lease fence
// is authoritative and the node has proved every local runtime owner absent.
func (r *rootfsRuntime) CrashFence(
	ctx context.Context,
	request rootfshandoff.StageRequest,
	operationID string,
	task crashTaskObservation,
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
	if err := r.awaitRegionalCrashFence(ctx, request, operationID); err != nil {
		return rootfshandoff.CrashFenceProof{}, err
	}
	if err := r.sessions.Release(ctx, request.Identity); err != nil {
		return rootfshandoff.CrashFenceProof{}, fmt.Errorf("release crashed RootFS session: %w", err)
	}
	session, err := r.sessions.CrashFence(request, operationID)
	if err != nil {
		return rootfshandoff.CrashFenceProof{}, fmt.Errorf("attest crashed RootFS session: %w", err)
	}
	binding, err := request.BindingDigest()
	if err != nil {
		return rootfshandoff.CrashFenceProof{}, err
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
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
		SnapshotterState: rootfshandoff.StateTombstoned, Session: session, ObservedAt: now,
	}
	if err := proof.Validate(); err != nil {
		return rootfshandoff.CrashFenceProof{}, fmt.Errorf("validate Nomad crash fence proof: %w", err)
	}
	if err := r.authority.CompleteCrashAbandonWriterGrant(ctx, request, operationID, proof); err != nil {
		return rootfshandoff.CrashFenceProof{}, fmt.Errorf("complete regional writer crash abandon: %w", err)
	}
	return proof, nil
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

func (r *rootfsRuntime) startRenewal(request rootfshandoff.StageRequest, observation protocol.LeaseObservation) {
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
		for {
			delay := time.Until(observation.RenewAfter)
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
			next, err := r.authority.RenewWriterGrant(ctx, request)
			if err != nil {
				if r.logger != nil {
					r.logger.Error("RootFS writer lease renewal failed", "parent", request.Parent, "error", err)
				}
				if ctx.Err() != nil {
					return
				}
				time.Sleep(time.Second)
				continue
			}
			observation = next
		}
	}()
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

func validateRootFSConfig(config *PluginConfig) error {
	if config == nil || !config.RootFSEnabled {
		return nil
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

func newRetireOperationID() string {
	return fmt.Sprintf("nomad-retire-%d", time.Now().UnixNano())
}
