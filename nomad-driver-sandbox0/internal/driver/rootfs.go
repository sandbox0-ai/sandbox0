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
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/objectstore"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	rootfssession "github.com/sandbox0-ai/sandbox0/pkg/rootfssession"
)

// rootfsRuntime owns the node-local NBD/XFS/Overlay session for this PoC.
// A production deployment moves this ownership to ctld and gives the task
// driver only an already-authorized mount.
type rootfsRuntime struct {
	sessions *rootfssession.Manager
}

// RootFSRuntime is the driver-facing RootFS attachment and retire boundary.
type RootFSRuntime interface {
	Ensure(context.Context, rootfshandoff.StageRequest) (rootfssession.Mount, error)
	Retire(context.Context, rootfshandoff.StageRequest, string) error
}

func newRootFSRuntime(config *PluginConfig) (*rootfsRuntime, error) {
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
	return &rootfsRuntime{sessions: sessions}, nil
}

func (r *rootfsRuntime) Ensure(ctx context.Context, request rootfshandoff.StageRequest) (rootfssession.Mount, error) {
	if err := request.Validate(); err != nil {
		return rootfssession.Mount{}, err
	}
	return r.sessions.Ensure(ctx, request)
}

func (r *rootfsRuntime) Retire(ctx context.Context, request rootfshandoff.StageRequest, operationID string) error {
	if strings.TrimSpace(operationID) == "" {
		return fmt.Errorf("retire operation ID is required")
	}
	if err := r.sessions.BeginRetire(request.Parent, request.Identity, operationID); err != nil {
		return err
	}
	if err := r.sessions.Release(ctx, request.Identity); err != nil {
		return err
	}
	result, err := r.sessions.RetireResult(request.Parent, request.Identity, operationID)
	if err != nil {
		return err
	}
	if result.DurabilityState == "" || result.Descriptor == nil {
		return fmt.Errorf("RootFS retire result is not durable")
	}
	return nil
}

func (r *rootfsRuntime) Close() error {
	if r == nil || r.sessions == nil {
		return nil
	}
	return r.sessions.Close()
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
	return nil
}

func newRetireOperationID() string {
	return fmt.Sprintf("nomad-retire-%d", time.Now().UnixNano())
}
