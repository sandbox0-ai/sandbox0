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

	"github.com/hashicorp/go-hclog"
	"github.com/sandbox0-ai/sandbox0/pkg/nomadruntime"
)

type RootFSRuntime = nomadruntime.Runtime
type RootFSConsumerRequest = nomadruntime.ConsumerRequest
type RootFSConsumerLease = nomadruntime.ConsumerLease
type crashTaskObservation = nomadruntime.CrashTaskObservation
type consumedRootFSAttachError = nomadruntime.ConsumedAttachError

type runtimeSlotJournalRegistration = nomadruntime.RuntimeSlotRegistration

const runtimeSlotJournalVersion = nomadruntime.RuntimeSlotJournalVersion

func newRootFSRuntime(config *PluginConfig, _ hclog.Logger) (RootFSRuntime, error) {
	if config == nil {
		return nil, fmt.Errorf("plugin config is required")
	}
	return nomadruntime.NewClient(config.RootFSNodeSocket)
}

func newNodeRuntimeClient(socketPath string) (*nomadruntime.Client, error) {
	return nomadruntime.NewClient(socketPath)
}

func validateRootFSConfig(config *PluginConfig) error {
	if config == nil {
		return fmt.Errorf("plugin config is required")
	}
	for name, value := range map[string]string{
		"rootfs_node_socket": config.RootFSNodeSocket,
	} {
		value = strings.TrimSpace(value)
		if !filepath.IsAbs(value) || filepath.Clean(value) == string(filepath.Separator) {
			return fmt.Errorf("%s must be a non-root absolute path", name)
		}
	}
	return nil
}

type rootFSRuntimeInfoProvider interface {
	RuntimeInfo(context.Context) (nomadruntime.RuntimeInfo, error)
}

func loadRootFSRuntimeInfo(ctx context.Context, runtime RootFSRuntime) (nomadruntime.RuntimeInfo, error) {
	provider, ok := runtime.(rootFSRuntimeInfoProvider)
	if !ok {
		return nomadruntime.RuntimeInfo{}, fmt.Errorf("RootFS runtime cannot report ctld-owned metadata")
	}
	requestCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	info, err := provider.RuntimeInfo(requestCtx)
	if err != nil {
		return nomadruntime.RuntimeInfo{}, fmt.Errorf("read ctld Nomad runtime info: %w", err)
	}
	if err := info.Validate(); err != nil {
		return nomadruntime.RuntimeInfo{}, fmt.Errorf("validate ctld Nomad runtime info: %w", err)
	}
	return info, nil
}
