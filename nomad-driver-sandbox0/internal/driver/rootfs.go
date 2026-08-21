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
	"fmt"
	"path/filepath"
	"strings"

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
	if config == nil || !config.RootFSEnabled {
		return nil, nil
	}
	return nomadruntime.NewClient(config.RootFSNodeSocket)
}

func newNodeRuntimeClient(socketPath string) (*nomadruntime.Client, error) {
	return nomadruntime.NewClient(socketPath)
}

func validateRootFSConfig(config *PluginConfig) error {
	if config == nil || !config.RootFSEnabled {
		return nil
	}
	for name, value := range map[string]string{
		"rootfs_node_socket": config.RootFSNodeSocket,
		"rootfs_mount_root":  config.RootFSMountRoot,
	} {
		value = strings.TrimSpace(value)
		if !filepath.IsAbs(value) || filepath.Clean(value) == string(filepath.Separator) {
			return fmt.Errorf("%s must be a non-root absolute path", name)
		}
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
	return nil
}
