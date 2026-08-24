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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
)

// RuntimeInfoVersion is the wire version of ctld-owned runtime metadata.
const RuntimeInfoVersion = 1

// RuntimeInfo is the immutable node-runtime configuration that the task
// driver must use instead of duplicating privileged ctld settings in HCL.
type RuntimeInfo struct {
	Version                         int    `json:"version"`
	MountRoot                       string `json:"mount_root"`
	MaxDirtyTailBytes               int64  `json:"max_dirty_tail_bytes"`
	MaxNodeDirtyTailBytes           int64  `json:"max_node_dirty_tail_bytes"`
	DirtyTailRetirementReserveBytes int64  `json:"dirty_tail_retirement_reserve_bytes"`
}

// Validate rejects incomplete, unbounded, or noncanonical runtime metadata.
func (i RuntimeInfo) Validate() error {
	if i.Version != RuntimeInfoVersion {
		return fmt.Errorf("unsupported ctld Nomad runtime info version %d", i.Version)
	}
	if err := validateCanonicalAbsolutePath("mount_root", i.MountRoot); err != nil {
		return err
	}
	if i.MaxDirtyTailBytes <= 0 || i.MaxNodeDirtyTailBytes <= 0 || i.DirtyTailRetirementReserveBytes <= 0 {
		return fmt.Errorf("ctld Nomad runtime dirty-tail limits must be positive")
	}
	if i.MaxDirtyTailBytes > maxDirtyTailLimitBytes || i.MaxNodeDirtyTailBytes > maxDirtyTailLimitBytes ||
		i.DirtyTailRetirementReserveBytes > maxDirtyTailLimitBytes {
		return fmt.Errorf("ctld Nomad runtime dirty-tail limits must not exceed %d bytes", maxDirtyTailLimitBytes)
	}
	if i.DirtyTailRetirementReserveBytes > i.MaxNodeDirtyTailBytes {
		return fmt.Errorf("ctld Nomad runtime dirty-tail retirement reserve exceeds the node limit")
	}
	return nil
}

// Digest binds slot readiness to the exact root-owned runtime metadata.
func (i RuntimeInfo) Digest() (string, error) {
	if err := i.Validate(); err != nil {
		return "", err
	}
	payload, err := json.Marshal(i)
	if err != nil {
		return "", fmt.Errorf("encode ctld Nomad runtime info: %w", err)
	}
	digest := sha256.Sum256(payload)
	return hex.EncodeToString(digest[:]), nil
}

func runtimeInfoFromConfig(config Config) RuntimeInfo {
	config.ApplyDefaults()
	return RuntimeInfo{
		Version: RuntimeInfoVersion, MountRoot: config.RootFSMountRoot,
		MaxDirtyTailBytes:               config.RootFSMaxDirtyTailBytes,
		MaxNodeDirtyTailBytes:           config.RootFSMaxNodeDirtyTailBytes,
		DirtyTailRetirementReserveBytes: config.RootFSDirtyTailRetirementReserveBytes,
	}
}
