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
	"errors"
	"fmt"
	"path/filepath"
	"slices"

	"github.com/hashicorp/nomad/plugins/drivers"
)

// recoveredDriverState validates only persisted, versioned configuration. Nomad
// does not persist TaskConfig.rawDriverConfig; decoding or defaulting that field
// here would make recovery depend on whether the handle crossed a client restart.
func recoveredDriverState(config *PluginConfig, handle *drivers.TaskHandle) (PersistedState, error) {
	var state PersistedState
	if handle.Version != 2 && handle.Version != taskHandleVersion {
		return state, fmt.Errorf("unsupported persisted driver handle version %d", handle.Version)
	}
	if err := handle.GetDriverState(&state); err != nil {
		return state, fmt.Errorf("decode persisted driver state: %w", err)
	}
	if state.DriverConfig == nil {
		return state, errors.New("persisted driver configuration is missing")
	}
	// StartTask already normalized these inputs. Recovery must reject an absent
	// command/security class instead of silently choosing a different contract.
	if err := validateRuntimeSlotTaskConfig(config, *state.DriverConfig); err != nil {
		return state, fmt.Errorf("validate persisted driver configuration: %w", err)
	}
	if !sameRecoveryTaskIdentity(state.TaskConfig, handle.Config) {
		return state, errors.New("persisted task identity does not match the Nomad task handle")
	}
	bundle := driverBundleDir(handle.Config)
	if handle.Version == 2 {
		bundle = filepath.Join(handle.Config.TaskDir().Dir, "gvisor-bundle")
	}
	if !filepath.IsAbs(bundle) || state.BundleDir != bundle ||
		state.RootMount != filepath.Join(bundle, "rootfs") || state.ContainerID != safeContainerID(handle.Config.ID) {
		return state, errors.New("persisted runtime paths do not match the Nomad task handle")
	}
	return state, nil
}

// Newer local lifecycle state may update phase/claim, but cannot replace the
// immutable identity or security inputs validated from the recovery handle.
func sameRecoveryIdentity(a, b PersistedState) bool {
	return sameRecoveryTaskIdentity(a.TaskConfig, b.TaskConfig) &&
		a.ContainerID == b.ContainerID && a.BundleDir == b.BundleDir && a.RootMount == b.RootMount &&
		a.DriverConfig != nil && b.DriverConfig != nil &&
		a.DriverConfig.Command == b.DriverConfig.Command && a.DriverConfig.SecurityClass == b.DriverConfig.SecurityClass &&
		slices.Equal(a.DriverConfig.Args, b.DriverConfig.Args)
}

func sameRecoveryTaskIdentity(a, b *drivers.TaskConfig) bool {
	if a == nil || b == nil || a.ID == "" || a.AllocID == "" || a.NodeID == "" || a.Namespace == "" || a.Name == "" ||
		a.ID != b.ID || a.AllocID != b.AllocID || a.NodeID != b.NodeID || a.Namespace != b.Namespace ||
		a.Name != b.Name || a.AllocDir != b.AllocDir {
		return false
	}
	return a.NetworkIsolation != nil && b.NetworkIsolation != nil &&
		a.NetworkIsolation.Mode == b.NetworkIsolation.Mode && a.NetworkIsolation.Path != "" &&
		a.NetworkIsolation.Path == b.NetworkIsolation.Path
}
