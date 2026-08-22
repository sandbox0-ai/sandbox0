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
	"fmt"
	"strconv"
	"strings"
)

const (
	RuntimeMetricTargetVersion = 1
	maxRuntimeMetricIDBytes    = 512
	maxRuntimeMetricCPUMilli   = 1_000_000
	maxRuntimeMetricMemoryMiB  = 1 << 30
)

// RuntimeMetricTarget is the non-secret logical and runtime identity returned
// by the root-owned node runtime. It deliberately contains no journal path,
// mount path, network namespace path, or writer credential.
type RuntimeMetricTarget struct {
	Version           int    `json:"version"`
	TeamID            string `json:"team_id"`
	SandboxID         string `json:"sandbox_id"`
	RuntimeGeneration int64  `json:"runtime_generation"`
	CPUMillicpu       int64  `json:"cpu_millicpu"`
	MemoryMiB         int64  `json:"memory_mib"`
	AllocationID      string `json:"allocation_id"`
	NodeBootID        string `json:"node_boot_id"`
	LaunchAttempt     string `json:"launch_attempt"`
	RunscContainerID  string `json:"runsc_container_id"`
	BindingDigest     string `json:"binding_digest"`
	SeriesEpoch       string `json:"series_epoch"`
}

// Validate rejects ambiguous, unbounded, or self-inconsistent metric targets.
func (t RuntimeMetricTarget) Validate() error {
	if t.Version != RuntimeMetricTargetVersion {
		return fmt.Errorf("unsupported runtime metric target version %d", t.Version)
	}
	for name, value := range map[string]string{
		"team_id": t.TeamID, "sandbox_id": t.SandboxID, "allocation_id": t.AllocationID,
		"node_boot_id": t.NodeBootID, "launch_attempt": t.LaunchAttempt,
		"runsc_container_id": t.RunscContainerID,
	} {
		if value == "" || strings.TrimSpace(value) != value || len(value) > maxRuntimeMetricIDBytes {
			return fmt.Errorf("%s must be canonical and contain 1..%d bytes", name, maxRuntimeMetricIDBytes)
		}
	}
	if t.RuntimeGeneration <= 0 {
		return fmt.Errorf("runtime_generation must be positive")
	}
	if t.CPUMillicpu <= 0 || t.CPUMillicpu > maxRuntimeMetricCPUMilli {
		return fmt.Errorf("cpu_millicpu must be within 1..%d", maxRuntimeMetricCPUMilli)
	}
	if t.MemoryMiB <= 0 || t.MemoryMiB > maxRuntimeMetricMemoryMiB {
		return fmt.Errorf("memory_mib must be within 1..%d", maxRuntimeMetricMemoryMiB)
	}
	if err := validateRuntimeMetricDigest("binding_digest", t.BindingDigest); err != nil {
		return err
	}
	expected := RuntimeMetricSeriesEpoch(t.AllocationID, t.NodeBootID, t.LaunchAttempt, t.RunscContainerID)
	if t.SeriesEpoch != expected {
		return fmt.Errorf("series_epoch does not match the runtime incarnation")
	}
	return nil
}

// RuntimeMetricSeriesEpoch derives the public counter-reset boundary from the
// exact allocation, node boot, launch attempt, and runsc container identity.
func RuntimeMetricSeriesEpoch(allocationID, nodeBootID, launchAttempt, runscContainerID string) string {
	hash := sha256.New()
	for _, value := range []string{allocationID, nodeBootID, launchAttempt, runscContainerID} {
		_, _ = hash.Write([]byte(strconv.Itoa(len(value))))
		_, _ = hash.Write([]byte{':'})
		_, _ = hash.Write([]byte(value))
	}
	return "runsc:" + hex.EncodeToString(hash.Sum(nil))
}

func validateRuntimeMetricDigest(name, value string) error {
	decoded, err := hex.DecodeString(value)
	if err != nil || len(decoded) != sha256.Size || hex.EncodeToString(decoded) != value {
		return fmt.Errorf("%s must be a canonical 32-byte hexadecimal digest", name)
	}
	return nil
}
