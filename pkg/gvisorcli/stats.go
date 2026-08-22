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

package gvisorcli

import (
	"fmt"
	"strings"
)

const (
	maxRunscContainerIDBytes = 512
	maxRunscCPUCount         = 4096
	maxRunscInterfaces       = 1024
	maxRunscInterfaceBytes   = 256
	maxRunscMemoryRawEntries = 1024
	maxRunscMemoryRawKey     = 256
)

// RunscStats mirrors the stable JSON event emitted by stock
// `runsc events --stats`. It intentionally does not import gVisor packages.
type RunscStats struct {
	Type string         `json:"type"`
	ID   string         `json:"id"`
	Data RunscStatsData `json:"data"`
}

type RunscStatsData struct {
	CPU               RunscCPU                 `json:"cpu"`
	Memory            RunscMemory              `json:"memory"`
	Pids              RunscPids                `json:"pids"`
	NetworkInterfaces []*RunscNetworkInterface `json:"network_interfaces"`
}

type RunscCPU struct {
	Usage RunscCPUUsage `json:"usage"`
}

type RunscCPUUsage struct {
	Kernel uint64   `json:"kernel,omitempty"`
	User   uint64   `json:"user,omitempty"`
	Total  uint64   `json:"total,omitempty"`
	PerCPU []uint64 `json:"percpu,omitempty"`
}

type RunscMemory struct {
	Cache     uint64            `json:"cache,omitempty"`
	Usage     RunscMemoryEntry  `json:"usage,omitempty"`
	Swap      RunscMemoryEntry  `json:"swap,omitempty"`
	Kernel    RunscMemoryEntry  `json:"kernel,omitempty"`
	KernelTCP RunscMemoryEntry  `json:"kernelTCP,omitempty"`
	Raw       map[string]uint64 `json:"raw,omitempty"`
}

type RunscMemoryEntry struct {
	Limit   uint64 `json:"limit"`
	Usage   uint64 `json:"usage,omitempty"`
	Max     uint64 `json:"max,omitempty"`
	Failcnt uint64 `json:"failcnt"`
}

type RunscPids struct {
	Current uint64 `json:"current,omitempty"`
	Limit   uint64 `json:"limit,omitempty"`
}

// RunscNetworkInterface uses exported Go field names because stock gVisor
// deliberately defines no JSON tags on this compatibility structure.
type RunscNetworkInterface struct {
	Name      string `json:"Name"`
	RxBytes   uint64 `json:"RxBytes"`
	RxPackets uint64 `json:"RxPackets"`
	RxErrors  uint64 `json:"RxErrors"`
	RxDropped uint64 `json:"RxDropped"`
	TxBytes   uint64 `json:"TxBytes"`
	TxPackets uint64 `json:"TxPackets"`
	TxErrors  uint64 `json:"TxErrors"`
	TxDropped uint64 `json:"TxDropped"`
}

// Validate rejects identity confusion and attacker-controlled cardinality.
func (s RunscStats) Validate(containerID string) error {
	if containerID == "" || strings.TrimSpace(containerID) != containerID || len(containerID) > maxRunscContainerIDBytes {
		return fmt.Errorf("expected container ID must be canonical and contain 1..%d bytes", maxRunscContainerIDBytes)
	}
	if s.Type != "stats" {
		return fmt.Errorf("event type %q is not stats", s.Type)
	}
	if s.ID != containerID {
		return fmt.Errorf("event container %q does not match %q", s.ID, containerID)
	}
	if len(s.Data.CPU.Usage.PerCPU) > maxRunscCPUCount {
		return fmt.Errorf("per-CPU sample has %d entries, maximum is %d", len(s.Data.CPU.Usage.PerCPU), maxRunscCPUCount)
	}
	if len(s.Data.Memory.Raw) > maxRunscMemoryRawEntries {
		return fmt.Errorf("memory raw sample has %d entries, maximum is %d", len(s.Data.Memory.Raw), maxRunscMemoryRawEntries)
	}
	for key := range s.Data.Memory.Raw {
		if key == "" || strings.TrimSpace(key) != key || len(key) > maxRunscMemoryRawKey {
			return fmt.Errorf("memory raw key must be canonical and at most %d bytes", maxRunscMemoryRawKey)
		}
	}
	if len(s.Data.NetworkInterfaces) > maxRunscInterfaces {
		return fmt.Errorf("network sample has %d interfaces, maximum is %d", len(s.Data.NetworkInterfaces), maxRunscInterfaces)
	}
	seen := make(map[string]struct{}, len(s.Data.NetworkInterfaces))
	for _, item := range s.Data.NetworkInterfaces {
		if item == nil {
			return fmt.Errorf("network interface must not be null")
		}
		if item.Name == "" || strings.TrimSpace(item.Name) != item.Name || len(item.Name) > maxRunscInterfaceBytes {
			return fmt.Errorf("network interface name must be canonical and at most %d bytes", maxRunscInterfaceBytes)
		}
		if _, found := seen[item.Name]; found {
			return fmt.Errorf("network interface %q is duplicated", item.Name)
		}
		seen[item.Name] = struct{}{}
	}
	return nil
}
