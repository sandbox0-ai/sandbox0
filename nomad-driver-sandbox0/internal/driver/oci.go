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
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	specs "github.com/opencontainers/runtime-spec/specs-go"
)

type specOptions struct {
	Command   string
	Args      []string
	AllocID   string
	TaskID    string
	NetNSPath string
	Resources *driversResources
}

// driversResources avoids exporting Nomad types through the OCI helper API.
type driversResources struct {
	CPUPeriod        int64
	CPUQuota         int64
	CPUShares        int64
	MemoryLimitBytes int64
}

func buildSpec(options specOptions) specs.Spec {
	process := specs.Process{
		User: specs.User{UID: 0, GID: 0},
		Args: append([]string{options.Command}, options.Args...),
		Env: []string{
			"PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
			"TERM=xterm",
		},
		Cwd: "/",
		Capabilities: &specs.LinuxCapabilities{
			Bounding:    []string{"CAP_AUDIT_WRITE", "CAP_KILL", "CAP_NET_BIND_SERVICE"},
			Effective:   []string{"CAP_AUDIT_WRITE", "CAP_KILL", "CAP_NET_BIND_SERVICE"},
			Inheritable: []string{},
			Permitted:   []string{"CAP_AUDIT_WRITE", "CAP_KILL", "CAP_NET_BIND_SERVICE"},
			Ambient:     []string{},
		},
		Rlimits: []specs.POSIXRlimit{{Type: "RLIMIT_NOFILE", Hard: 1024, Soft: 1024}},
	}

	namespaces := []specs.LinuxNamespace{
		{Type: specs.PIDNamespace},
		{Type: specs.NetworkNamespace, Path: options.NetNSPath},
		{Type: specs.IPCNamespace},
		{Type: specs.UTSNamespace},
		{Type: specs.MountNamespace},
	}

	linux := &specs.Linux{Namespaces: namespaces}
	if options.Resources != nil {
		resources := &specs.LinuxResources{}
		if options.Resources.MemoryLimitBytes > 0 {
			limit := options.Resources.MemoryLimitBytes
			resources.Memory = &specs.LinuxMemory{Limit: &limit}
		}
		if options.Resources.CPUPeriod > 0 || options.Resources.CPUQuota > 0 || options.Resources.CPUShares > 0 {
			resources.CPU = &specs.LinuxCPU{}
			if options.Resources.CPUPeriod > 0 {
				period := uint64(options.Resources.CPUPeriod)
				resources.CPU.Period = &period
			}
			if options.Resources.CPUQuota > 0 {
				quota := options.Resources.CPUQuota
				resources.CPU.Quota = &quota
			}
			if options.Resources.CPUShares > 0 {
				shares := uint64(options.Resources.CPUShares)
				resources.CPU.Shares = &shares
			}
		}
		linux.Resources = resources
	}

	return specs.Spec{
		Version:  specs.Version,
		Process:  &process,
		Root:     &specs.Root{Path: "rootfs", Readonly: false},
		Hostname: "sandbox0",
		Mounts: []specs.Mount{
			{Destination: "/proc", Type: "proc", Source: "proc"},
			{Destination: "/dev", Type: "tmpfs", Source: "tmpfs"},
			{Destination: "/sys", Type: "sysfs", Source: "sysfs", Options: []string{"nosuid", "noexec", "nodev", "ro"}},
		},
		Linux: linux,
		Annotations: map[string]string{
			"com.sandbox0.alloc-id":   options.AllocID,
			"com.sandbox0.task-id":    options.TaskID,
			"com.sandbox0.slot-state": "created",
		},
	}
}

func writeBundle(bundleDir string, spec specs.Spec) error {
	if err := os.MkdirAll(filepath.Join(bundleDir, "rootfs"), 0o755); err != nil {
		return fmt.Errorf("create OCI rootfs mountpoint: %w", err)
	}
	data, err := json.MarshalIndent(spec, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal OCI spec: %w", err)
	}
	configPath := filepath.Join(bundleDir, "config.json")
	if err := os.WriteFile(configPath, data, 0o644); err != nil {
		return fmt.Errorf("write OCI spec: %w", err)
	}
	return nil
}
