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
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
	"github.com/sandbox0-ai/sandbox0/pkg/sandboxspec"
)

type specOptions struct {
	Command                       string
	Args                          []string
	Env                           []string
	AllocID                       string
	TaskID                        string
	NetNSPath                     string
	ResolvConfPath                string
	ProcdInternalJWTPublicKeyFile string
	Resources                     *driversResources
	SecurityClass                 string
	EphemeralMounts               []runtimecontrol.EphemeralMount
}

const procdInternalJWTPublicKeyDestination = "/config/internal_jwt_public.key"

// driversResources avoids exporting Nomad types through the OCI helper API.
type driversResources struct {
	CPUPeriod        int64
	CPUQuota         int64
	CPUShares        int64
	CPUSetCpus       string
	MemoryLimitBytes int64
	PIDsLimit        int64
	CgroupPath       string
}

func buildSpec(options specOptions) specs.Spec {
	capabilities := standardCapabilities
	if options.SecurityClass == string(sandboxspec.SandboxSecurityClassPrivileged) {
		capabilities = privilegedCapabilities
	}
	process := specs.Process{
		User: specs.User{UID: 0, GID: 0},
		Args: append([]string{options.Command}, options.Args...),
		Env: []string{
			"PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
			"TERM=xterm",
		},
		Cwd: "/",
		Capabilities: &specs.LinuxCapabilities{
			Bounding:    append([]string(nil), capabilities...),
			Effective:   append([]string(nil), capabilities...),
			Inheritable: []string{},
			Permitted:   append([]string(nil), capabilities...),
			Ambient:     []string{},
		},
		Rlimits: []specs.POSIXRlimit{{Type: "RLIMIT_NOFILE", Hard: 1024, Soft: 1024}},
	}
	process.Env = append(process.Env, options.Env...)

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
			// OCI swap is the combined memory+swap ceiling. Equal limits convert
			// to memory.swap.max=0 on cgroup v2 and prevent unleased swap usage.
			resources.Memory = &specs.LinuxMemory{Limit: &limit, Swap: &limit}
		}
		if options.Resources.CPUPeriod > 0 || options.Resources.CPUQuota > 0 || options.Resources.CPUShares > 0 || options.Resources.CPUSetCpus != "" {
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
			resources.CPU.Cpus = options.Resources.CPUSetCpus
		}
		if options.Resources.PIDsLimit > 0 {
			limit := options.Resources.PIDsLimit
			resources.Pids = &specs.LinuxPids{Limit: &limit}
		}
		linux.Resources = resources
		linux.CgroupsPath = options.Resources.CgroupPath
	}

	mounts := []specs.Mount{
		{Destination: "/proc", Type: "proc", Source: "proc"},
		{Destination: "/dev", Type: "tmpfs", Source: "tmpfs", Options: []string{"nosuid", "strictatime", "mode=755", "size=65536k"}},
		{Destination: "/dev/pts", Type: "devpts", Source: "devpts", Options: []string{"nosuid", "noexec", "newinstance", "ptmxmode=0666", "mode=0620"}},
		{Destination: "/dev/shm", Type: "tmpfs", Source: "shm", Options: []string{"nosuid", "noexec", "nodev", "mode=1777", "size=67108864"}},
		{Destination: "/dev/mqueue", Type: "mqueue", Source: "mqueue", Options: []string{"nosuid", "noexec", "nodev"}},
		{Destination: "/sys", Type: "sysfs", Source: "sysfs", Options: []string{"nosuid", "noexec", "nodev", "ro"}},
	}
	if options.ResolvConfPath != "" {
		mounts = append(mounts, specs.Mount{
			Destination: "/etc/resolv.conf",
			Type:        "bind",
			Source:      options.ResolvConfPath,
			Options:     []string{"rbind", "ro", "nosuid", "nodev", "noexec"},
		})
	}
	for _, mount := range options.EphemeralMounts {
		ephemeral := specs.Mount{
			Destination: mount.MountPath, Type: "tmpfs", Source: "tmpfs",
			Options: []string{"nosuid", "nodev", "mode=1777", fmt.Sprintf("size=%d", mount.SizeBytes)},
		}
		if mount.MountPath == "/dev/shm" {
			ephemeral.Source = "shm"
			ephemeral.Options = append(ephemeral.Options, "noexec")
			for index := range mounts {
				if mounts[index].Destination == mount.MountPath {
					mounts[index] = ephemeral
				}
			}
			continue
		}
		mounts = append(mounts, ephemeral)
	}
	if options.ProcdInternalJWTPublicKeyFile != "" {
		mounts = append(mounts, specs.Mount{
			Destination: procdInternalJWTPublicKeyDestination,
			Type:        "bind",
			Source:      options.ProcdInternalJWTPublicKeyFile,
			Options:     []string{"rbind", "ro", "nosuid", "nodev", "noexec"},
		})
	}

	return specs.Spec{
		Version:  specs.Version,
		Process:  &process,
		Root:     &specs.Root{Path: "rootfs", Readonly: false},
		Hostname: "sandbox0",
		Mounts:   mounts,
		Linux:    linux,
		Annotations: map[string]string{
			"com.sandbox0.alloc-id":   options.AllocID,
			"com.sandbox0.task-id":    options.TaskID,
			"com.sandbox0.slot-state": "created",
		},
	}
}

var standardCapabilities = []string{
	"CAP_AUDIT_WRITE", "CAP_CHOWN", "CAP_DAC_OVERRIDE", "CAP_FOWNER", "CAP_FSETID",
	"CAP_KILL", "CAP_MKNOD", "CAP_NET_BIND_SERVICE", "CAP_NET_RAW", "CAP_SETFCAP",
	"CAP_SETGID", "CAP_SETPCAP", "CAP_SETUID", "CAP_SYS_CHROOT",
}

// privilegedCapabilities are guest-kernel capabilities. runsc still keeps
// the process inside the gVisor sandbox and never grants host-kernel access.
var privilegedCapabilities = []string{
	"CAP_AUDIT_CONTROL", "CAP_AUDIT_READ", "CAP_AUDIT_WRITE", "CAP_BLOCK_SUSPEND",
	"CAP_BPF", "CAP_CHECKPOINT_RESTORE", "CAP_CHOWN", "CAP_DAC_OVERRIDE",
	"CAP_DAC_READ_SEARCH", "CAP_FOWNER", "CAP_FSETID", "CAP_IPC_LOCK", "CAP_IPC_OWNER",
	"CAP_KILL", "CAP_LEASE", "CAP_LINUX_IMMUTABLE", "CAP_MAC_ADMIN", "CAP_MAC_OVERRIDE",
	"CAP_MKNOD", "CAP_NET_ADMIN", "CAP_NET_BIND_SERVICE", "CAP_NET_BROADCAST", "CAP_NET_RAW",
	"CAP_PERFMON", "CAP_SETFCAP", "CAP_SETGID", "CAP_SETPCAP", "CAP_SETUID", "CAP_SYS_ADMIN",
	"CAP_SYS_BOOT", "CAP_SYS_CHROOT", "CAP_SYS_MODULE", "CAP_SYS_NICE", "CAP_SYS_PACCT",
	"CAP_SYS_PTRACE", "CAP_SYS_RAWIO", "CAP_SYS_RESOURCE", "CAP_SYS_TIME", "CAP_SYS_TTY_CONFIG",
	"CAP_SYSLOG", "CAP_WAKE_ALARM",
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
