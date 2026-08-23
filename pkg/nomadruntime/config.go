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

// Package nomadruntime owns the privileged Nomad node runtime hosted by ctld.
package nomadruntime

import (
	"fmt"
	"net/url"
	"path/filepath"
	"strings"
	"unicode"

	rootfssession "github.com/sandbox0-ai/sandbox0/pkg/rootfssession"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"go.uber.org/zap"
)

// Config contains the ctld-owned Nomad runtime configuration.
type Config struct {
	SocketPath string

	RunscPath                 string
	RunscRoot                 string
	Platform                  string
	Overlay2                  string
	FileAccess                string
	DirectFS                  bool
	RuntimeResourceCgroupRoot string

	RootFSStatePath                       string
	RootFSBranchRoot                      string
	RootFSMountRoot                       string
	RootFSConsumerMountRoot               string
	RootFSConsumerNetNSRoot               string
	RootFSMaxDirtyTailBytes               int64
	RootFSMaxNodeDirtyTailBytes           int64
	RootFSDirtyTailRetirementReserveBytes int64
	RootFSNBDDevices                      []string

	RootFSObjectType                 string
	RootFSObjectBucket               string
	RootFSObjectRegion               string
	RootFSObjectEndpoint             string
	RootFSObjectAccessKey            string
	RootFSObjectSecretKey            string
	RootFSObjectSessionToken         string
	RootFSObjectEncryptionEnabled    bool
	RootFSObjectEncryptionKeyPath    string
	RootFSObjectEncryptionPassphrase string
	RootFSObjectEncryptionAlgorithm  string

	RootFSAuthorityURL            string
	RootFSAuthorityCAFile         string
	RootFSAuthorityClientCertFile string
	RootFSAuthorityClientKeyFile  string
	RootFSAuthorityTokenFile      string

	RuntimeSlotNodeBootIDFile string
	RuntimeSlotJournalPath    string
}

const maxDirtyTailLimitBytes = int64(1 << 50)

// ApplyDefaults fills production-safe node-local defaults.
func (c *Config) ApplyDefaults() {
	if c == nil {
		return
	}
	if strings.TrimSpace(c.SocketPath) == "" {
		c.SocketPath = "/run/sandbox0/ctld-nomad-runtime.sock"
	}
	if strings.TrimSpace(c.RunscPath) == "" {
		c.RunscPath = "/usr/local/bin/runsc"
	}
	if strings.TrimSpace(c.RunscRoot) == "" {
		c.RunscRoot = "/run/sandbox0/runsc"
	}
	if strings.TrimSpace(c.Platform) == "" {
		c.Platform = "systrap"
	}
	if strings.TrimSpace(c.Overlay2) == "" {
		c.Overlay2 = "none"
	}
	if strings.TrimSpace(c.FileAccess) == "" {
		c.FileAccess = "shared"
	}
	if strings.TrimSpace(c.RuntimeResourceCgroupRoot) == "" {
		c.RuntimeResourceCgroupRoot = protocol.RuntimeResourceCgroupRoot
	}
	if strings.TrimSpace(c.RootFSStatePath) == "" {
		c.RootFSStatePath = "/var/lib/sandbox0/ctld/nomad/rootfs-sessions.db"
	}
	if strings.TrimSpace(c.RootFSBranchRoot) == "" {
		c.RootFSBranchRoot = "/var/lib/sandbox0/ctld/nomad/rootfs-branches"
	}
	if strings.TrimSpace(c.RootFSMountRoot) == "" {
		c.RootFSMountRoot = "/run/sandbox0/rootfs"
	}
	if strings.TrimSpace(c.RootFSConsumerMountRoot) == "" {
		c.RootFSConsumerMountRoot = "/opt/nomad"
	}
	if strings.TrimSpace(c.RootFSConsumerNetNSRoot) == "" {
		c.RootFSConsumerNetNSRoot = "/run/netns"
	}
	if c.RootFSMaxDirtyTailBytes == 0 {
		c.RootFSMaxDirtyTailBytes = rootfssession.DefaultMaxDirtyTailBytes
	}
	if c.RootFSMaxNodeDirtyTailBytes == 0 {
		c.RootFSMaxNodeDirtyTailBytes = rootfssession.DefaultMaxNodeDirtyTailBytes
	}
	if c.RootFSDirtyTailRetirementReserveBytes == 0 {
		c.RootFSDirtyTailRetirementReserveBytes = rootfssession.DefaultDirtyTailRetirementReserveBytes
	}
	if strings.TrimSpace(c.RootFSObjectType) == "" {
		c.RootFSObjectType = "s3"
	}
	if strings.TrimSpace(c.RootFSObjectRegion) == "" {
		c.RootFSObjectRegion = "us-east-1"
	}
	if strings.TrimSpace(c.RuntimeSlotNodeBootIDFile) == "" {
		c.RuntimeSlotNodeBootIDFile = "/proc/sys/kernel/random/boot_id"
	}
	if strings.TrimSpace(c.RuntimeSlotJournalPath) == "" {
		c.RuntimeSlotJournalPath = "/var/lib/sandbox0/ctld/nomad/runtime-slots.db"
	}
}

// Validate checks configuration that can be verified before ctld acquires
// host runtime resources.
func (c Config) Validate() error {
	c.ApplyDefaults()
	if strings.TrimSpace(c.RootFSObjectBucket) == "" {
		return fmt.Errorf("rootfs_object_bucket is required")
	}
	if err := validateCanonicalHTTPSOrigin("rootfs_authority_url", c.RootFSAuthorityURL); err != nil {
		return err
	}
	paths := []struct{ name, value string }{
		{"socket_path", c.SocketPath},
		{"runsc_path", c.RunscPath},
		{"runsc_root", c.RunscRoot},
		{"runtime_resource_cgroup_root", c.RuntimeResourceCgroupRoot},
		{"rootfs_state_path", c.RootFSStatePath},
		{"rootfs_branch_root", c.RootFSBranchRoot},
		{"rootfs_mount_root", c.RootFSMountRoot},
		{"rootfs_consumer_mount_root", c.RootFSConsumerMountRoot},
		{"rootfs_consumer_netns_root", c.RootFSConsumerNetNSRoot},
		{"rootfs_authority_ca_file", c.RootFSAuthorityCAFile},
		{"rootfs_authority_client_cert_file", c.RootFSAuthorityClientCertFile},
		{"rootfs_authority_client_key_file", c.RootFSAuthorityClientKeyFile},
		{"rootfs_authority_token_file", c.RootFSAuthorityTokenFile},
		{"runtime_slot_node_boot_id_file", c.RuntimeSlotNodeBootIDFile},
		{"runtime_slot_journal_path", c.RuntimeSlotJournalPath},
	}
	if c.RootFSObjectEncryptionEnabled {
		paths = append(paths, struct{ name, value string }{
			"rootfs_object_encryption_key_path", c.RootFSObjectEncryptionKeyPath,
		})
	}
	for _, path := range paths {
		if err := validateCanonicalAbsolutePath(path.name, path.value); err != nil {
			return err
		}
	}
	if c.RuntimeResourceCgroupRoot != protocol.RuntimeResourceCgroupRoot {
		return fmt.Errorf("runtime_resource_cgroup_root must be %s", protocol.RuntimeResourceCgroupRoot)
	}
	if len(c.RootFSNBDDevices) == 0 {
		return fmt.Errorf("at least one RootFS NBD device is required")
	}
	seenDevices := make(map[string]struct{}, len(c.RootFSNBDDevices))
	for _, device := range c.RootFSNBDDevices {
		if err := validateNBDDevicePath(device); err != nil {
			return err
		}
		if _, exists := seenDevices[device]; exists {
			return fmt.Errorf("rootfs_nbd_device %q is duplicated", device)
		}
		seenDevices[device] = struct{}{}
	}
	if c.RootFSMaxDirtyTailBytes < 0 || c.RootFSMaxNodeDirtyTailBytes < 0 ||
		c.RootFSDirtyTailRetirementReserveBytes < 0 {
		return fmt.Errorf("RootFS dirty-tail limits must be non-negative")
	}
	if c.RootFSMaxDirtyTailBytes > maxDirtyTailLimitBytes || c.RootFSMaxNodeDirtyTailBytes > maxDirtyTailLimitBytes ||
		c.RootFSDirtyTailRetirementReserveBytes > maxDirtyTailLimitBytes {
		return fmt.Errorf("RootFS dirty-tail limits must not exceed %d bytes", maxDirtyTailLimitBytes)
	}
	if c.RootFSDirtyTailRetirementReserveBytes > c.RootFSMaxNodeDirtyTailBytes {
		return fmt.Errorf("RootFS dirty-tail retirement reserve must not exceed the node limit")
	}
	if c.RootFSStatePath == c.RuntimeSlotJournalPath {
		return fmt.Errorf("rootfs_state_path and runtime_slot_journal_path must be different files")
	}
	return nil
}

func validateNBDDevicePath(value string) error {
	if err := validateCanonicalAbsolutePath("rootfs_nbd_device", value); err != nil {
		return err
	}
	index := strings.TrimPrefix(value, "/dev/nbd")
	if index == value || index == "" || strings.IndexFunc(index, func(char rune) bool { return !unicode.IsDigit(char) }) >= 0 {
		return fmt.Errorf("rootfs_nbd_device must use /dev/nbd followed by a decimal device index")
	}
	return nil
}

func validateCanonicalAbsolutePath(name, value string) error {
	trimmed := strings.TrimSpace(value)
	if value != trimmed || !filepath.IsAbs(value) || value == string(filepath.Separator) || filepath.Clean(value) != value {
		return fmt.Errorf("%s must be a canonical non-root absolute path", name)
	}
	return nil
}

func validateCanonicalHTTPSOrigin(name, value string) error {
	parsed, err := url.Parse(value)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.User != nil ||
		parsed.Path != "" || parsed.RawPath != "" || parsed.RawQuery != "" || parsed.Fragment != "" ||
		value != strings.TrimSpace(value) {
		return fmt.Errorf("%s must be a canonical HTTPS origin", name)
	}
	return nil
}

type logger struct {
	base *zap.Logger
}

func newLogger(base *zap.Logger) logger {
	if base == nil {
		base = zap.NewNop()
	}
	return logger{base: base}
}

func (l logger) Named(name string) logger {
	return logger{base: l.base.Named(name)}
}

func (l logger) Info(message string, fields ...any) {
	l.base.Sugar().Infow(message, fields...)
}

func (l logger) Error(message string, fields ...any) {
	l.base.Sugar().Errorw(message, fields...)
}
