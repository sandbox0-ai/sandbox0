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
	"strings"
	"testing"
)

func validNodeRuntimeConfig() Config {
	return Config{
		RootFSObjectBucket:                    "sandbox0-rootfs",
		RootFSAuthorityURL:                    "https://manager.internal:9444",
		RootFSAuthorityCAFile:                 "/etc/sandbox0/pki/ca.pem",
		RootFSAuthorityClientCertFile:         "/etc/sandbox0/pki/ctld.pem",
		RootFSAuthorityClientKeyFile:          "/etc/sandbox0/pki/ctld-key.pem",
		RootFSAuthorityTokenFile:              "/etc/sandbox0/tokens/manager.token",
		RootFSNBDDevices:                      []string{"/dev/nbd0", "/dev/nbd1"},
		RootFSMaxDirtyTailBytes:               10 << 30,
		RootFSMaxNodeDirtyTailBytes:           40 << 30,
		RootFSDirtyTailRetirementReserveBytes: 64 << 20,
	}
}

func validNomadAllocationConfig() NomadAllocationConfig {
	return NomadAllocationConfig{
		ClusterID:                    "cluster-1",
		Address:                      "https://nomad.internal:4646",
		NodeID:                       "node-1",
		TokenFile:                    "/etc/sandbox0/tokens/nomad.token",
		CAFile:                       "/etc/sandbox0/pki/nomad-ca.pem",
		CertFile:                     "/etc/sandbox0/pki/nomad.pem",
		KeyFile:                      "/etc/sandbox0/pki/nomad-key.pem",
		RuntimeSlotChannelEnabled:    true,
		RuntimeSlotNodeUID:           "node-uid-1",
		RuntimeSlotChannelPeerURISAN: "spiffe://sandbox0.internal/region/runtime-slot-channel",
		RuntimeSlotControlRoot:       "/run/sandbox0/nomad-slots",
		RuntimeSlotCtldNetworkSocket: "/run/sandbox0/ctld-runtime-slot-network.sock",
		RuntimeResourceCPUMillicores: 4_000,
		RuntimeResourceMemoryBytes:   8 << 30,
		RuntimeResourceCPUSetCPUs:    "0-3",
		RuntimeResourceCPUSetMems:    "0",
	}
}

func TestConfigDefaultsUseCanonicalNetNSRoot(t *testing.T) {
	var config Config
	config.ApplyDefaults()
	if config.RootFSConsumerNetNSRoot != "/run/netns" {
		t.Fatalf("consumer network namespace root = %q", config.RootFSConsumerNetNSRoot)
	}
}

func TestConfigValidateRejectsUnsafeProductionInputs(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Config)
		match  string
	}{
		{name: "non HTTPS authority", mutate: func(c *Config) { c.RootFSAuthorityURL = "http://manager.internal" }, match: "HTTPS origin"},
		{name: "non canonical state", mutate: func(c *Config) { c.RootFSStatePath = "/var/lib/sandbox0/../sandbox0/state.db" }, match: "canonical"},
		{name: "different resource root", mutate: func(c *Config) { c.RuntimeResourceCgroupRoot = "/sys/fs/cgroup/other" }, match: "must be /sys/fs/cgroup/sandbox0"},
		{name: "duplicate device", mutate: func(c *Config) { c.RootFSNBDDevices = []string{"/dev/nbd0", "/dev/nbd0"} }, match: "duplicated"},
		{name: "relative device", mutate: func(c *Config) { c.RootFSNBDDevices = []string{"dev/nbd0"} }, match: "canonical"},
		{name: "arbitrary block device", mutate: func(c *Config) { c.RootFSNBDDevices = []string{"/dev/sda"} }, match: "/dev/nbd"},
		{name: "negative node cap", mutate: func(c *Config) { c.RootFSMaxNodeDirtyTailBytes = -1 }, match: "non-negative"},
		{name: "unbounded node cap", mutate: func(c *Config) { c.RootFSMaxNodeDirtyTailBytes = maxDirtyTailLimitBytes + 1 }, match: "must not exceed"},
		{name: "reserve exceeds cap", mutate: func(c *Config) { c.RootFSMaxNodeDirtyTailBytes = 32 << 20 }, match: "must not exceed"},
		{name: "missing encryption key", mutate: func(c *Config) { c.RootFSObjectEncryptionEnabled = true }, match: "encryption_key_path"},
		{name: "shared journals", mutate: func(c *Config) { c.RuntimeSlotJournalPath = "/var/lib/sandbox0/ctld/nomad/rootfs-sessions.db" }, match: "different files"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config := validNodeRuntimeConfig()
			test.mutate(&config)
			if err := config.Validate(); err == nil || !strings.Contains(err.Error(), test.match) {
				t.Fatalf("Validate() error = %v, want match %q", err, test.match)
			}
		})
	}
}

func TestValidateNomadAllocationConfigRejectsUntrustedEndpoints(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*NomadAllocationConfig)
		match  string
	}{
		{name: "insecure endpoint", mutate: func(c *NomadAllocationConfig) { c.Address = "http://nomad.internal:4646" }, match: "HTTPS origin"},
		{name: "relative token", mutate: func(c *NomadAllocationConfig) { c.TokenFile = "nomad.token" }, match: "canonical"},
		{name: "missing peer identity", mutate: func(c *NomadAllocationConfig) { c.RuntimeSlotChannelPeerURISAN = "" }, match: "SPIFFE"},
		{name: "query in peer identity", mutate: func(c *NomadAllocationConfig) { c.RuntimeSlotChannelPeerURISAN += "?node=1" }, match: "SPIFFE"},
		{name: "ambient control root", mutate: func(c *NomadAllocationConfig) { c.RuntimeSlotControlRoot = "/" }, match: "canonical"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config := validNomadAllocationConfig()
			test.mutate(&config)
			if err := validateNomadAllocationConfig(config); err == nil || !strings.Contains(err.Error(), test.match) {
				t.Fatalf("validateNomadAllocationConfig() error = %v, want match %q", err, test.match)
			}
		})
	}
}

func TestNewServicePerformsStaticValidationWithoutOpeningNodeResources(t *testing.T) {
	service, err := NewService(validNodeRuntimeConfig(), validNomadAllocationConfig(), nil)
	if err != nil || service == nil {
		t.Fatalf("NewService() = %v, %v", service, err)
	}
	if service.Ready() {
		t.Fatal("service is ready before acquiring the ctld HA primary lease")
	}
}
