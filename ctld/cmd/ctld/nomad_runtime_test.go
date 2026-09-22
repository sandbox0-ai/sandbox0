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

package main

import (
	"strings"
	"testing"
	"time"

	apiconfig "github.com/sandbox0-ai/sandbox0/pkg/config"
	"go.uber.org/zap"
	"gopkg.in/yaml.v3"
)

func validCtldNomadConfig() *apiconfig.CtldConfig {
	return &apiconfig.CtldConfig{
		DefaultClusterId: "cluster-1",
		RootFSObjectStorage: apiconfig.RootFSObjectStorageConfig{
			Type: "s3", Bucket: "rootfs", Region: "us-east-1", Endpoint: "https://s3.internal",
		},
		NomadRuntime: apiconfig.CtldNomadRuntimeConfig{
			Enabled:                 true,
			NBDDevices:              []string{"/dev/nbd0", "/dev/nbd1"},
			AuthorityURL:            "https://manager.internal:9444",
			AuthorityCAFile:         "/etc/sandbox0/pki/manager-ca.pem",
			AuthorityClientCertFile: "/etc/sandbox0/pki/ctld.pem",
			AuthorityClientKeyFile:  "/etc/sandbox0/pki/ctld-key.pem",
			AuthorityTokenFile:      "/etc/sandbox0/tokens/manager.token",
			AuthorityPeerURISAN:     "spiffe://sandbox0.internal/region/runtime-slot-channel",
			NomadAddress:            "https://127.0.0.1:4646",
			NomadNodeID:             "node-1",
			NomadNamespace:          "default",
			NomadJobID:              "sandbox0-warm-slots",
			NomadTokenFile:          "/etc/sandbox0/tokens/nomad.token",
			NomadCAFile:             "/etc/sandbox0/pki/nomad-ca.pem",
			NomadCertFile:           "/etc/sandbox0/pki/nomad.pem",
			NomadKeyFile:            "/etc/sandbox0/pki/nomad-key.pem",
			NodeUID:                 "node-uid-1",
			ControlRoot:             "/run/sandbox0/nomad-slots",
			ResourceCgroupRoot:      "/sys/fs/cgroup/sandbox0",
			ResourceCPUMillicores:   4_000,
			ResourceMemoryBytes:     8 << 30,
			ResourceCPUSetCPUs:      "0-3",
			ResourceCPUSetMems:      "0",
		},
	}
}

func TestConfiguredNomadRuntimeFactoryValidatesBeforePrimaryElection(t *testing.T) {
	factory, err := configuredNomadRuntimeFactory(&apiconfig.CtldConfig{}, "/run/sandbox0/network.sock")
	if err != nil || factory != nil {
		t.Fatalf("disabled factory = %v, %v", factory, err)
	}

	config := validCtldNomadConfig()
	factory, err = configuredNomadRuntimeFactory(config, "/run/sandbox0/ctld-runtime-slot-network.sock")
	if err != nil || factory == nil {
		t.Fatalf("configured factory = %v, %v", factory, err)
	}
	service, err := factory(zap.NewNop())
	if err != nil || service == nil {
		t.Fatalf("primary service = %v, %v", service, err)
	}
	if service.Ready() {
		t.Fatal("Nomad runtime is ready before the HA primary starts it")
	}

	config.NomadRuntime.NomadAddress = "http://127.0.0.1:4646"
	if _, err := configuredNomadRuntimeFactory(config, "/run/sandbox0/ctld-runtime-slot-network.sock"); err == nil || !strings.Contains(err.Error(), "HTTPS origin") {
		t.Fatalf("insecure Nomad address error = %v", err)
	}
	config.NomadRuntime.NomadAddress = "https://127.0.0.1:4646"
	config.NomadRuntime.NodeControlTimeout.Duration = 500 * time.Millisecond
	if _, err := configuredNomadRuntimeFactory(config, "/run/sandbox0/ctld-runtime-slot-network.sock"); err == nil || !strings.Contains(err.Error(), "between one second and one minute") {
		t.Fatalf("unsafe node control timeout error = %v", err)
	}
}

// Exercise the deployment YAML through static service validation; a missing
// field in either decoder or factory must not silently disable enforcement.
func TestConfiguredMigrationStagingQuotaRequiresCompleteLimits(t *testing.T) {
	for _, tc := range []struct {
		name, fields string
		valid        bool
	}{
		{"disabled", "", true},
		{"complete", "migration_staging_bytes: 8388608\nmigration_staging_project_id: 42\nmigration_staging_inodes: 64\n", true},
		{"missing-project", "migration_staging_bytes: 8388608\nmigration_staging_inodes: 64\n", false},
		{"missing-bytes", "migration_staging_project_id: 42\nmigration_staging_inodes: 64\n", false},
		{"missing-inodes", "migration_staging_bytes: 8388608\nmigration_staging_project_id: 42\n", false},
		{"unaligned-bytes", "migration_staging_bytes: 8388609\nmigration_staging_project_id: 42\nmigration_staging_inodes: 64\n", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			config := validCtldNomadConfig()
			if err := yaml.Unmarshal([]byte(tc.fields), &config.NomadRuntime); err != nil {
				t.Fatal(err)
			}
			factory, err := configuredNomadRuntimeFactory(config, "/run/sandbox0/ctld-runtime-slot-network.sock")
			if (err == nil) != tc.valid {
				t.Fatalf("configuration error = %v, valid = %v", err, tc.valid)
			}
			if tc.valid && factory == nil {
				t.Fatal("valid runtime lacks a factory")
			}
		})
	}
}
