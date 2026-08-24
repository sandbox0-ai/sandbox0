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
