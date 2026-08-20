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
	"path/filepath"
	"testing"

	"github.com/containerd/errdefs"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
)

func TestRootFSSessionNodeChannelIsExplicitAndFailClosed(t *testing.T) {
	if agent, err := newRootFSSessionNodeChannelAgent(PluginConfig{}, NomadAllocationConfig{}, nil); err != nil || agent != nil {
		t.Fatalf("disabled agent = %v, %v", agent, err)
	}
	directory := t.TempDir()
	config := PluginConfig{
		RootFSAuthorityURL:            "https://region.internal:8421",
		RootFSAuthorityCAFile:         filepath.Join(directory, "ca.pem"),
		RootFSAuthorityClientCertFile: filepath.Join(directory, "client.pem"),
		RootFSAuthorityClientKeyFile:  filepath.Join(directory, "client-key.pem"),
		RootFSAuthorityTokenFile:      filepath.Join(directory, "token"),
		RuntimeSlotNodeBootIDFile:     filepath.Join(directory, "boot-id"),
	}
	nomadConfig := NomadAllocationConfig{
		ClusterID: "cluster-1", NodeID: "node-1", RuntimeSlotChannelEnabled: true,
		RuntimeSlotNodeUID:           "node-uid-1",
		RuntimeSlotChannelPeerURISAN: "spiffe://sandbox0.test/region/runtime-slot-channel",
		RuntimeSlotControlRoot:       filepath.Join(directory, "control"),
	}
	agent, err := newRootFSSessionNodeChannelAgent(config, nomadConfig, &fakeRuntimeSlotCleaner{})
	if err != nil || agent == nil {
		t.Fatalf("enabled agent = %v, %v", agent, err)
	}
	nomadConfig.RuntimeSlotNodeUID = ""
	if _, err := newRootFSSessionNodeChannelAgent(config, nomadConfig, &fakeRuntimeSlotCleaner{}); !errdefs.IsInvalidArgument(err) {
		t.Fatalf("missing node UID error = %v", err)
	}
}

func TestRootFSSessionNodeChannelExecutorChecksLocalNodeBeforeCleanup(t *testing.T) {
	cleaner := &fakeRuntimeSlotCleaner{}
	executor := &rootFSSessionNodeChannelExecutor{
		clusterID: "cluster-1", nodeID: "node-1", nodeUID: "node-uid-1", cleaner: cleaner,
	}
	request := testNodeCleanupRequest()
	target := protocol.NodeChannelTarget{
		SlotID: request.SlotID, ClusterID: request.ClusterID, AllocationID: request.AllocationID,
		NodeID: request.NodeID, NodeUID: request.NodeUID, NodeBootID: request.NodeBootID,
	}
	proof, err := executor.Cleanup(t.Context(), target, request)
	if err != nil || proof.Request() != request || cleaner.request != request {
		t.Fatalf("cleanup proof = %+v, request = %+v, error = %v", proof, cleaner.request, err)
	}
	target.NodeID = "another-node"
	if _, err := executor.Cleanup(t.Context(), target, request); !errdefs.IsPermissionDenied(err) {
		t.Fatalf("cross-node cleanup error = %v", err)
	}
}
