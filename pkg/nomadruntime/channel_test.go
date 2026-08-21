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
	"context"
	"encoding/json"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/containerd/errdefs"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsrebase"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
)

type fakeRuntimeSlotNetworkSource struct {
	request protocol.RuntimeSlotNetworkPrepareRequest
}

type fakeRuntimeSlotRebaser struct {
	target  protocol.NodeChannelTarget
	request protocol.NodePausedRebaseControlRequest
	result  rootfsrebase.WorkerResult
	acks    int
}

func (r *fakeRuntimeSlotRebaser) AcknowledgePausedRootFSRebase(
	_ context.Context,
	target protocol.NodeChannelTarget,
	request protocol.NodePausedRebaseControlRequest,
) error {
	r.target = target
	r.request = request
	r.acks++
	return nil
}

func (r *fakeRuntimeSlotRebaser) ExecutePausedRootFSRebase(
	_ context.Context,
	target protocol.NodeChannelTarget,
	request protocol.NodePausedRebaseControlRequest,
) (rootfsrebase.WorkerResult, error) {
	r.target = target
	r.request = request
	return r.result, nil
}

func (r *fakeRuntimeSlotRebaser) RejectPausedRootFSRebase(
	_ context.Context,
	target protocol.NodeChannelTarget,
	request protocol.NodePausedRebaseControlRequest,
) (rootfsrebase.WorkerRejection, error) {
	r.target = target
	r.request = request
	return rootfsrebase.RejectWithoutResult(request.Worker)
}

func (s *fakeRuntimeSlotNetworkSource) runtimeSlotNetworkPrepareRequest(
	request protocol.NodeNetworkPrepareControlRequest,
) (protocol.RuntimeSlotNetworkPrepareRequest, error) {
	result := s.request
	result.Request = request
	return result, nil
}

func TestNomadRuntimeNodeChannelIsExplicitAndFailClosed(t *testing.T) {
	if agent, err := newNodeRuntimeChannelAgent(Config{}, NomadAllocationConfig{}, nil, nil); err != nil || agent != nil {
		t.Fatalf("disabled agent = %v, %v", agent, err)
	}
	directory := t.TempDir()
	config := Config{
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
	network, err := protocol.NewRuntimeSlotNetworkClient(filepath.Join(directory, "ctld-network.sock"), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	agent, err := newNodeRuntimeChannelAgent(config, nomadConfig, &fakeRuntimeSlotCleaner{}, network)
	if err != nil || agent == nil {
		t.Fatalf("enabled agent = %v, %v", agent, err)
	}
	nomadConfig.RuntimeSlotNodeUID = ""
	if _, err := newNodeRuntimeChannelAgent(config, nomadConfig, &fakeRuntimeSlotCleaner{}, network); !errdefs.IsInvalidArgument(err) {
		t.Fatalf("missing node UID error = %v", err)
	}
}

func TestNomadRuntimeNodeChannelExecutorChecksLocalNodeBeforeCleanup(t *testing.T) {
	cleaner := &fakeRuntimeSlotCleaner{}
	executor := &nodeRuntimeChannelExecutor{
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

func TestNomadRuntimeNodeChannelExecutorDelegatesPausedRebase(t *testing.T) {
	rebaser := &fakeRuntimeSlotRebaser{result: rootfsrebase.WorkerResult{ProofDigest: "proof-1"}}
	executor := &nodeRuntimeChannelExecutor{
		clusterID: "cluster-1", nodeID: "node-1", nodeUID: "node-uid-1", rebaser: rebaser,
	}
	target := protocol.NodeChannelTarget{
		ClusterID: "cluster-1", NodeID: "node-1", NodeUID: "node-uid-1", NodeBootID: "boot-1",
	}
	request := protocol.NodePausedRebaseControlRequest{
		Worker: rootfsrebase.WorkerRequest{OperationID: "rebase-operation-1"},
	}
	result, err := executor.PausedRebase(t.Context(), target, request)
	if err != nil || result.ProofDigest != rebaser.result.ProofDigest || rebaser.target != target ||
		rebaser.request.Worker.OperationID != request.Worker.OperationID {
		t.Fatalf("paused rebase = %+v, target = %+v, request = %+v, error = %v", result, rebaser.target, rebaser.request, err)
	}
	target.NodeUID = "another-node"
	if _, err := executor.PausedRebase(t.Context(), target, request); !errdefs.IsPermissionDenied(err) {
		t.Fatalf("cross-node paused rebase error = %v", err)
	}
	target.NodeUID = "node-uid-1"
	request.AcknowledgeProofDigest = "sha256:7e328799c89b54f08678a25d8e4d60129366a5c67f09fcca18b8de9f76f5fca7"
	if err := executor.AcknowledgePausedRebase(t.Context(), target, request); err != nil || rebaser.acks != 1 {
		t.Fatalf("paused rebase acknowledgement calls = %d, error = %v", rebaser.acks, err)
	}
}

func TestNomadRuntimeNodeChannelExecutorDelegatesNetworkToCtld(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("root-owned Unix socket test requires root")
	}
	directory := t.TempDir()
	socket := filepath.Join(directory, "ctld-network.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(socket, 0o600); err != nil {
		t.Fatal(err)
	}
	policy := `{"version":"v1","sandboxId":"sandbox-1","teamId":"team-1","mode":"block-all"}`
	request := protocol.NodeNetworkPrepareControlRequest{
		OperationID: "operation-1", ClaimID: "claim-1", SlotID: "slot-1",
		ClusterID: "cluster-1", AllocationID: "allocation-1", NodeID: "node-1",
		NodeUID: "node-uid-1", NodeBootID: "boot-1", NetNSIdentity: "netns-v1:1:2",
		NetworkPolicy: policy, PolicyDigest: protocol.NetworkPolicyDigest(policy),
	}
	server := &http.Server{Handler: http.HandlerFunc(func(writer http.ResponseWriter, httpRequest *http.Request) {
		var local protocol.RuntimeSlotNetworkPrepareRequest
		if err := json.NewDecoder(httpRequest.Body).Decode(&local); err != nil || local.Request != request || local.NetNSRelativePath != "allocation-1" {
			http.Error(writer, "unexpected request", http.StatusBadRequest)
			return
		}
		_ = json.NewEncoder(writer).Encode(rootfshandoff.NetworkPolicyToken{
			PodUID: request.AllocationID, PodSandboxID: protocol.RuntimeSlotNetworkIncarnationID(request), ClaimID: request.ClaimID,
			NetworkEpoch: 1, PolicyDigest: request.PolicyDigest, PodIP: "192.0.2.8",
			CtldGeneration: "ctld-1", NetNSIdentity: request.NetNSIdentity,
		})
	})}
	defer server.Close()
	go func() { _ = server.Serve(listener) }()
	client, err := protocol.NewRuntimeSlotNetworkClient(socket, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	source := &fakeRuntimeSlotNetworkSource{request: protocol.RuntimeSlotNetworkPrepareRequest{NetNSRelativePath: "allocation-1"}}
	executor := &nodeRuntimeChannelExecutor{
		clusterID: "cluster-1", nodeID: "node-1", nodeUID: "node-uid-1",
		network: client, networkSource: source,
	}
	target := protocol.NodeChannelTarget{
		SlotID: request.SlotID, ClusterID: request.ClusterID, AllocationID: request.AllocationID,
		NodeID: request.NodeID, NodeUID: request.NodeUID, NodeBootID: request.NodeBootID,
	}
	token, err := executor.PrepareNetwork(t.Context(), target, request)
	if err != nil || token.PodIP != "192.0.2.8" {
		t.Fatalf("PrepareNetwork() = %+v, %v", token, err)
	}
}
