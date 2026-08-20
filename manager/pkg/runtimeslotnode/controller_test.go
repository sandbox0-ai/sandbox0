package runtimeslotnode

import (
	"bytes"
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/runtimeslotreconciler"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
)

type fakeTransport struct {
	target  Target
	request protocol.NodeCleanupControlRequest
	mutate  func(*protocol.NodeCleanupControlProof)
}

func (f *fakeTransport) CleanupRuntimeSlot(
	_ context.Context,
	target Target,
	request protocol.NodeCleanupControlRequest,
) (protocol.NodeCleanupControlProof, error) {
	f.target = target
	f.request = request
	proof := protocol.NodeCleanupControlProof{
		Version: protocol.NodeCleanupProofVersion, OperationID: request.OperationID,
		WriterOperationID: request.WriterOperationID, WriterRetireKind: request.WriterRetireKind,
		SlotID:    request.SlotID,
		ClusterID: request.ClusterID, AllocationID: request.AllocationID,
		NodeID: request.NodeID, NodeUID: request.NodeUID, NodeBootID: request.NodeBootID,
		NetNSIdentity: request.NetNSIdentity, RunscContainerID: request.RunscContainerID,
		WriterGrantID: request.WriterGrantID, WriterAuthorityDigest: request.WriterAuthorityDigest,
		RootFSOperationID: request.WriterOperationID, RootFSProofDigest: strings.Repeat("cd", 32),
		RunscAbsent: true, StableMountAbsent: true, RootFSWriterAbsent: true, NetworkPolicyAbsent: true,
	}
	proof.ProofDigest, _ = proof.Digest()
	if f.mutate != nil {
		f.mutate(&proof)
	}
	return proof, nil
}

func TestControllerDispatchesAndValidatesExactNodeProof(t *testing.T) {
	transport := &fakeTransport{}
	controller, err := New(transport)
	if err != nil {
		t.Fatal(err)
	}
	request := testCleanupRequest()
	first, err := controller.Cleanup(context.Background(), request)
	if err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}
	second, err := controller.Cleanup(context.Background(), request)
	if err != nil || !reflect.DeepEqual(first, second) {
		t.Fatalf("retry = %+v, %v; want %+v", second, err, first)
	}
	if len(first.ProofDigest) != 32 || transport.target != (Target{
		ClusterID: request.ClusterID, NodeID: request.NodeID,
		NodeUID: request.NodeUID, NodeBootID: request.NodeBootID,
	}) || transport.request.WriterAuthorityDigest != strings.Repeat("ab", 32) {
		t.Fatalf("proof = %+v target = %+v request = %+v", first, transport.target, transport.request)
	}
}

func TestControllerRejectsChangedOrMalformedNodeProof(t *testing.T) {
	for _, mutate := range []func(*protocol.NodeCleanupControlProof){
		func(proof *protocol.NodeCleanupControlProof) { proof.NodeBootID = "another-boot" },
		func(proof *protocol.NodeCleanupControlProof) { proof.NetworkPolicyAbsent = false },
		func(proof *protocol.NodeCleanupControlProof) { proof.ProofDigest = strings.Repeat("ef", 32) },
	} {
		transport := &fakeTransport{mutate: mutate}
		controller, err := New(transport)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := controller.Cleanup(context.Background(), testCleanupRequest()); err == nil {
			t.Fatal("Cleanup() accepted a changed node proof")
		}
	}
}

func TestControllerRejectsWriterProofOnGrantlessCleanup(t *testing.T) {
	transport := &fakeTransport{}
	controller, err := New(transport)
	if err != nil {
		t.Fatal(err)
	}
	request := testCleanupRequest()
	request.WriterGrantID = ""
	_, err = controller.Cleanup(context.Background(), request)
	if err == nil || transport.request.OperationID != "" {
		t.Fatalf("Cleanup() = %v, transport request = %+v", err, transport.request)
	}
}

func testCleanupRequest() runtimeslotreconciler.NodeCleanupRequest {
	return runtimeslotreconciler.NodeCleanupRequest{
		OperationID: "cleanup-1", WriterOperationID: "writer-1",
		WriterRetireKind: protocol.WriterRetireKindCrashAbandon, SlotID: "slot-1",
		ClusterID: "cluster-1", AllocationID: "allocation-1", NodeID: "node-1",
		NodeUID: "node-uid-1", NodeBootID: "boot-1", NetNSIdentity: "netns-v1:1:2",
		RunscContainerID: "runsc-1", WriterGrantID: "grant-1",
		WriterAuthorityDigest: bytes.Repeat([]byte{0xab}, 32),
	}
}
