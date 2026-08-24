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

package rootfswriterauthority

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/rootfswriterauthority"
)

func TestCrashAbandonBeginRequestUsesDurableStageBinding(t *testing.T) {
	stage := crashAbandonClientTestStage()
	request, err := crashAbandonBeginRequest(stage, "  crash-operation  ")
	if err != nil {
		t.Fatalf("crashAbandonBeginRequest() error = %v", err)
	}
	digest, err := stage.BindingDigest()
	if err != nil {
		t.Fatal(err)
	}
	if request.OperationID != "crash-operation" || request.WriterEpoch != stage.Identity.WriterEpoch ||
		request.BindingVersion != stage.BindingVersion || request.BindingDigest != hex.EncodeToString(digest[:]) ||
		request.ExpectedOldGenerationID != stage.InitialGeneration {
		t.Fatalf("request = %+v, want exact durable binding", request)
	}
	if _, err := crashAbandonBeginRequest(stage, ""); err == nil {
		t.Fatal("crashAbandonBeginRequest() accepted an empty operation")
	}
}

func TestCrashAbandonCompleteRequestFlattensBindingFields(t *testing.T) {
	request := CrashAbandonCompleteRequest{
		CrashAbandonBeginRequest: CrashAbandonBeginRequest{
			WriterEpoch: 3, BindingVersion: 1, BindingDigest: strings.Repeat("a", 64),
			OperationID: "crash-operation", ExpectedOldGenerationID: "generation-1",
		},
		Proof: rootfshandoff.CrashFenceProof{OperationID: "crash-operation"},
	}
	payload, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(payload, &fields); err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{
		"writer_epoch", "binding_version", "binding_digest", "operation_id",
		"expected_old_generation_id", "proof",
	} {
		if _, ok := fields[field]; !ok {
			t.Fatalf("encoded request lacks %q: %s", field, payload)
		}
	}
}

func TestPublishRunningForkRequestBindsStageTargetAndCheckpoint(t *testing.T) {
	stage := crashAbandonClientTestStage()
	fork := rootfshandoff.RunningForkCheckpointRequest{
		OperationID: "running-fork", SourceSandboxID: "source-sandbox",
		TargetSandboxID: "target-sandbox", TargetGenerationID: "target-generation",
	}
	checkpoint := runningForkClientTestCheckpoint(t, stage, fork)
	request, err := publishRunningForkRequest(stage, fork, checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	if request.WriterEpoch != stage.Identity.WriterEpoch || request.BindingDigest != checkpoint.Proof.BindingDigest ||
		request.Checkpoint.Proof.OperationID != fork.OperationID {
		t.Fatalf("request = %+v, want exact running fork binding", request)
	}
	checkpoint.Proof.TargetSandboxID = "changed-target"
	if _, err := publishRunningForkRequest(stage, fork, checkpoint); err == nil {
		t.Fatal("publishRunningForkRequest() accepted a changed target")
	}
}

func TestManagerClientPublishesRunningForkOnCanonicalGrantPath(t *testing.T) {
	stage := crashAbandonClientTestStage()
	fork := rootfshandoff.RunningForkCheckpointRequest{
		OperationID: "running-fork", SourceSandboxID: "source-sandbox",
		TargetSandboxID: "target-sandbox", TargetGenerationID: "target-generation",
	}
	checkpoint := runningForkClientTestCheckpoint(t, stage, fork)
	var received PublishRunningForkRequest
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPut || request.URL.EscapedPath() != "/internal/v1/rootfs-writer-grants/grant-1/fork-running" {
			t.Errorf("request = %s %s", request.Method, request.URL.EscapedPath())
		}
		if request.Header.Get("Authorization") != "Bearer projected-token" {
			t.Errorf("authorization = %q", request.Header.Get("Authorization"))
		}
		if err := json.NewDecoder(request.Body).Decode(&received); err != nil {
			t.Error(err)
		}
		writer.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	baseURL, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte("projected-token\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	client := &ManagerClient{baseURL: baseURL, tokenFile: tokenFile, http: server.Client()}
	if err := client.PublishRunningFork(context.Background(), stage, fork, checkpoint); err != nil {
		t.Fatal(err)
	}
	if received.Checkpoint.ProofDigest != checkpoint.ProofDigest || received.BindingDigest != checkpoint.Proof.BindingDigest {
		t.Fatalf("received = %+v, want exact checkpoint", received)
	}
}

func TestManagerClientRequestsPressurePauseWithDurableBinding(t *testing.T) {
	stage := crashAbandonClientTestStage().WithoutWriterGrantToken()
	expected := rootfshandoff.PlannedRetireOperationID(
		stage.Parent, stage.Identity.WriterGrantID, stage.Identity.WriterEpoch,
	)
	var received protocol.DirtyTailPressureRequest
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPut || request.URL.EscapedPath() != protocol.DirtyTailPressurePath("grant-1") {
			t.Errorf("request = %s %s", request.Method, request.URL.EscapedPath())
		}
		if err := json.NewDecoder(request.Body).Decode(&received); err != nil {
			t.Error(err)
		}
		writer.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(writer).Encode(protocol.DirtyTailPressureResponse{OperationID: expected})
	}))
	defer server.Close()
	baseURL, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte("projected-token\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	client := &ManagerClient{baseURL: baseURL, tokenFile: tokenFile, http: server.Client()}
	operationID, err := client.RequestWriterPressurePause(context.Background(), stage, rootfsblock.DirtyTailPressure{
		Scope:     protocol.DirtyTailPressureScopeSession,
		UsedBytes: rootfsblock.LogicalBlockSize, RequestedBytes: rootfsblock.LogicalBlockSize,
		LimitBytes: rootfsblock.LogicalBlockSize,
	})
	if err != nil {
		t.Fatal(err)
	}
	if operationID != expected || received.WriterEpoch != stage.Identity.WriterEpoch ||
		received.BindingVersion != stage.BindingVersion || received.Scope != protocol.DirtyTailPressureScopeSession {
		t.Fatalf("operation=%q request=%+v", operationID, received)
	}
}

func runningForkClientTestCheckpoint(
	t *testing.T,
	stage rootfshandoff.StageRequest,
	fork rootfshandoff.RunningForkCheckpointRequest,
) rootfshandoff.RunningForkCheckpointResult {
	t.Helper()
	root := digest.FromString("mapping-root").String()
	descriptor, err := rootfsblock.EncodeDescriptor(rootfsblock.Descriptor{
		Version: rootfsblock.DescriptorVersion, LogicalSizeBytes: rootfsblock.LogicalBlockSize,
		BlockSizeBytes: rootfsblock.LogicalBlockSize,
		MappingRoot: rootfsblock.MappingRootLocator{
			Version: rootfsblock.MappingPageVersion, RootDigest: root,
			Object: rootfsblock.ObjectRange{
				Key: "maps/root", Length: 1, Checksum: digest.FromString("mapping-page").String(),
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	binding, err := stage.BindingDigest()
	if err != nil {
		t.Fatal(err)
	}
	proof := rootfshandoff.RunningForkCheckpointProof{
		Version: rootfshandoff.RunningForkCheckpointVersion, OperationID: fork.OperationID,
		SourceSandboxID: fork.SourceSandboxID, SourceFilesystemID: stage.Identity.RootFSID,
		TargetSandboxID: fork.TargetSandboxID, SourceWriterGrantID: stage.Identity.WriterGrantID,
		SourceWriterEpoch: stage.Identity.WriterEpoch, BindingVersion: stage.BindingVersion,
		BindingDigest: hex.EncodeToString(binding[:]), ExpectedSourceGenerationID: stage.InitialGeneration,
		CheckpointGenerationID: fork.TargetGenerationID, CheckpointSequence: 1,
		CheckpointDescriptorDigest: digest.FromBytes(descriptor).String(),
	}
	proofDigest, err := proof.Digest()
	if err != nil {
		t.Fatal(err)
	}
	result := rootfshandoff.RunningForkCheckpointResult{
		Generation: rootfshandoff.GenerationDescriptor{
			Version: rootfshandoff.GenerationDescriptorVersion, GenerationID: fork.TargetGenerationID,
			FilesystemID: fork.TargetSandboxID, SourceOCIDigest: digest.FromString("source-oci").String(),
			BaseArtifactDigest: digest.FromString("base-artifact").String(), BaseBlockRoot: root,
			CurrentBlockHead: root, WriterEpoch: stage.Identity.WriterEpoch, FormatGeneration: 1,
			DurabilityState: rootfsblock.DurabilityS3, LocatorVersion: 2, Descriptor: descriptor,
		},
		Proof: proof, ProofDigest: hex.EncodeToString(proofDigest[:]),
	}
	if err := result.Validate(); err != nil {
		t.Fatal(err)
	}
	return result
}

func crashAbandonClientTestStage() rootfshandoff.StageRequest {
	return rootfshandoff.StageRequest{
		BindingVersion:    rootfshandoff.WriterBindingVersion,
		Parent:            "sha256:" + strings.Repeat("1", 64),
		InitialGeneration: "generation-1",
		ExpectedPolicyToken: rootfshandoff.NetworkPolicyToken{
			AllocationID: "allocation-1", NetworkIncarnationID: "sandbox-1", ClaimID: "claim-1",
			NetworkEpoch: 1, PolicyDigest: "sha256:" + strings.Repeat("2", 64),
			SourceIP: "192.0.2.2", CtldGeneration: "ctld-1", NetNSIdentity: "netns-1",
		},
		Identity: rootfshandoff.Identity{
			NodeUID: "node-1", BootID: "boot-1", RuntimeGeneration: "1",
			AllocationID: "allocation-1", NetworkIncarnationID: "sandbox-1", TaskName: "slot",
			SourceOCIDigest: "image-1", RootFSDriver: "nomad-driver", RuntimeClass: "sandbox0-gvisor",
			SlotNonce: "slot-1", ClaimID: "claim-1", LaunchAttempt: "attempt-1",
			RootFSID: "rootfs-1", WriterEpoch: 3, WriterGrantID: "grant-1",
			WriterGrantTokenDigest: rootfshandoff.WriterGrantTokenDigest("writer-token"),
		},
	}
}
