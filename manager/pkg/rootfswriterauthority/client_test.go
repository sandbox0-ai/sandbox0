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
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"

	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
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

func crashAbandonClientTestStage() rootfshandoff.StageRequest {
	return rootfshandoff.StageRequest{
		BindingVersion:    rootfshandoff.WriterBindingVersion,
		Parent:            "sha256:" + strings.Repeat("1", 64),
		InitialGeneration: "generation-1",
		ExpectedPolicyToken: rootfshandoff.NetworkPolicyToken{
			PodUID: "allocation-1", PodSandboxID: "sandbox-1", ClaimID: "claim-1",
			NetworkEpoch: 1, PolicyDigest: "sha256:" + strings.Repeat("2", 64),
			PodIP: "192.0.2.2", CtldGeneration: "ctld-1", NetNSIdentity: "netns-1",
		},
		Identity: rootfshandoff.Identity{
			NodeUID: "node-1", BootID: "boot-1", RuntimeGeneration: "1",
			PodUID: "allocation-1", PodSandboxID: "sandbox-1", ContainerName: "slot",
			Image: "image-1", Snapshotter: "nomad-driver", RuntimeName: "sandbox0-gvisor",
			SlotNonce: "slot-1", ClaimID: "claim-1", LaunchAttempt: "attempt-1",
			RootFSID: "rootfs-1", WriterEpoch: 3, WriterGrantID: "grant-1",
			WriterGrantTokenDigest: rootfshandoff.WriterGrantTokenDigest("writer-token"),
		},
	}
}
