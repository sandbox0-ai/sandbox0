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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/containerd/errdefs"
	"github.com/opencontainers/go-digest"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"

	"github.com/sandbox0-ai/sandbox0/pkg/objectstore"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	rootfssession "github.com/sandbox0-ai/sandbox0/pkg/rootfssession"
)

func TestRootFSRuntimeReclaimExternallyRetiredTerminalExpiry(t *testing.T) {
	for _, tc := range []struct {
		name          string
		missing       bool
		unreclaimed   bool
		age           bool
		freshBoundary string
		wantForgotten bool
	}{
		{name: "expired_reclaimed", age: true, wantForgotten: true},
		{name: "unexpired"},
		{name: "recent_request", age: true, freshBoundary: "requested_at"},
		{name: "recent_observation", age: true, freshBoundary: "observed_at"},
		{name: "recent_reclamation", age: true, freshBoundary: "updated_at"},
		{name: "expired_unreclaimed", age: true, unreclaimed: true},
		{name: "missing", missing: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fixture := newRuntimeTerminalExpiryFixture(t)
			if !tc.missing {
				fixture.fence(t, !tc.unreclaimed)
			}
			before := fixture.restart(t, tc.age, tc.freshBoundary)
			authority := &runtimeTerminalExpiryAuthority{}
			runtime := &rootfsRuntime{sessions: fixture.manager, authority: authority}
			hostCalls := append([]string(nil), fixture.host.calls...)

			forgotten, err := runtime.ReclaimExternallyRetired(t.Context(), fixture.stage)
			require.Equal(t, tc.wantForgotten, forgotten)
			if tc.wantForgotten {
				require.NoError(t, err)
				require.Empty(t, authority.requests, "expired reclaimed history must not query regional authority")
			} else {
				require.ErrorIs(t, err, errdefs.ErrPermissionDenied, "a terminal-authority 403 must fail closed")
				require.Equal(t, []rootfshandoff.StageRequest{fixture.stage}, authority.requests)
			}
			require.Equal(t, hostCalls, fixture.host.calls, "history expiry and authority denial must not touch host devices or mounts")

			parent, parentErr := fixture.manager.Parent(fixture.stage.Identity)
			if tc.wantForgotten || tc.missing {
				require.ErrorIs(t, parentErr, errdefs.ErrNotFound)
			} else {
				require.NoError(t, parentErr)
				require.Equal(t, fixture.stage.Parent, parent)
			}
			require.NoError(t, fixture.manager.Close())
			after := runtimeTerminalExpiryJournal(t, fixture.config.StatePath, "", false, "")
			if tc.wantForgotten {
				require.Len(t, before.records, 1)
				require.Len(t, before.identities, 1)
				require.Empty(t, after.records)
				require.Empty(t, after.identities)
			} else {
				require.Equal(t, before, after, "denied retirement must preserve the exact journal and identity index")
			}
		})
	}
}

func TestRootFSRuntimeReclaimExternallyRetiredRejectsDifferentBinding(t *testing.T) {
	for _, field := range []string{"node", "boot", "runtime_generation", "writer_grant"} {
		t.Run(field, func(t *testing.T) {
			fixture := newRuntimeTerminalExpiryFixture(t)
			fixture.fence(t, true)
			before := fixture.restart(t, true, "")
			other := fixture.stage
			switch field {
			case "node":
				other.Identity.NodeUID += "-other"
			case "boot":
				other.Identity.BootID += "-other"
			case "runtime_generation":
				other.Identity.RuntimeGeneration += "-other"
			case "writer_grant":
				other.Identity.WriterGrantID += "-other"
			}
			// Keep parent, RootFS ID and epoch identical: validation alone, or
			// matching just the writer identity index, must not authorize expiry.
			require.NoError(t, other.ValidateDurableBinding())
			authority := &runtimeTerminalExpiryAuthority{}
			runtime := &rootfsRuntime{sessions: fixture.manager, authority: authority}
			hostCalls := append([]string(nil), fixture.host.calls...)

			forgotten, err := runtime.ReclaimExternallyRetired(t.Context(), other)
			require.False(t, forgotten)
			require.ErrorIs(t, err, errdefs.ErrFailedPrecondition)
			require.Empty(t, authority.requests, "local binding rejection must not be swallowed as authority fallback")
			require.Equal(t, hostCalls, fixture.host.calls)
			parent, err := fixture.manager.Parent(fixture.stage.Identity)
			require.NoError(t, err)
			require.Equal(t, fixture.stage.Parent, parent)
			require.NoError(t, fixture.manager.Close())
			require.Equal(t, before, runtimeTerminalExpiryJournal(t, fixture.config.StatePath, "", false, ""))
		})
	}
}

type runtimeTerminalExpiryAuthority struct {
	// Any authority operation besides terminal verification is unexpected and
	// panics through this deliberately nil embedded interface.
	rootFSWriterAuthority
	requests []rootfshandoff.StageRequest
}

func (a *runtimeTerminalExpiryAuthority) VerifyTerminalWriterGrant(_ context.Context, stage rootfshandoff.StageRequest) error {
	a.requests = append(a.requests, stage)
	// Match the real client's HTTP 403 classification without opening a server.
	return fmt.Errorf("terminal authority HTTP 403: %w", errdefs.ErrPermissionDenied)
}

type runtimeTerminalExpiryHost struct {
	// The fixture never attaches a device. Unexpected host operations panic
	// instead of accidentally simulating a successful privileged operation.
	rootfssession.HostRuntime
	t     *testing.T
	calls []string
}

func (h *runtimeTerminalExpiryHost) UnmountOverlay(_ string, requireSync bool) error {
	require.False(h.t, requireSync, "pre-attachment cancellation has no filesystem to sync")
	h.calls = append(h.calls, "unmount_overlay")
	return nil
}

func (h *runtimeTerminalExpiryHost) UnmountXFS(_ string, requireSync bool) error {
	require.False(h.t, requireSync, "pre-attachment cancellation has no filesystem to sync")
	h.calls = append(h.calls, "unmount_xfs")
	return nil
}

func (h *runtimeTerminalExpiryHost) InspectPreAttachmentCrashFence(_, _ string) (rootfssession.CrashFenceHostObservation, error) {
	h.calls = append(h.calls, "inspect_pre_attachment")
	return rootfssession.CrashFenceHostObservation{
		NBDPoolAbsent: true, MergedMountAbsent: true, XFSMountAbsent: true,
	}, nil
}

type runtimeTerminalExpiryFixture struct {
	config  rootfssession.Config
	manager *rootfssession.Manager
	host    *runtimeTerminalExpiryHost
	stage   rootfshandoff.StageRequest
}

func newRuntimeTerminalExpiryFixture(t *testing.T) *runtimeTerminalExpiryFixture {
	t.Helper()
	store := objectstore.NewMemoryStore("").(objectstore.ContextConditionalStore)
	publisher := rootfsblock.ObjectStorePublisher{Store: store}
	logical := bytes.Repeat([]byte{0x53}, 3*rootfsblock.LogicalBlockSize)
	built, err := rootfsblock.BuildMaterializedGeneration(t.Context(), bytes.NewReader(logical), int64(len(logical)), publisher, rootfsblock.BuildOptions{})
	require.NoError(t, err)
	stage := rootfshandoff.StageRequest{
		BindingVersion:    rootfshandoff.WriterBindingVersion,
		Parent:            digest.FromString("terminal-expiry-parent").String(),
		InitialGeneration: "terminal-expiry-generation",
		Identity: rootfshandoff.Identity{
			NodeUID: "node", BootID: "boot", RuntimeGeneration: "runtime", AllocationID: "allocation",
			NetworkIncarnationID: "sandbox", TaskName: "app", SourceOCIDigest: "source-compatibility",
			RootFSDriver: "sandbox0-rootfs", RuntimeClass: "io.containerd.runsc.v1", SlotNonce: "slot",
			ClaimID: "claim", LaunchAttempt: "attempt", RootFSID: "rootfs", WriterEpoch: 1, WriterGrantID: "grant",
			WriterGrantTokenDigest: rootfshandoff.WriterGrantTokenDigest("test-only-issuance-token"),
		},
		ExpectedPolicyToken: rootfshandoff.NetworkPolicyToken{
			AllocationID: "allocation", NetworkIncarnationID: "sandbox", ClaimID: "claim", NetworkEpoch: 1,
			PolicyDigest: "policy", SourceIP: "10.0.0.2", CtldGeneration: "ctld", NetNSIdentity: "netns",
		},
		Generation: &rootfshandoff.GenerationDescriptor{
			Version: rootfshandoff.GenerationDescriptorVersion, GenerationID: "terminal-expiry-generation",
			FilesystemID: "rootfs", SourceOCIDigest: digest.FromString("oci").String(),
			BaseArtifactDigest: digest.FromString("artifact").String(),
			BaseBlockRoot:      built.Descriptor.MappingRoot.RootDigest, CurrentBlockHead: built.Descriptor.MappingRoot.RootDigest,
			WriterEpoch: 0, FormatGeneration: 2, DurabilityState: "s3_materialized", LocatorVersion: 1,
			Descriptor: built.Payload,
		},
	}
	require.NoError(t, stage.ValidateDurableBinding())
	base := t.TempDir()
	host := &runtimeTerminalExpiryHost{t: t}
	fixture := &runtimeTerminalExpiryFixture{
		config: rootfssession.Config{
			StatePath: filepath.Join(base, "state", "sessions.db"), BranchRoot: filepath.Join(base, "branches"),
			MountRoot: filepath.Join(base, "mounts"), Source: store, Publisher: publisher, Runtime: host,
		},
		host: host, stage: stage,
	}
	fixture.manager, err = rootfssession.New(fixture.config)
	require.NoError(t, err)
	t.Cleanup(func() {
		if fixture.manager != nil {
			require.NoError(t, fixture.manager.Close())
		}
	})
	return fixture
}

func (f *runtimeTerminalExpiryFixture) fence(t *testing.T, reclaimed bool) {
	t.Helper()
	require.NoError(t, f.manager.Reserve(f.stage))
	require.NoError(t, f.manager.Release(t.Context(), f.stage.Identity))
	proof, err := f.manager.CrashFenceExternal(f.stage, "regional-terminal-operation")
	require.NoError(t, err)
	require.NoError(t, proof.Validate())
	require.True(t, proof.NBDPoolAbsent)
	require.False(t, proof.DeviceBound)
	require.Equal(t, []string{"unmount_overlay", "unmount_xfs", "inspect_pre_attachment"}, f.host.calls)
	if reclaimed {
		// Production reaches this durable boundary only after regional terminal
		// verification. The test creates that prior success before injecting 403.
		require.NoError(t, f.manager.ReclaimTerminalArtifacts(f.stage.Parent, f.stage.Identity))
	}
}

func (f *runtimeTerminalExpiryFixture) restart(t *testing.T, age bool, freshBoundary string) runtimeTerminalJournalSnapshot {
	t.Helper()
	require.NoError(t, f.manager.Close())
	snapshot := runtimeTerminalExpiryJournal(t, f.config.StatePath, f.stage.Parent, age, freshBoundary)
	var err error
	f.manager, err = rootfssession.New(f.config)
	require.NoError(t, err)
	return snapshot
}

type runtimeTerminalJournalSnapshot struct {
	records    map[string]string
	identities map[string]string
}

// runtimeTerminalExpiryJournal accesses only a closed, t.TempDir-owned fixture
// database. Preserve the private record schema using RawMessage; aging must not
// fabricate physical evidence or bypass the manager's real lifecycle methods.
func runtimeTerminalExpiryJournal(t *testing.T, path, parent string, age bool, freshBoundary string) runtimeTerminalJournalSnapshot {
	t.Helper()
	db, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: time.Second, ReadOnly: !age})
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()
	const recordsBucket = "rootfs-sessions-v1"
	const identitiesBucket = "rootfs-session-identities-v1"
	if age {
		now := time.Now().UTC()
		old := now.Add(-2 * rootfssession.ExternalTerminalProofRetention)
		stamp := func(field string) json.RawMessage {
			at := old
			if field == freshBoundary {
				at = now
			}
			payload, err := json.Marshal(at.Format(time.RFC3339Nano))
			require.NoError(t, err)
			return payload
		}
		require.NoError(t, db.Update(func(tx *bolt.Tx) error {
			bucket := tx.Bucket([]byte(recordsBucket))
			require.NotNil(t, bucket)
			var stored, fence, result map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(bucket.Get([]byte(parent)), &stored))
			require.NoError(t, json.Unmarshal(stored["crash_fence"], &fence))
			require.NoError(t, json.Unmarshal(fence["result"], &result))
			stored["created_at"] = stamp("created_at")
			stored["updated_at"] = stamp("updated_at")
			fence["requested_at"] = stamp("requested_at")
			result["observed_at"] = stamp("observed_at")
			fence["result"], err = json.Marshal(result)
			require.NoError(t, err)
			stored["crash_fence"], err = json.Marshal(fence)
			require.NoError(t, err)
			payload, err := json.Marshal(stored)
			if err != nil {
				return err
			}
			return bucket.Put([]byte(parent), payload)
		}))
	}
	snapshot := runtimeTerminalJournalSnapshot{records: make(map[string]string), identities: make(map[string]string)}
	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		for name, entries := range map[string]map[string]string{recordsBucket: snapshot.records, identitiesBucket: snapshot.identities} {
			bucket := tx.Bucket([]byte(name))
			require.NotNil(t, bucket)
			if err := bucket.ForEach(func(key, value []byte) error {
				entries[string(key)] = string(value)
				return nil
			}); err != nil {
				return err
			}
		}
		return nil
	}))
	return snapshot
}
