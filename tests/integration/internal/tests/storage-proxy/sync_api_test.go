package storageproxy

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/db"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/fsmeta"
	storagefsserver "github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/fsserver"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/pathnorm"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/volsync"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/volume"
	pb "github.com/sandbox0-ai/sandbox0/storage-proxy/proto/fs"
	"github.com/sirupsen/logrus"
)

type syncBootstrapResponseBody struct {
	Snapshot struct {
		ID string `json:"id"`
	} `json:"snapshot"`
	ReplayAfterSeq      int64  `json:"replay_after_seq"`
	ArchiveDownloadPath string `json:"archive_download_path"`
}

func TestSyncAPIRequestReplaySurvivesServiceRestart(t *testing.T) {
	env := newStorageProxySyncTestEnv(t)
	env.createVolume(t, "vol-1")

	upsertRecorder := env.newAuthedRequest(t, "PUT", "/sandboxvolumes/vol-1/sync/replicas/replica-1", newJSONBody(t, map[string]any{
		"display_name":   "Laptop",
		"platform":       "linux",
		"root_path":      "/workspace",
		"case_sensitive": true,
	}))
	if upsertRecorder.Code != 200 {
		t.Fatalf("upsert status = %d, want 200", upsertRecorder.Code)
	}

	firstRecorder := env.newAuthedRequest(t, "POST", "/sandboxvolumes/vol-1/sync/replicas/replica-1/changes", newJSONBody(t, map[string]any{
		"request_id": "req-1",
		"base_seq":   0,
		"changes": []map[string]any{{
			"event_type": "write",
			"path":       "/app/main.go",
		}},
	}))
	if firstRecorder.Code != 200 {
		t.Fatalf("first append status = %d, want 200", firstRecorder.Code)
	}
	firstResp, apiErr, err := spec.DecodeResponse[volsync.AppendChangesResponse](firstRecorder.Body)
	if err != nil {
		t.Fatalf("decode first append response: %v", err)
	}
	if apiErr != nil {
		t.Fatalf("unexpected first append api error: %+v", apiErr)
	}
	if len(firstResp.Accepted) != 1 || firstResp.HeadSeq != 1 {
		t.Fatalf("first append response = %+v, want one accepted change at head 1", firstResp)
	}

	env.sync = volsync.NewService(env.repo, nil)
	env.server = newStorageProxyTestServer(env)

	replayRecorder := env.newAuthedRequest(t, "POST", "/sandboxvolumes/vol-1/sync/replicas/replica-1/changes", newJSONBody(t, map[string]any{
		"request_id": "req-1",
		"base_seq":   0,
		"changes": []map[string]any{{
			"event_type": "write",
			"path":       "/app/main.go",
		}},
	}))
	if replayRecorder.Code != 200 {
		t.Fatalf("replay append status = %d, want 200", replayRecorder.Code)
	}
	replayResp, apiErr, err := spec.DecodeResponse[volsync.AppendChangesResponse](replayRecorder.Body)
	if err != nil {
		t.Fatalf("decode replay append response: %v", err)
	}
	if apiErr != nil {
		t.Fatalf("unexpected replay append api error: %+v", apiErr)
	}
	if len(replayResp.Accepted) != 1 || replayResp.Accepted[0].Seq != firstResp.Accepted[0].Seq || replayResp.HeadSeq != firstResp.HeadSeq {
		t.Fatalf("replay response = %+v, want same accepted seq/head as first response %+v", replayResp, firstResp)
	}

	listRecorder := env.newAuthedRequest(t, "GET", "/sandboxvolumes/vol-1/sync/changes?after_seq=0", nil)
	if listRecorder.Code != 200 {
		t.Fatalf("list changes status = %d, want 200", listRecorder.Code)
	}
	listResp, apiErr, err := spec.DecodeResponse[volsync.ListChangesResponse](listRecorder.Body)
	if err != nil {
		t.Fatalf("decode list changes response: %v", err)
	}
	if apiErr != nil {
		t.Fatalf("unexpected list changes api error: %+v", apiErr)
	}
	if len(listResp.Changes) != 1 || listResp.Changes[0].Path != "/app/main.go" {
		t.Fatalf("list changes response = %+v, want exactly one journal entry for /app/main.go", listResp)
	}
}

func TestSyncAPILearnedPortablePolicyConstrainsFutureReplicaPushes(t *testing.T) {
	env := newStorageProxySyncTestEnv(t)
	env.createVolume(t, "vol-1")

	linuxRecorder := env.newAuthedRequest(t, "PUT", "/sandboxvolumes/vol-1/sync/replicas/replica-linux", newJSONBody(t, map[string]any{
		"display_name":   "Linux",
		"platform":       "linux",
		"root_path":      "/workspace",
		"case_sensitive": true,
	}))
	if linuxRecorder.Code != 200 {
		t.Fatalf("linux upsert status = %d, want 200", linuxRecorder.Code)
	}

	windowsRecorder := env.newAuthedRequest(t, "PUT", "/sandboxvolumes/vol-1/sync/replicas/replica-win", newJSONBody(t, map[string]any{
		"display_name":   "Windows",
		"platform":       "windows",
		"root_path":      "C:/workspace",
		"case_sensitive": false,
		"capabilities": map[string]any{
			"case_sensitive":                    false,
			"unicode_normalization_insensitive": true,
			"windows_compatible_paths":          true,
		},
	}))
	if windowsRecorder.Code != 200 {
		t.Fatalf("windows upsert status = %d, want 200", windowsRecorder.Code)
	}

	linuxReplayRecorder := env.newAuthedRequest(t, "PUT", "/sandboxvolumes/vol-1/sync/replicas/replica-linux", newJSONBody(t, map[string]any{
		"display_name":   "Linux",
		"platform":       "linux",
		"root_path":      "/workspace",
		"case_sensitive": true,
	}))
	if linuxReplayRecorder.Code != 200 {
		t.Fatalf("linux re-upsert status = %d, want 200", linuxReplayRecorder.Code)
	}
	linuxResp, apiErr, err := spec.DecodeResponse[volsync.ReplicaEnvelope](linuxReplayRecorder.Body)
	if err != nil {
		t.Fatalf("decode linux re-upsert response: %v", err)
	}
	if apiErr != nil {
		t.Fatalf("unexpected linux re-upsert api error: %+v", apiErr)
	}
	if linuxResp.Replica == nil || !linuxResp.Replica.Capabilities.WindowsCompatiblePaths || linuxResp.Replica.Capabilities.CaseSensitive {
		t.Fatalf("linux replica capabilities = %+v, want learned windows-safe policy", linuxResp.Replica)
	}

	appendRecorder := env.newAuthedRequest(t, "POST", "/sandboxvolumes/vol-1/sync/replicas/replica-linux/changes", newJSONBody(t, map[string]any{
		"request_id": "req-linux-after-win",
		"base_seq":   0,
		"changes": []map[string]any{{
			"event_type": "write",
			"path":       "/app/CON.txt",
		}},
	}))
	if appendRecorder.Code != 200 {
		t.Fatalf("append status = %d, want 200", appendRecorder.Code)
	}
	appendResp, apiErr, err := spec.DecodeResponse[volsync.AppendChangesResponse](appendRecorder.Body)
	if err != nil {
		t.Fatalf("decode append response: %v", err)
	}
	if apiErr != nil {
		t.Fatalf("unexpected append api error: %+v", apiErr)
	}
	if len(appendResp.Accepted) != 0 || len(appendResp.Conflicts) != 1 {
		t.Fatalf("append response = %+v, want one portability conflict and no accepted changes", appendResp)
	}
	if appendResp.Conflicts[0].Reason != pathnorm.IssueCodeWindowsReservedName {
		t.Fatalf("conflict reason = %q, want %q", appendResp.Conflicts[0].Reason, pathnorm.IssueCodeWindowsReservedName)
	}

	listRecorder := env.newAuthedRequest(t, "GET", "/sandboxvolumes/vol-1/sync/changes?after_seq=0", nil)
	if listRecorder.Code != 200 {
		t.Fatalf("list changes status = %d, want 200", listRecorder.Code)
	}
	listResp, apiErr, err := spec.DecodeResponse[volsync.ListChangesResponse](listRecorder.Body)
	if err != nil {
		t.Fatalf("decode list changes response: %v", err)
	}
	if apiErr != nil {
		t.Fatalf("unexpected list changes api error: %+v", apiErr)
	}
	if len(listResp.Changes) != 0 {
		t.Fatalf("list changes response = %+v, want no persisted journal entries for incompatible path push", listResp)
	}
}

func TestSyncAPIListChangesReturnsReseedRequiredAfterCompaction(t *testing.T) {
	env := newStorageProxySyncTestEnv(t)
	env.createVolume(t, "vol-1")

	if err := env.sync.RecordRemoteChange(env.ctx, &volsync.RemoteChange{
		VolumeID:   "vol-1",
		TeamID:     "team-1",
		SandboxID:  "sandbox-1",
		EventType:  db.SyncEventWrite,
		Path:       "/app/one.txt",
		OccurredAt: time.Now().UTC().Add(-2 * time.Second),
	}); err != nil {
		t.Fatalf("RecordRemoteChange(one) error = %v", err)
	}
	if err := env.sync.RecordRemoteChange(env.ctx, &volsync.RemoteChange{
		VolumeID:   "vol-1",
		TeamID:     "team-1",
		SandboxID:  "sandbox-1",
		EventType:  db.SyncEventWrite,
		Path:       "/app/two.txt",
		OccurredAt: time.Now().UTC().Add(-1 * time.Second),
	}); err != nil {
		t.Fatalf("RecordRemoteChange(two) error = %v", err)
	}

	compactResp, err := env.sync.CompactJournal(env.ctx, &volsync.CompactJournalRequest{
		VolumeID:            "vol-1",
		TeamID:              "team-1",
		CompactedThroughSeq: 1,
	})
	if err != nil {
		t.Fatalf("CompactJournal() error = %v", err)
	}
	if compactResp.CompactedThroughSeq != 1 || compactResp.HeadSeq != 2 {
		t.Fatalf("CompactJournal() = %+v, want compacted_through_seq=1 head_seq=2", compactResp)
	}

	listRecorder := env.newAuthedRequest(t, "GET", "/sandboxvolumes/vol-1/sync/changes?after=0", nil)
	if listRecorder.Code != 409 {
		t.Fatalf("list changes status = %d, want 409", listRecorder.Code)
	}
	_, apiErr, err := spec.DecodeResponse[volsync.ListChangesResponse](listRecorder.Body)
	if err != nil {
		t.Fatalf("decode list changes error response: %v", err)
	}
	if apiErr == nil || apiErr.Message != volsync.ErrReseedRequired.Error() {
		t.Fatalf("api error = %+v, want reseed_required", apiErr)
	}
	details, ok := apiErr.Details.(map[string]any)
	if !ok {
		t.Fatalf("api error details = %#v, want object", apiErr.Details)
	}
	if details["reason"] != "reseed_required" {
		t.Fatalf("details.reason = %#v, want reseed_required", details["reason"])
	}
	if details["retained_after_seq"] != float64(1) {
		t.Fatalf("details.retained_after_seq = %#v, want 1", details["retained_after_seq"])
	}
	if details["head_seq"] != float64(2) {
		t.Fatalf("details.head_seq = %#v, want 2", details["head_seq"])
	}
}

func TestSyncAPIRemoteChangeAppearsInListChanges(t *testing.T) {
	env := newStorageProxySyncTestEnv(t)
	env.createVolume(t, "vol-1")

	occurredAt := time.Date(2026, 3, 25, 12, 0, 0, 0, time.UTC)
	if err := env.sync.RecordRemoteChange(env.ctx, &volsync.RemoteChange{
		VolumeID:   "vol-1",
		TeamID:     "team-1",
		SandboxID:  "sandbox-1",
		EventType:  db.SyncEventRename,
		Path:       "/app/new.go",
		OldPath:    "/app/old.go",
		OccurredAt: occurredAt,
	}); err != nil {
		t.Fatalf("RecordRemoteChange() error = %v", err)
	}

	listRecorder := env.newAuthedRequest(t, "GET", "/sandboxvolumes/vol-1/sync/changes?after=0", nil)
	if listRecorder.Code != 200 {
		t.Fatalf("list changes status = %d, want 200", listRecorder.Code)
	}
	listResp, apiErr, err := spec.DecodeResponse[volsync.ListChangesResponse](listRecorder.Body)
	if err != nil {
		t.Fatalf("decode list changes response: %v", err)
	}
	if apiErr != nil {
		t.Fatalf("unexpected list changes api error: %+v", apiErr)
	}
	if len(listResp.Changes) != 1 {
		t.Fatalf("changes = %+v, want exactly one remote journal entry", listResp.Changes)
	}
	change := listResp.Changes[0]
	if change.Source != db.SyncSourceSandbox || change.EventType != db.SyncEventRename {
		t.Fatalf("change source/event = %q/%q, want sandbox/rename", change.Source, change.EventType)
	}
	if change.Path != "/app/new.go" || change.OldPath == nil || *change.OldPath != "/app/old.go" {
		t.Fatalf("change paths = %+v, want rename /app/old.go -> /app/new.go", change)
	}
	if change.Metadata == nil || string(*change.Metadata) == "" {
		t.Fatalf("change metadata = %v, want sandbox_id metadata", change.Metadata)
	}
}

func TestSyncAPISandboxOriginatedReplayableWritesExposeReplayMetadataAndPayload(t *testing.T) {
	env := newStorageProxySyncTestEnv(t)
	env.createVolume(t, "vol-1")

	volCtx := newMountedIntegrationVolumeContext(t, "vol-1", "team-1")
	fsServer := storagefsserver.NewFileSystemServer(&integrationMountedVolumeManager{
		volumes: map[string]*volume.VolumeContext{
			"vol-1": volCtx,
		},
	}, env.repo, nil, nil, logrus.New(), env.sync, nil)

	ctx := internalauth.WithClaims(context.Background(), &internalauth.Claims{
		TeamID:    "team-1",
		UserID:    "user-1",
		SandboxID: "sandbox-1",
	})

	createResp, err := fsServer.Create(ctx, &pb.CreateRequest{
		VolumeId: "vol-1",
		Parent:   uint64(fsmeta.RootInode),
		Name:     "hello.txt",
		Mode:     0o644,
	})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	payload := []byte("hello from sandbox")
	writeResp, err := fsServer.Write(ctx, &pb.WriteRequest{
		VolumeId: "vol-1",
		Inode:    createResp.Inode,
		HandleId: createResp.HandleId,
		Data:     payload,
		Offset:   0,
	})
	if err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if writeResp.BytesWritten != int64(len(payload)) {
		t.Fatalf("BytesWritten = %d, want %d", writeResp.BytesWritten, len(payload))
	}

	if _, err := fsServer.Flush(ctx, &pb.FlushRequest{
		VolumeId: "vol-1",
		HandleId: createResp.HandleId,
	}); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	if _, err := fsServer.Release(ctx, &pb.ReleaseRequest{
		VolumeId: "vol-1",
		Inode:    createResp.Inode,
		HandleId: createResp.HandleId,
	}); err != nil {
		t.Fatalf("Release() error = %v", err)
	}

	if _, err := fsServer.SetAttr(ctx, &pb.SetAttrRequest{
		VolumeId: "vol-1",
		Inode:    createResp.Inode,
		Valid:    uint32(fsmeta.SetAttrMode),
		Attr: &pb.GetAttrResponse{
			Mode: 0o600,
		},
	}); err != nil {
		t.Fatalf("SetAttr(mode) error = %v", err)
	}

	listRecorder := env.newAuthedRequest(t, "GET", "/sandboxvolumes/vol-1/sync/changes?after=0", nil)
	if listRecorder.Code != 200 {
		t.Fatalf("list changes status = %d, want 200", listRecorder.Code)
	}
	listResp, apiErr, err := spec.DecodeResponse[volsync.ListChangesResponse](listRecorder.Body)
	if err != nil {
		t.Fatalf("decode list changes response: %v", err)
	}
	if apiErr != nil {
		t.Fatalf("unexpected list changes api error: %+v", apiErr)
	}
	if len(listResp.Changes) != 3 {
		t.Fatalf("changes = %+v, want create + write + chmod", listResp.Changes)
	}

	createEntry := listResp.Changes[0]
	if createEntry.Source != db.SyncSourceSandbox || createEntry.EventType != db.SyncEventCreate {
		t.Fatalf("create entry source/event = %q/%q, want sandbox/create", createEntry.Source, createEntry.EventType)
	}
	if createEntry.EntryKind == nil || *createEntry.EntryKind != "file" {
		t.Fatalf("create entry_kind = %#v, want file", createEntry.EntryKind)
	}
	if createEntry.Mode == nil || *createEntry.Mode != int64(0o644) {
		t.Fatalf("create mode = %#v, want 0644", createEntry.Mode)
	}
	if createEntry.ContentRef != nil {
		t.Fatalf("create content_ref = %#v, want nil", createEntry.ContentRef)
	}

	writeEntry := listResp.Changes[1]
	if writeEntry.Source != db.SyncSourceSandbox || writeEntry.EventType != db.SyncEventWrite {
		t.Fatalf("write entry source/event = %q/%q, want sandbox/write", writeEntry.Source, writeEntry.EventType)
	}
	if writeEntry.EntryKind == nil || *writeEntry.EntryKind != "file" {
		t.Fatalf("write entry_kind = %#v, want file", writeEntry.EntryKind)
	}
	if writeEntry.Mode == nil || *writeEntry.Mode != int64(0o644) {
		t.Fatalf("write mode = %#v, want 0644", writeEntry.Mode)
	}
	if writeEntry.ContentRef == nil || *writeEntry.ContentRef == "" {
		t.Fatalf("write content_ref = %#v, want non-empty", writeEntry.ContentRef)
	}
	if writeEntry.SizeBytes == nil || *writeEntry.SizeBytes != int64(len(payload)) {
		t.Fatalf("write size_bytes = %#v, want %d", writeEntry.SizeBytes, len(payload))
	}
	wantSHA256 := sha256.Sum256(payload)
	if writeEntry.ContentSHA256 == nil || *writeEntry.ContentSHA256 != hex.EncodeToString(wantSHA256[:]) {
		t.Fatalf("write content_sha256 = %#v, want %q", writeEntry.ContentSHA256, hex.EncodeToString(wantSHA256[:]))
	}

	replayRecorder := env.newAuthedRequest(t, "GET", "/sandboxvolumes/vol-1/sync/replay-payload?content_ref="+*writeEntry.ContentRef, nil)
	if replayRecorder.Code != 200 {
		t.Fatalf("replay payload status = %d, want 200", replayRecorder.Code)
	}
	if got := replayRecorder.Body.Bytes(); string(got) != string(payload) {
		t.Fatalf("replay payload = %q, want %q", string(got), string(payload))
	}

	chmodEntry := listResp.Changes[2]
	if chmodEntry.Source != db.SyncSourceSandbox || chmodEntry.EventType != db.SyncEventChmod {
		t.Fatalf("chmod entry source/event = %q/%q, want sandbox/chmod", chmodEntry.Source, chmodEntry.EventType)
	}
	if chmodEntry.Mode == nil || *chmodEntry.Mode != int64(0o600) {
		t.Fatalf("chmod mode = %#v, want 0600", chmodEntry.Mode)
	}
	if chmodEntry.ContentRef != nil {
		t.Fatalf("chmod content_ref = %#v, want nil", chmodEntry.ContentRef)
	}
}

func TestSyncAPIConflictPersistsAndListConflictsReturnsStoredConflict(t *testing.T) {
	env := newStorageProxySyncTestEnv(t)
	env.createVolume(t, "vol-1")

	upsertRecorder := env.newAuthedRequest(t, "PUT", "/sandboxvolumes/vol-1/sync/replicas/replica-1", newJSONBody(t, map[string]any{
		"display_name":   "Laptop",
		"platform":       "linux",
		"root_path":      "/workspace",
		"case_sensitive": true,
	}))
	if upsertRecorder.Code != 200 {
		t.Fatalf("upsert status = %d, want 200", upsertRecorder.Code)
	}

	if err := env.sync.RecordRemoteChange(env.ctx, &volsync.RemoteChange{
		VolumeID:   "vol-1",
		TeamID:     "team-1",
		SandboxID:  "sandbox-1",
		EventType:  db.SyncEventWrite,
		Path:       "/app/main.go",
		OccurredAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("RecordRemoteChange() error = %v", err)
	}

	appendRecorder := env.newAuthedRequest(t, "POST", "/sandboxvolumes/vol-1/sync/replicas/replica-1/changes", newJSONBody(t, map[string]any{
		"request_id": "req-conflict-1",
		"base_seq":   0,
		"changes": []map[string]any{{
			"event_type": "write",
			"path":       "/app/main.go",
		}},
	}))
	if appendRecorder.Code != 200 {
		t.Fatalf("append status = %d, want 200", appendRecorder.Code)
	}
	appendResp, apiErr, err := spec.DecodeResponse[volsync.AppendChangesResponse](appendRecorder.Body)
	if err != nil {
		t.Fatalf("decode append response: %v", err)
	}
	if apiErr != nil {
		t.Fatalf("unexpected append api error: %+v", apiErr)
	}
	if len(appendResp.Accepted) != 0 || len(appendResp.Conflicts) != 1 {
		t.Fatalf("append response = %+v, want one persisted conflict and no accepted changes", appendResp)
	}

	conflictsRecorder := env.newAuthedRequest(t, "GET", "/sandboxvolumes/vol-1/sync/conflicts?status=open", nil)
	if conflictsRecorder.Code != 200 {
		t.Fatalf("list conflicts status = %d, want 200", conflictsRecorder.Code)
	}
	conflictsResp, apiErr, err := spec.DecodeResponse[volsync.ListConflictsResponse](conflictsRecorder.Body)
	if err != nil {
		t.Fatalf("decode list conflicts response: %v", err)
	}
	if apiErr != nil {
		t.Fatalf("unexpected list conflicts api error: %+v", apiErr)
	}
	if len(conflictsResp.Conflicts) != 1 {
		t.Fatalf("conflicts = %+v, want exactly one stored conflict", conflictsResp.Conflicts)
	}
	if conflictsResp.Conflicts[0].ID != appendResp.Conflicts[0].ID {
		t.Fatalf("conflict id = %q, want %q", conflictsResp.Conflicts[0].ID, appendResp.Conflicts[0].ID)
	}
	if conflictsResp.Conflicts[0].Reason != "concurrent_update" {
		t.Fatalf("conflict reason = %q, want concurrent_update", conflictsResp.Conflicts[0].Reason)
	}
}

func TestSyncAPIUpdateCursorSupportsReseedRecoveryAfterCompaction(t *testing.T) {
	env := newStorageProxySyncTestEnv(t)
	env.createVolume(t, "vol-1")

	upsertRecorder := env.newAuthedRequest(t, "PUT", "/sandboxvolumes/vol-1/sync/replicas/replica-1", newJSONBody(t, map[string]any{
		"display_name":   "Laptop",
		"platform":       "linux",
		"root_path":      "/workspace",
		"case_sensitive": true,
	}))
	if upsertRecorder.Code != 200 {
		t.Fatalf("upsert status = %d, want 200", upsertRecorder.Code)
	}

	if err := env.sync.RecordRemoteChange(env.ctx, &volsync.RemoteChange{
		VolumeID:   "vol-1",
		TeamID:     "team-1",
		SandboxID:  "sandbox-1",
		EventType:  db.SyncEventWrite,
		Path:       "/app/one.txt",
		OccurredAt: time.Now().UTC().Add(-2 * time.Second),
	}); err != nil {
		t.Fatalf("RecordRemoteChange(one) error = %v", err)
	}
	if err := env.sync.RecordRemoteChange(env.ctx, &volsync.RemoteChange{
		VolumeID:   "vol-1",
		TeamID:     "team-1",
		SandboxID:  "sandbox-1",
		EventType:  db.SyncEventWrite,
		Path:       "/app/two.txt",
		OccurredAt: time.Now().UTC().Add(-1 * time.Second),
	}); err != nil {
		t.Fatalf("RecordRemoteChange(two) error = %v", err)
	}

	if _, err := env.sync.CompactJournal(env.ctx, &volsync.CompactJournalRequest{
		VolumeID:            "vol-1",
		TeamID:              "team-1",
		CompactedThroughSeq: 1,
	}); err != nil {
		t.Fatalf("CompactJournal() error = %v", err)
	}

	staleCursorRecorder := env.newAuthedRequest(t, "PUT", "/sandboxvolumes/vol-1/sync/replicas/replica-1/cursor", newJSONBody(t, map[string]any{
		"last_applied_seq": 0,
	}))
	if staleCursorRecorder.Code != 409 {
		t.Fatalf("stale cursor status = %d, want 409", staleCursorRecorder.Code)
	}
	_, apiErr, err := spec.DecodeResponse[volsync.ReplicaEnvelope](staleCursorRecorder.Body)
	if err != nil {
		t.Fatalf("decode stale cursor response: %v", err)
	}
	if apiErr == nil || apiErr.Message != volsync.ErrReseedRequired.Error() {
		t.Fatalf("api error = %+v, want reseed_required", apiErr)
	}

	recoveryCursorRecorder := env.newAuthedRequest(t, "PUT", "/sandboxvolumes/vol-1/sync/replicas/replica-1/cursor", newJSONBody(t, map[string]any{
		"last_applied_seq": 2,
	}))
	if recoveryCursorRecorder.Code != 200 {
		t.Fatalf("recovery cursor status = %d, want 200", recoveryCursorRecorder.Code)
	}
	recoveryResp, apiErr, err := spec.DecodeResponse[volsync.ReplicaEnvelope](recoveryCursorRecorder.Body)
	if err != nil {
		t.Fatalf("decode recovery cursor response: %v", err)
	}
	if apiErr != nil {
		t.Fatalf("unexpected recovery cursor api error: %+v", apiErr)
	}
	if recoveryResp.Replica == nil || recoveryResp.Replica.LastAppliedSeq != 2 || recoveryResp.HeadSeq != 2 {
		t.Fatalf("recovery response = %+v, want replica at seq/head 2", recoveryResp)
	}

	env.sync = volsync.NewService(env.repo, nil)
	env.server = newStorageProxyTestServer(env)

	replicaRecorder := env.newAuthedRequest(t, "GET", "/sandboxvolumes/vol-1/sync/replicas/replica-1", nil)
	if replicaRecorder.Code != 200 {
		t.Fatalf("get replica status = %d, want 200", replicaRecorder.Code)
	}
	replicaResp, apiErr, err := spec.DecodeResponse[volsync.ReplicaEnvelope](replicaRecorder.Body)
	if err != nil {
		t.Fatalf("decode get replica response: %v", err)
	}
	if apiErr != nil {
		t.Fatalf("unexpected get replica api error: %+v", apiErr)
	}
	if replicaResp.Replica == nil || replicaResp.Replica.LastAppliedSeq != 2 {
		t.Fatalf("replica response = %+v, want persisted last_applied_seq=2", replicaResp)
	}
}

func TestSyncAPIResolveConflictPersistsAcrossRestart(t *testing.T) {
	env := newStorageProxySyncTestEnv(t)
	env.createVolume(t, "vol-1")

	upsertRecorder := env.newAuthedRequest(t, "PUT", "/sandboxvolumes/vol-1/sync/replicas/replica-1", newJSONBody(t, map[string]any{
		"display_name":   "Laptop",
		"platform":       "linux",
		"root_path":      "/workspace",
		"case_sensitive": true,
	}))
	if upsertRecorder.Code != 200 {
		t.Fatalf("upsert status = %d, want 200", upsertRecorder.Code)
	}

	if err := env.sync.RecordRemoteChange(env.ctx, &volsync.RemoteChange{
		VolumeID:   "vol-1",
		TeamID:     "team-1",
		SandboxID:  "sandbox-1",
		EventType:  db.SyncEventWrite,
		Path:       "/app/main.go",
		OccurredAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("RecordRemoteChange() error = %v", err)
	}

	appendRecorder := env.newAuthedRequest(t, "POST", "/sandboxvolumes/vol-1/sync/replicas/replica-1/changes", newJSONBody(t, map[string]any{
		"request_id": "req-conflict-resolve-1",
		"base_seq":   0,
		"changes": []map[string]any{{
			"event_type": "write",
			"path":       "/app/main.go",
		}},
	}))
	if appendRecorder.Code != 200 {
		t.Fatalf("append status = %d, want 200", appendRecorder.Code)
	}
	appendResp, apiErr, err := spec.DecodeResponse[volsync.AppendChangesResponse](appendRecorder.Body)
	if err != nil {
		t.Fatalf("decode append response: %v", err)
	}
	if apiErr != nil {
		t.Fatalf("unexpected append api error: %+v", apiErr)
	}
	if len(appendResp.Conflicts) != 1 {
		t.Fatalf("append response = %+v, want one conflict", appendResp)
	}

	resolveRecorder := env.newAuthedRequest(t, "PUT", "/sandboxvolumes/vol-1/sync/conflicts/"+appendResp.Conflicts[0].ID, newJSONBody(t, map[string]any{
		"status":     "resolved",
		"resolution": "keep_remote",
		"note":       "accepted sandbox version",
	}))
	if resolveRecorder.Code != 200 {
		t.Fatalf("resolve conflict status = %d, want 200", resolveRecorder.Code)
	}

	env.sync = volsync.NewService(env.repo, nil)
	env.server = newStorageProxyTestServer(env)

	resolvedRecorder := env.newAuthedRequest(t, "GET", "/sandboxvolumes/vol-1/sync/conflicts?status=resolved", nil)
	if resolvedRecorder.Code != 200 {
		t.Fatalf("list resolved conflicts status = %d, want 200", resolvedRecorder.Code)
	}
	resolvedResp, apiErr, err := spec.DecodeResponse[volsync.ListConflictsResponse](resolvedRecorder.Body)
	if err != nil {
		t.Fatalf("decode resolved conflicts response: %v", err)
	}
	if apiErr != nil {
		t.Fatalf("unexpected resolved conflicts api error: %+v", apiErr)
	}
	if len(resolvedResp.Conflicts) != 1 {
		t.Fatalf("resolved conflicts = %+v, want one resolved conflict", resolvedResp.Conflicts)
	}
	if resolvedResp.Conflicts[0].Status != db.SyncConflictStatusResolved {
		t.Fatalf("resolved conflict status = %q, want %q", resolvedResp.Conflicts[0].Status, db.SyncConflictStatusResolved)
	}
	if resolvedResp.Conflicts[0].Metadata == nil || string(*resolvedResp.Conflicts[0].Metadata) == "" {
		t.Fatalf("resolved conflict metadata = %v, want resolution metadata", resolvedResp.Conflicts[0].Metadata)
	}
}

func TestSyncAPIPersistedNamespacePolicySurvivesRestart(t *testing.T) {
	env := newStorageProxySyncTestEnv(t)
	env.createVolume(t, "vol-1")

	windowsRecorder := env.newAuthedRequest(t, "PUT", "/sandboxvolumes/vol-1/sync/replicas/replica-win", newJSONBody(t, map[string]any{
		"display_name":   "Windows",
		"platform":       "windows",
		"root_path":      "C:/workspace",
		"case_sensitive": false,
		"capabilities": map[string]any{
			"case_sensitive":                    false,
			"unicode_normalization_insensitive": true,
			"windows_compatible_paths":          true,
		},
	}))
	if windowsRecorder.Code != 200 {
		t.Fatalf("windows upsert status = %d, want 200", windowsRecorder.Code)
	}

	env.sync = volsync.NewService(env.repo, nil)
	env.server = newStorageProxyTestServer(env)

	linuxRecorder := env.newAuthedRequest(t, "PUT", "/sandboxvolumes/vol-1/sync/replicas/replica-linux", newJSONBody(t, map[string]any{
		"display_name":   "Linux",
		"platform":       "linux",
		"root_path":      "/workspace",
		"case_sensitive": true,
	}))
	if linuxRecorder.Code != 200 {
		t.Fatalf("linux upsert status = %d, want 200", linuxRecorder.Code)
	}
	linuxResp, apiErr, err := spec.DecodeResponse[volsync.ReplicaEnvelope](linuxRecorder.Body)
	if err != nil {
		t.Fatalf("decode linux upsert response: %v", err)
	}
	if apiErr != nil {
		t.Fatalf("unexpected linux upsert api error: %+v", apiErr)
	}
	if linuxResp.Replica == nil || !linuxResp.Replica.Capabilities.WindowsCompatiblePaths || linuxResp.Replica.Capabilities.CaseSensitive {
		t.Fatalf("linux replica capabilities = %+v, want persisted windows-safe policy after restart", linuxResp.Replica)
	}

	appendRecorder := env.newAuthedRequest(t, "POST", "/sandboxvolumes/vol-1/sync/replicas/replica-linux/changes", newJSONBody(t, map[string]any{
		"request_id": "req-linux-restart-policy",
		"base_seq":   0,
		"changes": []map[string]any{{
			"event_type": "write",
			"path":       "/app/aux.txt",
		}},
	}))
	if appendRecorder.Code != 200 {
		t.Fatalf("append status = %d, want 200", appendRecorder.Code)
	}
	appendResp, apiErr, err := spec.DecodeResponse[volsync.AppendChangesResponse](appendRecorder.Body)
	if err != nil {
		t.Fatalf("decode append response: %v", err)
	}
	if apiErr != nil {
		t.Fatalf("unexpected append api error: %+v", apiErr)
	}
	if len(appendResp.Accepted) != 0 || len(appendResp.Conflicts) != 1 {
		t.Fatalf("append response = %+v, want one persisted portability conflict and no accepted changes", appendResp)
	}
	if appendResp.Conflicts[0].Reason != pathnorm.IssueCodeWindowsReservedName {
		t.Fatalf("conflict reason = %q, want %q", appendResp.Conflicts[0].Reason, pathnorm.IssueCodeWindowsReservedName)
	}
}

func TestSyncAPIBootstrapReturnsReplayAnchorAndArchive(t *testing.T) {
	env := newStorageProxySyncTestEnv(t)
	env.createVolume(t, "vol-1")
	env.snapshotMgr.exportBody = []byte("bootstrap-archive-body")

	if err := env.sync.RecordRemoteChange(env.ctx, &volsync.RemoteChange{
		VolumeID:   "vol-1",
		TeamID:     "team-1",
		SandboxID:  "sandbox-1",
		EventType:  db.SyncEventWrite,
		Path:       "/app/main.go",
		OccurredAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("RecordRemoteChange() error = %v", err)
	}

	bootstrapRecorder := env.newAuthedRequest(t, "POST", "/sandboxvolumes/vol-1/sync/bootstrap", newJSONBody(t, map[string]any{
		"snapshot_name": "bootstrap-a",
	}))
	if bootstrapRecorder.Code != 201 {
		t.Fatalf("bootstrap status = %d, want 201", bootstrapRecorder.Code)
	}
	bootstrapResp, apiErr, err := spec.DecodeResponse[syncBootstrapResponseBody](bootstrapRecorder.Body)
	if err != nil {
		t.Fatalf("decode bootstrap response: %v", err)
	}
	if apiErr != nil {
		t.Fatalf("unexpected bootstrap api error: %+v", apiErr)
	}
	if bootstrapResp.Snapshot.ID == "" || bootstrapResp.ReplayAfterSeq != 1 {
		t.Fatalf("bootstrap response = %+v, want snapshot id and replay_after_seq=1", bootstrapResp)
	}
	wantArchivePath := "/api/v1/sandboxvolumes/vol-1/sync/bootstrap/archive?snapshot_id=" + bootstrapResp.Snapshot.ID
	if bootstrapResp.ArchiveDownloadPath != wantArchivePath {
		t.Fatalf("archive_download_path = %q, want %q", bootstrapResp.ArchiveDownloadPath, wantArchivePath)
	}

	archiveRecorder := env.newAuthedRequest(t, "GET", "/sandboxvolumes/vol-1/sync/bootstrap/archive?snapshot_id="+bootstrapResp.Snapshot.ID, nil)
	if archiveRecorder.Code != 200 {
		t.Fatalf("archive status = %d, want 200", archiveRecorder.Code)
	}
	if got := archiveRecorder.Body.String(); got != "bootstrap-archive-body" {
		t.Fatalf("archive body = %q, want %q", got, "bootstrap-archive-body")
	}
	if env.snapshotMgr.lastExport == nil || env.snapshotMgr.lastExport.SnapshotID != bootstrapResp.Snapshot.ID {
		t.Fatalf("last export = %+v, want snapshot_id=%q", env.snapshotMgr.lastExport, bootstrapResp.Snapshot.ID)
	}
}

func TestSyncAPIBootstrapRejectsNamespaceIncompatibleSnapshotAndCleansUp(t *testing.T) {
	env := newStorageProxySyncTestEnv(t)
	env.createVolume(t, "vol-1")
	env.snapshotMgr.compatibilityIssues = []pathnorm.CompatibilityIssue{{
		Code:    pathnorm.IssueCodeWindowsReservedName,
		Path:    "/app/CON.txt",
		Segment: "CON.txt",
	}}

	bootstrapRecorder := env.newAuthedRequest(t, "POST", "/sandboxvolumes/vol-1/sync/bootstrap", newJSONBody(t, map[string]any{
		"capabilities": map[string]any{
			"case_sensitive":                    false,
			"unicode_normalization_insensitive": true,
			"windows_compatible_paths":          true,
		},
	}))
	if bootstrapRecorder.Code != 409 {
		t.Fatalf("bootstrap status = %d, want 409", bootstrapRecorder.Code)
	}
	_, apiErr, err := spec.DecodeResponse[syncBootstrapResponseBody](bootstrapRecorder.Body)
	if err != nil {
		t.Fatalf("decode bootstrap conflict response: %v", err)
	}
	if apiErr == nil || apiErr.Message == "" {
		t.Fatalf("api error = %+v, want namespace incompatibility error", apiErr)
	}
	details, ok := apiErr.Details.(map[string]any)
	if !ok {
		t.Fatalf("api error details = %#v, want object", apiErr.Details)
	}
	if details["reason"] != "namespace_incompatible" {
		t.Fatalf("details.reason = %#v, want namespace_incompatible", details["reason"])
	}
	if len(env.snapshotMgr.deletedSnapshotIDs) != 1 {
		t.Fatalf("deleted snapshots = %v, want one cleanup after rejected bootstrap", env.snapshotMgr.deletedSnapshotIDs)
	}
}

func TestSyncAPIConflictArtifactMaterializationWritesArtifactAndJournalEntry(t *testing.T) {
	env := newStorageProxySyncTestEnv(t)
	env.createVolume(t, "vol-1")

	volCtx := newMountedIntegrationVolumeContext(t, "vol-1", "team-1")
	env.sync.SetConflictArtifactWriter(volsync.NewConflictArtifactWriter(&integrationMountedVolumeManager{
		volumes: map[string]*volume.VolumeContext{
			"vol-1": volCtx,
		},
	}, logrus.New()))
	env.server = newStorageProxyTestServer(env)

	upsertRecorder := env.newAuthedRequest(t, "PUT", "/sandboxvolumes/vol-1/sync/replicas/replica-1", newJSONBody(t, map[string]any{
		"display_name":   "Laptop",
		"platform":       "linux",
		"root_path":      "/workspace",
		"case_sensitive": true,
	}))
	if upsertRecorder.Code != 200 {
		t.Fatalf("upsert status = %d, want 200", upsertRecorder.Code)
	}

	if err := env.sync.RecordRemoteChange(env.ctx, &volsync.RemoteChange{
		VolumeID:   "vol-1",
		TeamID:     "team-1",
		SandboxID:  "sandbox-1",
		EventType:  db.SyncEventWrite,
		Path:       "/app/main.go",
		OccurredAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("RecordRemoteChange() error = %v", err)
	}

	appendRecorder := env.newAuthedRequest(t, "POST", "/sandboxvolumes/vol-1/sync/replicas/replica-1/changes", newJSONBody(t, map[string]any{
		"request_id": "req-conflict-artifact-1",
		"base_seq":   0,
		"changes": []map[string]any{{
			"event_type": "write",
			"path":       "/app/main.go",
		}},
	}))
	if appendRecorder.Code != 200 {
		t.Fatalf("append status = %d, want 200", appendRecorder.Code)
	}
	appendResp, apiErr, err := spec.DecodeResponse[volsync.AppendChangesResponse](appendRecorder.Body)
	if err != nil {
		t.Fatalf("decode append response: %v", err)
	}
	if apiErr != nil {
		t.Fatalf("unexpected append api error: %+v", apiErr)
	}
	if len(appendResp.Accepted) != 0 || len(appendResp.Conflicts) != 1 {
		t.Fatalf("append response = %+v, want one conflict and no accepted changes", appendResp)
	}
	conflict := appendResp.Conflicts[0]
	if conflict.ArtifactPath == "" {
		t.Fatalf("conflict artifact_path = %q, want non-empty", conflict.ArtifactPath)
	}

	artifactBytes := readMountedFile(t, volCtx, conflict.ArtifactPath)
	var payload map[string]any
	if err := json.Unmarshal(artifactBytes, &payload); err != nil {
		t.Fatalf("unmarshal artifact payload: %v", err)
	}
	if payload["conflict_id"] != conflict.ID {
		t.Fatalf("artifact conflict_id = %#v, want %q", payload["conflict_id"], conflict.ID)
	}
	if payload["artifact_path"] != conflict.ArtifactPath {
		t.Fatalf("artifact path = %#v, want %q", payload["artifact_path"], conflict.ArtifactPath)
	}
	if payload["reason"] != conflict.Reason {
		t.Fatalf("artifact reason = %#v, want %q", payload["reason"], conflict.Reason)
	}

	listRecorder := env.newAuthedRequest(t, "GET", "/sandboxvolumes/vol-1/sync/changes?after=0", nil)
	if listRecorder.Code != 200 {
		t.Fatalf("list changes status = %d, want 200", listRecorder.Code)
	}
	listResp, apiErr, err := spec.DecodeResponse[volsync.ListChangesResponse](listRecorder.Body)
	if err != nil {
		t.Fatalf("decode list changes response: %v", err)
	}
	if apiErr != nil {
		t.Fatalf("unexpected list changes api error: %+v", apiErr)
	}
	if len(listResp.Changes) != 2 {
		t.Fatalf("changes = %+v, want remote write + artifact write", listResp.Changes)
	}
	last := listResp.Changes[len(listResp.Changes)-1]
	if last.Path != conflict.ArtifactPath {
		t.Fatalf("last journal path = %q, want %q", last.Path, conflict.ArtifactPath)
	}
}
