package http

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/db"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/pathnorm"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/volsync"
	"github.com/sirupsen/logrus"
)

type fakeHTTPSyncManager struct {
	lastUpsert        *volsync.UpsertReplicaRequest
	lastListChanges   *volsync.ListChangesRequest
	lastAppend        *volsync.AppendChangesRequest
	lastUpdateCursor  *volsync.UpdateCursorRequest
	lastListConflicts *volsync.ListConflictsRequest
	lastResolve       *volsync.ResolveConflictRequest
	lastOpenReplay    *volsync.OpenReplayPayloadRequest
	head              int64
	listChangesErr    error
	appendErr         error
	updateCursorErr   error
	openReplayErr     error
	replayPayload     []byte
}

func (f *fakeHTTPSyncManager) UpsertReplica(ctx context.Context, req *volsync.UpsertReplicaRequest) (*volsync.ReplicaEnvelope, error) {
	f.lastUpsert = req
	return &volsync.ReplicaEnvelope{
		Replica: &db.SyncReplica{
			ID:             req.ReplicaID,
			VolumeID:       req.VolumeID,
			TeamID:         req.TeamID,
			DisplayName:    req.DisplayName,
			Platform:       req.Platform,
			RootPath:       req.RootPath,
			CaseSensitive:  req.CaseSensitive,
			Capabilities:   pathnorm.NormalizeFilesystemCapabilities(req.Platform, req.CaseSensitive, req.Capabilities),
			LastSeenAt:     time.Now().UTC(),
			LastAppliedSeq: 0,
		},
		HeadSeq: 7,
	}, nil
}

func (f *fakeHTTPSyncManager) GetReplica(ctx context.Context, volumeID, teamID, replicaID string) (*volsync.ReplicaEnvelope, error) {
	return &volsync.ReplicaEnvelope{
		Replica: &db.SyncReplica{
			ID:             replicaID,
			VolumeID:       volumeID,
			TeamID:         teamID,
			DisplayName:    "MacBook",
			Platform:       "darwin",
			RootPath:       "/tmp/work",
			CaseSensitive:  false,
			Capabilities:   pathnorm.DefaultFilesystemCapabilities("darwin", false),
			LastSeenAt:     time.Now().UTC(),
			LastAppliedSeq: 6,
		},
		HeadSeq: 9,
	}, nil
}

func (f *fakeHTTPSyncManager) GetHead(ctx context.Context, volumeID, teamID string) (int64, error) {
	if f.head == 0 {
		return 7, nil
	}
	return f.head, nil
}

func (f *fakeHTTPSyncManager) ListChanges(ctx context.Context, req *volsync.ListChangesRequest) (*volsync.ListChangesResponse, error) {
	f.lastListChanges = req
	if f.listChangesErr != nil {
		return nil, f.listChangesErr
	}
	return &volsync.ListChangesResponse{
		HeadSeq:          9,
		RetainedAfterSeq: 4,
		Changes: []*db.SyncJournalEntry{{
			Seq:           9,
			VolumeID:      req.VolumeID,
			TeamID:        req.TeamID,
			Source:        db.SyncSourceSandbox,
			EventType:     db.SyncEventWrite,
			Path:          "/app/main.go",
			EntryKind:     stringPtr("file"),
			Mode:          int64Ptr(0o644),
			ContentRef:    stringPtr("sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"),
			ContentSHA256: stringPtr("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"),
			SizeBytes:     int64Ptr(12),
		}},
	}, nil
}

func (f *fakeHTTPSyncManager) OpenReplayPayload(ctx context.Context, req *volsync.OpenReplayPayloadRequest) (io.ReadCloser, error) {
	f.lastOpenReplay = req
	if f.openReplayErr != nil {
		return nil, f.openReplayErr
	}
	return io.NopCloser(bytes.NewReader(f.replayPayload)), nil
}

func (f *fakeHTTPSyncManager) ListConflicts(ctx context.Context, req *volsync.ListConflictsRequest) (*volsync.ListConflictsResponse, error) {
	f.lastListConflicts = req
	return &volsync.ListConflictsResponse{
		Conflicts: []*db.SyncConflict{{
			ID:           "conflict-1",
			VolumeID:     req.VolumeID,
			TeamID:       req.TeamID,
			Path:         "/app/main.go",
			ArtifactPath: "/app/main.sandbox0-conflict-replica-1-seq-7.go",
			Reason:       "concurrent_update",
			Status:       db.SyncConflictStatusOpen,
			Metadata: mustRawJSON(tMustMarshalJSON(map[string]any{
				"latest_source":     db.SyncSourceSandbox,
				"latest_sandbox_id": "sandbox-1",
			})),
		}},
	}, nil
}

func (f *fakeHTTPSyncManager) ResolveConflict(ctx context.Context, req *volsync.ResolveConflictRequest) (*db.SyncConflict, error) {
	f.lastResolve = req
	return &db.SyncConflict{
		ID:           req.ConflictID,
		VolumeID:     req.VolumeID,
		TeamID:       req.TeamID,
		Path:         "/app/main.go",
		ArtifactPath: "/app/main.sandbox0-conflict-replica-1-seq-7.go",
		Reason:       "concurrent_update",
		Status:       req.Status,
	}, nil
}

func (f *fakeHTTPSyncManager) AppendReplicaChanges(ctx context.Context, req *volsync.AppendChangesRequest) (*volsync.AppendChangesResponse, error) {
	f.lastAppend = req
	if f.appendErr != nil {
		return nil, f.appendErr
	}
	return &volsync.AppendChangesResponse{
		HeadSeq: 8,
		Accepted: []*db.SyncJournalEntry{{
			Seq:       8,
			VolumeID:  req.VolumeID,
			TeamID:    req.TeamID,
			Source:    db.SyncSourceReplica,
			EventType: db.SyncEventWrite,
			Path:      "/app/main.go",
		}},
	}, nil
}

func (f *fakeHTTPSyncManager) UpdateReplicaCursor(ctx context.Context, req *volsync.UpdateCursorRequest) (*volsync.ReplicaEnvelope, error) {
	f.lastUpdateCursor = req
	if f.updateCursorErr != nil {
		return nil, f.updateCursorErr
	}
	return &volsync.ReplicaEnvelope{
		Replica: &db.SyncReplica{
			ID:             req.ReplicaID,
			VolumeID:       req.VolumeID,
			TeamID:         req.TeamID,
			LastAppliedSeq: req.LastAppliedSeq,
			LastSeenAt:     time.Now().UTC(),
		},
		HeadSeq: 9,
	}, nil
}

type fakeHTTPVolumeMutationBarrier struct {
	calls        int
	lastVolumeID string
}

func (f *fakeHTTPVolumeMutationBarrier) WithExclusive(ctx context.Context, volumeID string, fn func(context.Context) error) error {
	f.calls++
	f.lastVolumeID = volumeID
	return fn(ctx)
}

func tMustMarshalJSON(v any) []byte {
	payload, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return payload
}

func mustRawJSON(payload []byte) *json.RawMessage {
	raw := json.RawMessage(payload)
	return &raw
}

func stringPtr(value string) *string {
	return &value
}

func int64Ptr(value int64) *int64 {
	return &value
}

func TestUpsertSyncReplicaHandler(t *testing.T) {
	syncMgr := &fakeHTTPSyncManager{}
	server := &Server{
		logger:  logrus.New(),
		syncMgr: syncMgr,
	}

	req := httptest.NewRequest(http.MethodPut, "/sandboxvolumes/vol-1/sync/replicas/replica-1", bytes.NewBufferString(`{"display_name":"MacBook","platform":"darwin","root_path":"/tmp/work","case_sensitive":false,"capabilities":{"case_sensitive":false,"unicode_normalization_insensitive":true}}`))
	req.SetPathValue("id", "vol-1")
	req.SetPathValue("replica_id", "replica-1")
	req = req.WithContext(internalauth.WithClaims(req.Context(), &internalauth.Claims{
		TeamID: "team-1",
		UserID: "user-1",
	}))
	recorder := httptest.NewRecorder()

	server.upsertSyncReplica(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
	if syncMgr.lastUpsert == nil {
		t.Fatal("expected sync manager to be called")
	}
	if syncMgr.lastUpsert.TeamID != "team-1" || syncMgr.lastUpsert.VolumeID != "vol-1" || syncMgr.lastUpsert.ReplicaID != "replica-1" {
		t.Fatalf("unexpected upsert request: %+v", syncMgr.lastUpsert)
	}
	if syncMgr.lastUpsert.Capabilities == nil || syncMgr.lastUpsert.Capabilities.CaseSensitive {
		t.Fatalf("capabilities = %+v, want case-insensitive replica capabilities", syncMgr.lastUpsert.Capabilities)
	}

	resp, apiErr, err := spec.DecodeResponse[volsync.ReplicaEnvelope](recorder.Body)
	if err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if apiErr != nil {
		t.Fatalf("unexpected api error: %+v", apiErr)
	}
	if resp.HeadSeq != 7 {
		t.Fatalf("head seq = %d, want 7", resp.HeadSeq)
	}
}

func TestUpsertSyncReplicaHandlerRejectsMalformedCapabilities(t *testing.T) {
	syncMgr := &fakeHTTPSyncManager{}
	server := &Server{
		logger:  logrus.New(),
		syncMgr: syncMgr,
	}

	req := httptest.NewRequest(http.MethodPut, "/sandboxvolumes/vol-1/sync/replicas/replica-1", bytes.NewBufferString(`{"display_name":"MacBook","capabilities":"invalid"}`))
	req.SetPathValue("id", "vol-1")
	req.SetPathValue("replica_id", "replica-1")
	req = req.WithContext(internalauth.WithClaims(req.Context(), &internalauth.Claims{
		TeamID: "team-1",
		UserID: "user-1",
	}))
	recorder := httptest.NewRecorder()

	server.upsertSyncReplica(recorder, req)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusBadRequest)
	}
	if syncMgr.lastUpsert != nil {
		t.Fatalf("expected handler to reject before hitting sync manager: %+v", syncMgr.lastUpsert)
	}
}

func TestGetSyncReplicaHandler(t *testing.T) {
	syncMgr := &fakeHTTPSyncManager{}
	server := &Server{
		logger:  logrus.New(),
		syncMgr: syncMgr,
	}

	req := httptest.NewRequest(http.MethodGet, "/sandboxvolumes/vol-1/sync/replicas/replica-1", nil)
	req.SetPathValue("id", "vol-1")
	req.SetPathValue("replica_id", "replica-1")
	req = req.WithContext(internalauth.WithClaims(req.Context(), &internalauth.Claims{
		TeamID: "team-1",
		UserID: "user-1",
	}))
	recorder := httptest.NewRecorder()

	server.getSyncReplica(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}

	resp, apiErr, err := spec.DecodeResponse[volsync.ReplicaEnvelope](recorder.Body)
	if err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if apiErr != nil {
		t.Fatalf("unexpected api error: %+v", apiErr)
	}
	if resp.HeadSeq != 9 || resp.Replica == nil || resp.Replica.ID != "replica-1" || resp.Replica.LastAppliedSeq != 6 {
		t.Fatalf("response = %+v, want head_seq=9 replica_id=replica-1 last_applied_seq=6", resp)
	}
}

func TestAppendSyncChangesHandler(t *testing.T) {
	syncMgr := &fakeHTTPSyncManager{}
	server := &Server{
		logger:  logrus.New(),
		syncMgr: syncMgr,
	}

	body, err := json.Marshal(map[string]any{
		"request_id": "req-1",
		"base_seq":   3,
		"changes": []map[string]any{{
			"event_type": "write",
			"path":       "/app/main.go",
		}},
	})
	if err != nil {
		t.Fatalf("Marshal request: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/sandboxvolumes/vol-1/sync/replicas/replica-1/changes", bytes.NewReader(body))
	req.SetPathValue("id", "vol-1")
	req.SetPathValue("replica_id", "replica-1")
	req = req.WithContext(internalauth.WithClaims(req.Context(), &internalauth.Claims{
		TeamID: "team-1",
		UserID: "user-1",
	}))
	recorder := httptest.NewRecorder()

	server.appendSyncChanges(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
	if syncMgr.lastAppend == nil {
		t.Fatal("expected sync manager to be called")
	}
	if syncMgr.lastAppend.RequestID != "req-1" || syncMgr.lastAppend.BaseSeq != 3 || len(syncMgr.lastAppend.Changes) != 1 {
		t.Fatalf("unexpected append request: %+v", syncMgr.lastAppend)
	}
}

func TestAppendSyncChangesHandlerRejectsExpiredReplicaLease(t *testing.T) {
	syncMgr := &fakeHTTPSyncManager{appendErr: volsync.ErrReplicaLeaseExpired}
	server := &Server{
		logger:  logrus.New(),
		syncMgr: syncMgr,
	}

	body, err := json.Marshal(map[string]any{
		"request_id": "req-expired",
		"base_seq":   3,
		"changes": []map[string]any{{
			"event_type": "write",
			"path":       "/app/main.go",
		}},
	})
	if err != nil {
		t.Fatalf("Marshal request: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/sandboxvolumes/vol-1/sync/replicas/replica-1/changes", bytes.NewReader(body))
	req.SetPathValue("id", "vol-1")
	req.SetPathValue("replica_id", "replica-1")
	req = req.WithContext(internalauth.WithClaims(req.Context(), &internalauth.Claims{
		TeamID: "team-1",
		UserID: "user-1",
	}))
	recorder := httptest.NewRecorder()

	server.appendSyncChanges(recorder, req)

	if recorder.Code != http.StatusConflict {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusConflict)
	}
	_, apiErr, err := spec.DecodeResponse[volsync.AppendChangesResponse](recorder.Body)
	if err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if apiErr == nil || apiErr.Code != spec.CodeConflict {
		t.Fatalf("api error = %+v, want conflict", apiErr)
	}
}

func TestAppendSyncChangesHandlerRejectsMissingRequestID(t *testing.T) {
	syncMgr := &fakeHTTPSyncManager{}
	server := &Server{
		logger:  logrus.New(),
		syncMgr: syncMgr,
	}

	body, err := json.Marshal(map[string]any{
		"base_seq": 3,
		"changes": []map[string]any{{
			"event_type": "write",
			"path":       "/app/main.go",
		}},
	})
	if err != nil {
		t.Fatalf("Marshal request: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/sandboxvolumes/vol-1/sync/replicas/replica-1/changes", bytes.NewReader(body))
	req.SetPathValue("id", "vol-1")
	req.SetPathValue("replica_id", "replica-1")
	req = req.WithContext(internalauth.WithClaims(req.Context(), &internalauth.Claims{
		TeamID: "team-1",
		UserID: "user-1",
	}))
	recorder := httptest.NewRecorder()

	server.appendSyncChanges(recorder, req)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusBadRequest)
	}
	if syncMgr.lastAppend != nil {
		t.Fatalf("expected handler to reject before hitting sync manager: %+v", syncMgr.lastAppend)
	}
}

func TestAppendSyncChangesHandlerRejectsInvalidRequestID(t *testing.T) {
	syncMgr := &fakeHTTPSyncManager{appendErr: volsync.ErrInvalidRequestID}
	server := &Server{
		logger:  logrus.New(),
		syncMgr: syncMgr,
	}

	body, err := json.Marshal(map[string]any{
		"request_id": "bad request id",
		"base_seq":   3,
		"changes": []map[string]any{{
			"event_type": "write",
			"path":       "/app/main.go",
		}},
	})
	if err != nil {
		t.Fatalf("Marshal request: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/sandboxvolumes/vol-1/sync/replicas/replica-1/changes", bytes.NewReader(body))
	req.SetPathValue("id", "vol-1")
	req.SetPathValue("replica_id", "replica-1")
	req = req.WithContext(internalauth.WithClaims(req.Context(), &internalauth.Claims{
		TeamID: "team-1",
		UserID: "user-1",
	}))
	recorder := httptest.NewRecorder()

	server.appendSyncChanges(recorder, req)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusBadRequest)
	}
	_, apiErr, err := spec.DecodeResponse[volsync.AppendChangesResponse](recorder.Body)
	if err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if apiErr == nil || apiErr.Code != spec.CodeBadRequest || apiErr.Message != volsync.ErrInvalidRequestID.Error() {
		t.Fatalf("api error = %+v, want invalid request id bad request", apiErr)
	}
}

func TestUpdateSyncReplicaCursorHandler(t *testing.T) {
	syncMgr := &fakeHTTPSyncManager{}
	server := &Server{
		logger:  logrus.New(),
		syncMgr: syncMgr,
	}

	req := httptest.NewRequest(http.MethodPut, "/sandboxvolumes/vol-1/sync/replicas/replica-1/cursor", bytes.NewBufferString(`{"last_applied_seq":8}`))
	req.SetPathValue("id", "vol-1")
	req.SetPathValue("replica_id", "replica-1")
	req = req.WithContext(internalauth.WithClaims(req.Context(), &internalauth.Claims{
		TeamID: "team-1",
		UserID: "user-1",
	}))
	recorder := httptest.NewRecorder()

	server.updateSyncReplicaCursor(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
	if syncMgr.lastUpdateCursor == nil {
		t.Fatal("expected sync manager to be called")
	}
	if syncMgr.lastUpdateCursor.VolumeID != "vol-1" || syncMgr.lastUpdateCursor.ReplicaID != "replica-1" || syncMgr.lastUpdateCursor.LastAppliedSeq != 8 {
		t.Fatalf("unexpected cursor update request: %+v", syncMgr.lastUpdateCursor)
	}

	resp, apiErr, err := spec.DecodeResponse[volsync.ReplicaEnvelope](recorder.Body)
	if err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if apiErr != nil {
		t.Fatalf("unexpected api error: %+v", apiErr)
	}
	if resp.HeadSeq != 9 || resp.Replica == nil || resp.Replica.LastAppliedSeq != 8 {
		t.Fatalf("response = %+v, want head_seq=9 and last_applied_seq=8", resp)
	}
}

func TestUpdateSyncReplicaCursorHandlerRejectsRegression(t *testing.T) {
	syncMgr := &fakeHTTPSyncManager{updateCursorErr: volsync.ErrCursorRegression}
	server := &Server{
		logger:  logrus.New(),
		syncMgr: syncMgr,
	}

	req := httptest.NewRequest(http.MethodPut, "/sandboxvolumes/vol-1/sync/replicas/replica-1/cursor", bytes.NewBufferString(`{"last_applied_seq":2}`))
	req.SetPathValue("id", "vol-1")
	req.SetPathValue("replica_id", "replica-1")
	req = req.WithContext(internalauth.WithClaims(req.Context(), &internalauth.Claims{
		TeamID: "team-1",
		UserID: "user-1",
	}))
	recorder := httptest.NewRecorder()

	server.updateSyncReplicaCursor(recorder, req)

	if recorder.Code != http.StatusConflict {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusConflict)
	}
	_, apiErr, err := spec.DecodeResponse[volsync.ReplicaEnvelope](recorder.Body)
	if err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if apiErr == nil || apiErr.Code != spec.CodeConflict || apiErr.Message != volsync.ErrCursorRegression.Error() {
		t.Fatalf("api error = %+v, want cursor regression conflict", apiErr)
	}
}

func TestUpdateSyncReplicaCursorHandlerRejectsExpiredReplicaLease(t *testing.T) {
	syncMgr := &fakeHTTPSyncManager{updateCursorErr: volsync.ErrReplicaLeaseExpired}
	server := &Server{
		logger:  logrus.New(),
		syncMgr: syncMgr,
	}

	req := httptest.NewRequest(http.MethodPut, "/sandboxvolumes/vol-1/sync/replicas/replica-1/cursor", bytes.NewBufferString(`{"last_applied_seq":8}`))
	req.SetPathValue("id", "vol-1")
	req.SetPathValue("replica_id", "replica-1")
	req = req.WithContext(internalauth.WithClaims(req.Context(), &internalauth.Claims{
		TeamID: "team-1",
		UserID: "user-1",
	}))
	recorder := httptest.NewRecorder()

	server.updateSyncReplicaCursor(recorder, req)

	if recorder.Code != http.StatusConflict {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusConflict)
	}
	_, apiErr, err := spec.DecodeResponse[volsync.ReplicaEnvelope](recorder.Body)
	if err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if apiErr == nil || apiErr.Code != spec.CodeConflict || apiErr.Message != volsync.ErrReplicaLeaseExpired.Error() {
		t.Fatalf("api error = %+v, want expired replica conflict", apiErr)
	}
}

func TestListSyncConflictsHandler(t *testing.T) {
	syncMgr := &fakeHTTPSyncManager{}
	server := &Server{
		logger:  logrus.New(),
		syncMgr: syncMgr,
	}

	req := httptest.NewRequest(http.MethodGet, "/sandboxvolumes/vol-1/sync/conflicts?status=open&limit=10", nil)
	req.SetPathValue("id", "vol-1")
	req = req.WithContext(internalauth.WithClaims(req.Context(), &internalauth.Claims{
		TeamID: "team-1",
		UserID: "user-1",
	}))
	recorder := httptest.NewRecorder()

	server.listSyncConflicts(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
	if syncMgr.lastListConflicts == nil {
		t.Fatal("expected sync manager to be called")
	}
	if syncMgr.lastListConflicts.Status != "open" || syncMgr.lastListConflicts.Limit != 10 {
		t.Fatalf("unexpected list conflicts request: %+v", syncMgr.lastListConflicts)
	}
	resp, apiErr, err := spec.DecodeResponse[volsync.ListConflictsResponse](recorder.Body)
	if err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if apiErr != nil {
		t.Fatalf("unexpected api error: %+v", apiErr)
	}
	if len(resp.Conflicts) != 1 || resp.Conflicts[0].Metadata == nil {
		t.Fatalf("response conflicts = %+v, want one conflict with metadata", resp.Conflicts)
	}
	var metadata map[string]any
	if err := json.Unmarshal(*resp.Conflicts[0].Metadata, &metadata); err != nil {
		t.Fatalf("unmarshal metadata: %v", err)
	}
	if metadata["latest_source"] != db.SyncSourceSandbox || metadata["latest_sandbox_id"] != "sandbox-1" {
		t.Fatalf("metadata = %#v, want sandbox actor metadata", metadata)
	}
}

func TestListSyncChangesHandler(t *testing.T) {
	syncMgr := &fakeHTTPSyncManager{}
	server := &Server{
		logger:  logrus.New(),
		syncMgr: syncMgr,
	}

	req := httptest.NewRequest(http.MethodGet, "/sandboxvolumes/vol-1/sync/changes?after=4&limit=10", nil)
	req.SetPathValue("id", "vol-1")
	req = req.WithContext(internalauth.WithClaims(req.Context(), &internalauth.Claims{
		TeamID: "team-1",
		UserID: "user-1",
	}))
	recorder := httptest.NewRecorder()

	server.listSyncChanges(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
	if syncMgr.lastListChanges == nil {
		t.Fatal("expected sync manager to be called")
	}
	if syncMgr.lastListChanges.AfterSeq != 4 || syncMgr.lastListChanges.Limit != 10 {
		t.Fatalf("unexpected list changes request: %+v", syncMgr.lastListChanges)
	}

	resp, apiErr, err := spec.DecodeResponse[volsync.ListChangesResponse](recorder.Body)
	if err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if apiErr != nil {
		t.Fatalf("unexpected api error: %+v", apiErr)
	}
	if resp.HeadSeq != 9 || resp.RetainedAfterSeq != 4 || len(resp.Changes) != 1 || resp.Changes[0].Path != "/app/main.go" {
		t.Fatalf("response = %+v, want head=9 retained_after=4 one /app/main.go change", resp)
	}
	if resp.Changes[0].EntryKind == nil || *resp.Changes[0].EntryKind != "file" {
		t.Fatalf("entry kind = %#v, want file", resp.Changes[0].EntryKind)
	}
	if resp.Changes[0].ContentRef == nil || *resp.Changes[0].ContentRef == "" {
		t.Fatalf("content ref = %#v, want non-empty", resp.Changes[0].ContentRef)
	}
}

func TestListSyncChangesHandlerRejectsReseedRequired(t *testing.T) {
	syncMgr := &fakeHTTPSyncManager{
		listChangesErr: &volsync.ReseedRequiredError{
			RetainedAfterSeq: 5,
			HeadSeq:          12,
		},
	}
	server := &Server{
		logger:  logrus.New(),
		syncMgr: syncMgr,
	}

	req := httptest.NewRequest(http.MethodGet, "/sandboxvolumes/vol-1/sync/changes?after=2&limit=10", nil)
	req.SetPathValue("id", "vol-1")
	req = req.WithContext(internalauth.WithClaims(req.Context(), &internalauth.Claims{
		TeamID: "team-1",
		UserID: "user-1",
	}))
	recorder := httptest.NewRecorder()

	server.listSyncChanges(recorder, req)

	if recorder.Code != http.StatusConflict {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusConflict)
	}
	_, apiErr, err := spec.DecodeResponse[volsync.ListChangesResponse](recorder.Body)
	if err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if apiErr == nil || apiErr.Code != spec.CodeConflict || apiErr.Message != volsync.ErrReseedRequired.Error() {
		t.Fatalf("api error = %+v, want reseed-required conflict", apiErr)
	}
	details, ok := apiErr.Details.(map[string]any)
	if !ok {
		t.Fatalf("details = %#v, want object", apiErr.Details)
	}
	if details["reason"] != "reseed_required" {
		t.Fatalf("reason = %#v, want reseed_required", details["reason"])
	}
	if details["retained_after_seq"] != float64(5) || details["head_seq"] != float64(12) {
		t.Fatalf("details = %#v, want retained_after_seq=5 head_seq=12", details)
	}
}

func TestDownloadSyncReplayPayloadHandler(t *testing.T) {
	syncMgr := &fakeHTTPSyncManager{replayPayload: []byte("sandbox-body\n")}
	server := &Server{
		logger:  logrus.New(),
		syncMgr: syncMgr,
	}

	req := httptest.NewRequest(http.MethodGet, "/sandboxvolumes/vol-1/sync/replay-payload?content_ref=sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", nil)
	req.SetPathValue("id", "vol-1")
	req = req.WithContext(internalauth.WithClaims(req.Context(), &internalauth.Claims{
		TeamID: "team-1",
		UserID: "user-1",
	}))
	recorder := httptest.NewRecorder()

	server.downloadSyncReplayPayload(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
	if got := recorder.Body.String(); got != "sandbox-body\n" {
		t.Fatalf("body = %q, want %q", got, "sandbox-body\n")
	}
	if syncMgr.lastOpenReplay == nil || syncMgr.lastOpenReplay.ContentRef == "" {
		t.Fatalf("open replay request = %+v, want content_ref", syncMgr.lastOpenReplay)
	}
}

func TestDownloadSyncReplayPayloadHandlerRejectsInvalidRef(t *testing.T) {
	syncMgr := &fakeHTTPSyncManager{openReplayErr: volsync.ErrInvalidReplayPayloadRef}
	server := &Server{
		logger:  logrus.New(),
		syncMgr: syncMgr,
	}

	req := httptest.NewRequest(http.MethodGet, "/sandboxvolumes/vol-1/sync/replay-payload?content_ref=bad-ref", nil)
	req.SetPathValue("id", "vol-1")
	req = req.WithContext(internalauth.WithClaims(req.Context(), &internalauth.Claims{
		TeamID: "team-1",
		UserID: "user-1",
	}))
	recorder := httptest.NewRecorder()

	server.downloadSyncReplayPayload(recorder, req)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusBadRequest)
	}
}

func TestCreateSyncBootstrapHandler(t *testing.T) {
	syncMgr := &fakeHTTPSyncManager{head: 12}
	snapshotMgr := &fakeHTTPSnapshotManager{}
	barrier := &fakeHTTPVolumeMutationBarrier{}
	server := &Server{
		logger:      logrus.New(),
		syncMgr:     syncMgr,
		snapshotMgr: snapshotMgr,
		barrier:     barrier,
	}

	req := httptest.NewRequest(http.MethodPost, "/sandboxvolumes/vol-1/sync/bootstrap", bytes.NewBufferString(`{"snapshot_name":"bootstrap-a","snapshot_description":"seed local replica"}`))
	req.SetPathValue("id", "vol-1")
	req = req.WithContext(internalauth.WithClaims(req.Context(), &internalauth.Claims{
		TeamID: "team-1",
		UserID: "user-1",
	}))
	recorder := httptest.NewRecorder()

	server.createSyncBootstrap(recorder, req)

	if recorder.Code != http.StatusCreated {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusCreated)
	}

	resp, apiErr, err := spec.DecodeResponse[syncBootstrapResponse](recorder.Body)
	if err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if apiErr != nil {
		t.Fatalf("unexpected api error: %+v", apiErr)
	}
	if resp.ReplayAfterSeq != 12 {
		t.Fatalf("replay_after_seq = %d, want 12", resp.ReplayAfterSeq)
	}
	if resp.ArchiveDownloadPath != "/api/v1/sandboxvolumes/vol-1/sync/bootstrap/archive?snapshot_id=snap-1" {
		t.Fatalf("archive_download_path = %q", resp.ArchiveDownloadPath)
	}
	if barrier.calls != 1 || barrier.lastVolumeID != "vol-1" {
		t.Fatalf("barrier = %+v, want one exclusive call for vol-1", barrier)
	}
}

func TestCreateSyncBootstrapHandlerAuditsPortableCapabilities(t *testing.T) {
	syncMgr := &fakeHTTPSyncManager{head: 12}
	snapshotMgr := &fakeHTTPSnapshotManager{}
	server := &Server{
		logger:      logrus.New(),
		syncMgr:     syncMgr,
		snapshotMgr: snapshotMgr,
	}

	req := httptest.NewRequest(http.MethodPost, "/sandboxvolumes/vol-1/sync/bootstrap", bytes.NewBufferString(`{"snapshot_name":"bootstrap-a","snapshot_description":"seed local replica","capabilities":{"case_sensitive":false,"unicode_normalization_insensitive":true,"windows_compatible_paths":true}}`))
	req.SetPathValue("id", "vol-1")
	req = req.WithContext(internalauth.WithClaims(req.Context(), &internalauth.Claims{
		TeamID: "team-1",
		UserID: "user-1",
	}))
	recorder := httptest.NewRecorder()

	server.createSyncBootstrap(recorder, req)

	if recorder.Code != http.StatusCreated {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusCreated)
	}
	if snapshotMgr.lastCompatibility == nil {
		t.Fatal("expected snapshot compatibility audit to run")
	}
	if snapshotMgr.lastCompatibility.VolumeID != "vol-1" || snapshotMgr.lastCompatibility.SnapshotID != "snap-1" {
		t.Fatalf("unexpected compatibility request: %+v", snapshotMgr.lastCompatibility)
	}
	if !snapshotMgr.lastCompatibility.Capabilities.WindowsCompatiblePaths || snapshotMgr.lastCompatibility.Capabilities.CaseSensitive {
		t.Fatalf("capabilities = %+v, want portable case-insensitive capabilities", snapshotMgr.lastCompatibility.Capabilities)
	}
}

func TestCreateSyncBootstrapHandlerRejectsMalformedCapabilities(t *testing.T) {
	syncMgr := &fakeHTTPSyncManager{}
	snapshotMgr := &fakeHTTPSnapshotManager{}
	server := &Server{
		logger:      logrus.New(),
		syncMgr:     syncMgr,
		snapshotMgr: snapshotMgr,
	}

	req := httptest.NewRequest(http.MethodPost, "/sandboxvolumes/vol-1/sync/bootstrap", bytes.NewBufferString(`{"snapshot_name":"bootstrap-a","capabilities":"invalid"}`))
	req.SetPathValue("id", "vol-1")
	req = req.WithContext(internalauth.WithClaims(req.Context(), &internalauth.Claims{
		TeamID: "team-1",
		UserID: "user-1",
	}))
	recorder := httptest.NewRecorder()

	server.createSyncBootstrap(recorder, req)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusBadRequest)
	}
	if snapshotMgr.lastCreate != nil {
		t.Fatalf("expected handler to reject before hitting snapshot manager: %+v", snapshotMgr.lastCreate)
	}
}

func TestCreateSyncBootstrapHandlerRejectsCasefoldCollisionsForCaseInsensitiveReplica(t *testing.T) {
	syncMgr := &fakeHTTPSyncManager{head: 12}
	snapshotMgr := &fakeHTTPSnapshotManager{
		compatibilityIssues: []pathnorm.CompatibilityIssue{
			pathnorm.BuildCasefoldCollisionIssue("/app/main.go", []string{"/app/Main.go", "/app/main.go"}),
		},
	}
	server := &Server{
		logger:      logrus.New(),
		syncMgr:     syncMgr,
		snapshotMgr: snapshotMgr,
	}

	req := httptest.NewRequest(http.MethodPost, "/sandboxvolumes/vol-1/sync/bootstrap", bytes.NewBufferString(`{"snapshot_name":"bootstrap-a","snapshot_description":"seed local replica","case_sensitive":false}`))
	req.SetPathValue("id", "vol-1")
	req = req.WithContext(internalauth.WithClaims(req.Context(), &internalauth.Claims{
		TeamID: "team-1",
		UserID: "user-1",
	}))
	recorder := httptest.NewRecorder()

	server.createSyncBootstrap(recorder, req)

	if recorder.Code != http.StatusConflict {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusConflict)
	}
	_, apiErr, err := spec.DecodeResponse[syncBootstrapResponse](recorder.Body)
	if err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if apiErr == nil {
		t.Fatal("expected api error")
	}
	if apiErr.Code != spec.CodeConflict {
		t.Fatalf("api error code = %q, want %q", apiErr.Code, spec.CodeConflict)
	}
	details, ok := apiErr.Details.(map[string]any)
	if !ok {
		t.Fatalf("error details type = %T, want map[string]any", apiErr.Details)
	}
	if details["reason"] != "namespace_incompatible" {
		t.Fatalf("reason = %#v, want namespace_incompatible", details["reason"])
	}
	if details["snapshot_id"] != "snap-1" {
		t.Fatalf("snapshot_id = %#v, want snap-1", details["snapshot_id"])
	}
	issues, ok := details["issues"].([]any)
	if !ok || len(issues) != 1 {
		t.Fatalf("issues = %#v, want one compatibility issue", details["issues"])
	}
	if len(snapshotMgr.deletedSnapshot) != 1 || snapshotMgr.deletedSnapshot[0] != "snap-1" {
		t.Fatalf("deleted snapshots = %v, want [snap-1]", snapshotMgr.deletedSnapshot)
	}
}

func TestDownloadSyncBootstrapArchiveHandler(t *testing.T) {
	snapshotMgr := &fakeHTTPSnapshotManager{exportBody: []byte("archive-bytes")}
	server := &Server{
		logger:      logrus.New(),
		snapshotMgr: snapshotMgr,
	}

	req := httptest.NewRequest(http.MethodGet, "/sandboxvolumes/vol-1/sync/bootstrap/archive?snapshot_id=snap-1", nil)
	req.SetPathValue("id", "vol-1")
	req = req.WithContext(internalauth.WithClaims(req.Context(), &internalauth.Claims{
		TeamID: "team-1",
		UserID: "user-1",
	}))
	recorder := httptest.NewRecorder()

	server.downloadSyncBootstrapArchive(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
	if got := recorder.Header().Get("Content-Type"); got != "application/gzip" {
		t.Fatalf("content-type = %q, want application/gzip", got)
	}
	if snapshotMgr.lastExport == nil {
		t.Fatal("expected snapshot archive export to be called")
	}
	if snapshotMgr.lastExport.VolumeID != "vol-1" || snapshotMgr.lastExport.SnapshotID != "snap-1" || snapshotMgr.lastExport.TeamID != "team-1" {
		t.Fatalf("unexpected export request: %+v", snapshotMgr.lastExport)
	}
	if recorder.Body.String() != "archive-bytes" {
		t.Fatalf("body = %q, want %q", recorder.Body.String(), "archive-bytes")
	}
}

func TestResolveSyncConflictHandler(t *testing.T) {
	syncMgr := &fakeHTTPSyncManager{}
	server := &Server{
		logger:  logrus.New(),
		syncMgr: syncMgr,
	}

	req := httptest.NewRequest(http.MethodPut, "/sandboxvolumes/vol-1/sync/conflicts/conflict-1", bytes.NewBufferString(`{"status":"resolved","resolution":"keep_remote","note":"accepted remote version"}`))
	req.SetPathValue("id", "vol-1")
	req.SetPathValue("conflict_id", "conflict-1")
	req = req.WithContext(internalauth.WithClaims(req.Context(), &internalauth.Claims{
		TeamID: "team-1",
		UserID: "user-1",
	}))
	recorder := httptest.NewRecorder()

	server.resolveSyncConflict(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
	if syncMgr.lastResolve == nil || syncMgr.lastResolve.Status != db.SyncConflictStatusResolved {
		t.Fatalf("unexpected resolve request: %+v", syncMgr.lastResolve)
	}
}
