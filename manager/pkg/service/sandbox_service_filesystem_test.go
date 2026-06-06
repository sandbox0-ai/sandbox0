package service

import (
	"context"
	"errors"
	"testing"
)

type recordingSandboxFilesystemStore struct {
	acquireReqs []SandboxFilesystemAcquireRequest
	acquireResp *SandboxFilesystemRecord
	acquireErr  error
	releaseReqs []SandboxFilesystemReleaseRequest
	releaseErr  error
	deleteReqs  []SandboxFilesystemDeleteRequest
	deleteErr   error
}

func (s *recordingSandboxFilesystemStore) AcquireOwner(_ context.Context, req SandboxFilesystemAcquireRequest) (*SandboxFilesystemRecord, error) {
	s.acquireReqs = append(s.acquireReqs, req)
	if s.acquireErr != nil {
		return nil, s.acquireErr
	}
	return s.acquireResp, nil
}

func (s *recordingSandboxFilesystemStore) ReleaseOwner(_ context.Context, req SandboxFilesystemReleaseRequest) error {
	s.releaseReqs = append(s.releaseReqs, req)
	return s.releaseErr
}

func (s *recordingSandboxFilesystemStore) DeleteForSandbox(_ context.Context, req SandboxFilesystemDeleteRequest) error {
	s.deleteReqs = append(s.deleteReqs, req)
	return s.deleteErr
}

func TestAcquireClaimFilesystemUsesInternalStore(t *testing.T) {
	store := &recordingSandboxFilesystemStore{
		acquireResp: &SandboxFilesystemRecord{
			FilesystemID:    "fs-existing",
			TeamID:          "team-a",
			UserID:          "user-a",
			BaseImageRef:    "ubuntu@sha256:abc",
			BaseImageDigest: "sha256:abc",
		},
	}
	svc := &SandboxService{sandboxFilesystemStore: store}
	req := &ClaimRequest{
		TeamID:                 "team-a",
		UserID:                 "user-a",
		SandboxID:              "sandbox-a",
		RuntimeGeneration:      2,
		FilesystemID:           "fs-existing",
		FilesystemBaseImageRef: "ubuntu:latest",
	}

	if err := svc.acquireClaimFilesystem(context.Background(), req, nil); err != nil {
		t.Fatalf("acquireClaimFilesystem() error = %v", err)
	}
	if len(store.acquireReqs) != 1 {
		t.Fatalf("acquire requests = %d, want 1", len(store.acquireReqs))
	}
	got := store.acquireReqs[0]
	if got.FilesystemID != "fs-existing" || got.TeamID != "team-a" || got.OwnerSandboxID != "sandbox-a" || got.OwnerRuntimeGeneration != 2 {
		t.Fatalf("unexpected acquire request: %+v", got)
	}
	if got.LifecycleOwnerSandboxID != "sandbox-a" {
		t.Fatalf("lifecycle owner = %q, want sandbox-a", got.LifecycleOwnerSandboxID)
	}
	if req.FilesystemBaseImageRef != "ubuntu@sha256:abc" {
		t.Fatalf("base image ref = %q, want recorded digest ref", req.FilesystemBaseImageRef)
	}
	if req.FilesystemBaseImageDigest != "sha256:abc" {
		t.Fatalf("base image digest = %q, want sha256:abc", req.FilesystemBaseImageDigest)
	}
}

func TestAcquireClaimFilesystemPassesGeneratedFilesystemLifecycleOwner(t *testing.T) {
	store := &recordingSandboxFilesystemStore{}
	svc := &SandboxService{sandboxFilesystemStore: store}
	req := &ClaimRequest{
		TeamID:                            "team-a",
		UserID:                            "user-a",
		SandboxID:                         "sandbox-a",
		RuntimeGeneration:                 2,
		FilesystemID:                      "fs-generated",
		FilesystemLifecycleOwnerSandboxID: "sandbox-a",
	}

	if err := svc.acquireClaimFilesystem(context.Background(), req, nil); err != nil {
		t.Fatalf("acquireClaimFilesystem() error = %v", err)
	}
	if len(store.acquireReqs) != 1 {
		t.Fatalf("acquire requests = %d, want 1", len(store.acquireReqs))
	}
	if got := store.acquireReqs[0].LifecycleOwnerSandboxID; got != "sandbox-a" {
		t.Fatalf("lifecycle owner = %q, want sandbox-a", got)
	}
}

func TestAcquireClaimFilesystemMapsBusyOwnerToClaimConflict(t *testing.T) {
	store := &recordingSandboxFilesystemStore{acquireErr: ErrSandboxFilesystemBusy}
	svc := &SandboxService{sandboxFilesystemStore: store}
	req := &ClaimRequest{
		TeamID:            "team-a",
		SandboxID:         "sandbox-a",
		RuntimeGeneration: 1,
		FilesystemID:      "fs-busy",
	}

	err := svc.acquireClaimFilesystem(context.Background(), req, nil)
	if !errors.Is(err, ErrClaimConflict) {
		t.Fatalf("acquireClaimFilesystem() error = %v, want ErrClaimConflict", err)
	}
}

func TestAcquireClaimFilesystemMapsForeignFilesystemToInvalidClaim(t *testing.T) {
	store := &recordingSandboxFilesystemStore{acquireErr: ErrSandboxFilesystemForbidden}
	svc := &SandboxService{sandboxFilesystemStore: store}
	req := &ClaimRequest{
		TeamID:            "team-a",
		SandboxID:         "sandbox-a",
		RuntimeGeneration: 1,
		FilesystemID:      "fs-foreign",
	}

	err := svc.acquireClaimFilesystem(context.Background(), req, nil)
	if !errors.Is(err, ErrInvalidClaimRequest) {
		t.Fatalf("acquireClaimFilesystem() error = %v, want ErrInvalidClaimRequest", err)
	}
}

func TestCleanupDeletedSandboxReleasesFilesystemOwner(t *testing.T) {
	store := &recordingSandboxFilesystemStore{}
	svc := &SandboxService{sandboxFilesystemStore: store}

	err := svc.CleanupDeletedSandbox(context.Background(), SandboxLifecycleInfo{
		SandboxID:         "sandbox-a",
		FilesystemID:      "fs-a",
		RuntimeGeneration: 3,
	})
	if err != nil {
		t.Fatalf("CleanupDeletedSandbox() error = %v", err)
	}
	if len(store.releaseReqs) != 1 {
		t.Fatalf("release requests = %d, want 1", len(store.releaseReqs))
	}
	got := store.releaseReqs[0]
	if got.FilesystemID != "fs-a" || got.OwnerSandboxID != "sandbox-a" || got.OwnerRuntimeGeneration != 3 {
		t.Fatalf("unexpected release request: %+v", got)
	}
	if len(store.deleteReqs) != 1 {
		t.Fatalf("delete requests = %d, want 1", len(store.deleteReqs))
	}
	if got := store.deleteReqs[0]; got.FilesystemID != "fs-a" || got.LifecycleOwnerSandboxID != "sandbox-a" {
		t.Fatalf("unexpected delete request: %+v", got)
	}
}

func TestCleanupDeletedSandboxMarksCleanedAfterFilesystemOwnerRelease(t *testing.T) {
	filesystems := &recordingSandboxFilesystemStore{}
	store := &memorySandboxStore{records: map[string]*SandboxRecord{
		"sandbox-a": {
			ID:                "sandbox-a",
			Status:            "running",
			FilesystemID:      "fs-a",
			RuntimeGeneration: 3,
		},
	}}
	svc := &SandboxService{
		sandboxFilesystemStore: filesystems,
		sandboxStore:           store,
	}

	err := svc.CleanupDeletedSandbox(context.Background(), SandboxLifecycleInfo{
		SandboxID:             "sandbox-a",
		FilesystemID:          "fs-a",
		RuntimeGeneration:     3,
		RuntimeDeletionReason: runtimeDeletionReasonCleaned,
	})
	if err != nil {
		t.Fatalf("CleanupDeletedSandbox() error = %v", err)
	}
	if len(filesystems.releaseReqs) != 1 {
		t.Fatalf("release requests = %d, want 1", len(filesystems.releaseReqs))
	}
	if len(filesystems.deleteReqs) != 0 {
		t.Fatalf("delete requests = %d, want 0 for runtime clean", len(filesystems.deleteReqs))
	}
	if store.cleans != 1 {
		t.Fatalf("store cleans = %d, want 1", store.cleans)
	}
	if got := store.records["sandbox-a"].Status; got != SandboxStatusCleaned {
		t.Fatalf("status = %q, want cleaned", got)
	}
}

func TestCleanupDeletedSandboxDoesNotMarkCleanedWhenFilesystemOwnerReleaseFails(t *testing.T) {
	filesystems := &recordingSandboxFilesystemStore{releaseErr: errors.New("release failed")}
	store := &memorySandboxStore{records: map[string]*SandboxRecord{
		"sandbox-a": {
			ID:                "sandbox-a",
			Status:            "running",
			FilesystemID:      "fs-a",
			RuntimeGeneration: 3,
		},
	}}
	svc := &SandboxService{
		sandboxFilesystemStore: filesystems,
		sandboxStore:           store,
	}

	err := svc.CleanupDeletedSandbox(context.Background(), SandboxLifecycleInfo{
		SandboxID:             "sandbox-a",
		FilesystemID:          "fs-a",
		RuntimeGeneration:     3,
		RuntimeDeletionReason: runtimeDeletionReasonCleaned,
	})
	if err == nil {
		t.Fatal("CleanupDeletedSandbox() error = nil, want release failure")
	}
	if store.cleans != 0 {
		t.Fatalf("store cleans = %d, want 0", store.cleans)
	}
	if got := store.records["sandbox-a"].Status; got != "running" {
		t.Fatalf("status = %q, want running", got)
	}
}

func TestCleanupDeletedSandboxDoesNotDeleteFilesystemWhenOwnerReleaseFails(t *testing.T) {
	filesystems := &recordingSandboxFilesystemStore{releaseErr: errors.New("release failed")}
	svc := &SandboxService{sandboxFilesystemStore: filesystems}

	err := svc.CleanupDeletedSandbox(context.Background(), SandboxLifecycleInfo{
		SandboxID:         "sandbox-a",
		FilesystemID:      "fs-a",
		RuntimeGeneration: 3,
	})
	if err == nil {
		t.Fatal("CleanupDeletedSandbox() error = nil, want release failure")
	}
	if len(filesystems.releaseReqs) != 1 {
		t.Fatalf("release requests = %d, want 1", len(filesystems.releaseReqs))
	}
	if len(filesystems.deleteReqs) != 0 {
		t.Fatalf("delete requests = %d, want 0", len(filesystems.deleteReqs))
	}
}
