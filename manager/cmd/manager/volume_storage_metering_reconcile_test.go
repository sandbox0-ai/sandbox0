package main

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/db"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/s0fs"
)

type fakeVolumeStorageMeteringBackend struct {
	candidates []volumeStorageMeteringCandidate
	states     map[string]*s0fs.SnapshotState
	loadErrs   map[string]error
	recordErrs map[string]error
	recorded   []string
}

func (f *fakeVolumeStorageMeteringBackend) ListCandidates(context.Context) ([]volumeStorageMeteringCandidate, error) {
	return f.candidates, nil
}

func (f *fakeVolumeStorageMeteringBackend) LoadCurrentState(
	_ context.Context,
	candidate volumeStorageMeteringCandidate,
) (*s0fs.SnapshotState, *s0fs.Manifest, error) {
	if err := f.loadErrs[candidate.Volume.ID]; err != nil {
		return nil, nil, err
	}
	state := f.states[candidate.Volume.ID]
	return state, &s0fs.Manifest{
		VolumeID:    candidate.Volume.ID,
		ManifestSeq: candidate.Head.ManifestSeq,
		State:       state,
	}, nil
}

func (f *fakeVolumeStorageMeteringBackend) RecordCurrentState(
	_ context.Context,
	candidate volumeStorageMeteringCandidate,
	_ *s0fs.Manifest,
	_ int64,
) error {
	if err := f.recordErrs[candidate.Volume.ID]; err != nil {
		return err
	}
	f.recorded = append(f.recorded, candidate.Volume.ID)
	return nil
}

func TestReconcileVolumeStorageMeteringAudit(t *testing.T) {
	t.Parallel()
	projectedTen := int64(10)
	projectedZero := int64(0)
	backend := &fakeVolumeStorageMeteringBackend{
		candidates: []volumeStorageMeteringCandidate{
			{Volume: db.SandboxVolume{ID: "headless"}},
			reconcileTestCandidate("matched", &projectedTen),
			reconcileTestCandidate("mismatched", &projectedZero),
			reconcileTestCandidate("missing", nil),
		},
		states: map[string]*s0fs.SnapshotState{
			"matched":    reconcileTestState(10),
			"mismatched": reconcileTestState(20),
			"missing":    reconcileTestState(5),
		},
	}

	result, expected, err := reconcileVolumeStorageMetering(context.Background(), backend, false, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("reconcileVolumeStorageMetering() error = %v", err)
	}
	if result.TotalVolumes != 4 ||
		result.HeadlessVolumes != 1 ||
		result.MatchedVolumes != 1 ||
		result.MismatchedVolumes != 2 ||
		result.MissingProjectionVolumes != 1 ||
		result.UpdatedVolumes != 0 ||
		result.FailedVolumes != 0 {
		t.Fatalf("unexpected result: %+v", result)
	}
	if result.LogicalBytes != 35 {
		t.Fatalf("LogicalBytes = %d, want 35", result.LogicalBytes)
	}
	if result.ProjectedBytesBefore != 10 {
		t.Fatalf("ProjectedBytesBefore = %d, want 10", result.ProjectedBytesBefore)
	}
	if len(expected) != 3 || expected["mismatched"] != 20 || expected["missing"] != 5 {
		t.Fatalf("expected sizes = %#v", expected)
	}
	if len(backend.recorded) != 0 {
		t.Fatalf("audit recorded volumes: %v", backend.recorded)
	}
}

func TestReconcileVolumeStorageMeteringApply(t *testing.T) {
	t.Parallel()
	projectedTen := int64(10)
	projectedZero := int64(0)
	backend := &fakeVolumeStorageMeteringBackend{
		candidates: []volumeStorageMeteringCandidate{
			reconcileTestCandidate("matched", &projectedTen),
			reconcileTestCandidate("mismatched", &projectedZero),
			reconcileTestCandidate("missing", nil),
		},
		states: map[string]*s0fs.SnapshotState{
			"matched":    reconcileTestState(10),
			"mismatched": reconcileTestState(20),
			"missing":    reconcileTestState(5),
		},
	}

	result, _, err := reconcileVolumeStorageMetering(context.Background(), backend, true, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("reconcileVolumeStorageMetering() error = %v", err)
	}
	if result.UpdatedVolumes != 2 || result.MatchedVolumes != 1 {
		t.Fatalf("unexpected result: %+v", result)
	}
	if len(backend.recorded) != 2 ||
		backend.recorded[0] != "mismatched" ||
		backend.recorded[1] != "missing" {
		t.Fatalf("recorded volumes = %v", backend.recorded)
	}
}

func TestReconcileVolumeStorageMeteringReportsFailures(t *testing.T) {
	t.Parallel()
	loadErr := errors.New("manifest unavailable")
	backend := &fakeVolumeStorageMeteringBackend{
		candidates: []volumeStorageMeteringCandidate{
			reconcileTestCandidate("broken", nil),
		},
		loadErrs: map[string]error{"broken": loadErr},
	}
	var stderr bytes.Buffer

	result, _, err := reconcileVolumeStorageMetering(context.Background(), backend, true, &stderr)
	if !errors.Is(err, loadErr) {
		t.Fatalf("reconcileVolumeStorageMetering() error = %v, want %v", err, loadErr)
	}
	if result.FailedVolumes != 1 || result.UpdatedVolumes != 0 {
		t.Fatalf("unexpected result: %+v", result)
	}
	if stderr.Len() == 0 {
		t.Fatal("failure was not written to stderr")
	}
}

func reconcileTestCandidate(id string, projectedSize *int64) volumeStorageMeteringCandidate {
	return volumeStorageMeteringCandidate{
		Volume:             db.SandboxVolume{ID: id, TeamID: "team-1"},
		Head:               &s0fs.CommittedHead{VolumeID: id, ManifestSeq: 1},
		ProjectedSizeBytes: projectedSize,
	}
}

func reconcileTestState(size int) *s0fs.SnapshotState {
	return &s0fs.SnapshotState{
		Data: map[uint64][]byte{1: bytes.Repeat([]byte{'x'}, size)},
	}
}
