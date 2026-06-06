package portal

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/s0fs"
)

func TestSandboxFilesystemHeadJSONRoundTrip(t *testing.T) {
	updatedAt := time.Date(2026, 6, 5, 20, 0, 0, 0, time.UTC)
	data, err := encodeSandboxFilesystemHead("fs-a", &s0fs.CommittedHead{
		VolumeID:      "fs-a",
		ManifestSeq:   3,
		CheckpointSeq: 2,
		ManifestKey:   "manifests/3.json",
		UpdatedAt:     updatedAt,
	})
	if err != nil {
		t.Fatalf("encodeSandboxFilesystemHead() error = %v", err)
	}
	var raw map[string]any
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}
	if raw["volume_id"] != "fs-a" {
		t.Fatalf("volume_id = %v, want fs-a", raw["volume_id"])
	}

	head, err := decodeSandboxFilesystemHead("fs-a", data)
	if err != nil {
		t.Fatalf("decodeSandboxFilesystemHead() error = %v", err)
	}
	if head.VolumeID != "fs-a" || head.ManifestSeq != 3 || head.CheckpointSeq != 2 || head.ManifestKey != "manifests/3.json" || !head.UpdatedAt.Equal(updatedAt) {
		t.Fatalf("decoded head = %+v", head)
	}
}

func TestSandboxFilesystemHeadEmptyState(t *testing.T) {
	for _, raw := range [][]byte{nil, []byte("{}"), []byte("null")} {
		head, err := decodeSandboxFilesystemHead("fs-a", raw)
		if err != nil {
			t.Fatalf("decodeSandboxFilesystemHead(%q) error = %v", string(raw), err)
		}
		if head != nil {
			t.Fatalf("decodeSandboxFilesystemHead(%q) = %+v, want nil", string(raw), head)
		}
	}
}

func TestSandboxFilesystemHeadCASRules(t *testing.T) {
	if !canAdvanceSandboxFilesystemHead(nil, 0, 1) {
		t.Fatal("expected first committed head insert to be allowed")
	}
	if canAdvanceSandboxFilesystemHead(nil, 1, 2) {
		t.Fatal("expected first committed head insert with non-zero expected seq to be rejected")
	}
	existing := &s0fs.CommittedHead{ManifestSeq: 3}
	if !canAdvanceSandboxFilesystemHead(existing, 3, 4) {
		t.Fatal("expected matching CAS advance to be allowed")
	}
	if canAdvanceSandboxFilesystemHead(existing, 2, 4) {
		t.Fatal("expected stale CAS advance to be rejected")
	}
	if canAdvanceSandboxFilesystemHead(existing, 3, 3) {
		t.Fatal("expected non-advancing CAS update to be rejected")
	}
}
