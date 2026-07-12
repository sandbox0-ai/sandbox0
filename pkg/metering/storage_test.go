package metering

import (
	"testing"
	"time"
)

func TestStorageWindowFromStateCarriesFractionalByteHours(t *testing.T) {
	start := time.Date(2026, 7, 12, 10, 0, 0, 0, time.UTC)
	state := &StorageProjectionState{
		SubjectType: SubjectTypeVolume,
		SubjectID:   "volume-1",
		SizeBytes:   1,
		ObservedAt:  start,
	}

	window, remainder := StorageWindowFromState(state, start.Add(30*time.Minute))
	if window != nil {
		t.Fatalf("first half-hour window = %#v, want nil", window)
	}
	if remainder != int64(30*time.Minute) {
		t.Fatalf("first remainder = %d, want %d", remainder, int64(30*time.Minute))
	}

	state.ObservedAt = start.Add(30 * time.Minute)
	state.UnbilledByteNanoseconds = remainder
	window, remainder = StorageWindowFromState(state, start.Add(time.Hour))
	if window == nil || window.Value != 1 || window.Unit != WindowUnitByteHours {
		t.Fatalf("second half-hour window = %#v, want one byte-hour", window)
	}
	if remainder != 0 {
		t.Fatalf("second remainder = %d, want 0", remainder)
	}
}

func TestStorageWindowFromStateUsesRootFSWindowType(t *testing.T) {
	start := time.Date(2026, 7, 12, 10, 0, 0, 0, time.UTC)
	state := &StorageProjectionState{
		SubjectType: SubjectTypeRootFS,
		SubjectID:   "sandbox-1",
		SizeBytes:   1024,
		ObservedAt:  start,
	}
	window, _ := StorageWindowFromState(state, start.Add(time.Hour))
	if window == nil || window.WindowType != WindowTypeSandboxRootFSByteHours || window.Value != 1024 {
		t.Fatalf("rootfs window = %#v", window)
	}
}
