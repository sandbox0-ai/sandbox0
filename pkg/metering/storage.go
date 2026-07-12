package metering

import (
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"time"
)

// StorageWindowFromState advances a storage projection state to end while
// carrying sub-byte-hour usage forward without losing precision.
func StorageWindowFromState(state *StorageProjectionState, end time.Time) (*Window, int64) {
	if state == nil {
		return nil, 0
	}
	remainder := normalizeStorageRemainder(state.UnbilledByteNanoseconds)
	end = end.UTC()
	start := state.ObservedAt.UTC()
	if !end.After(start) {
		return nil, remainder
	}
	value, remainder := storageByteHoursWithRemainder(state.SizeBytes, end.Sub(start), remainder)
	if value <= 0 {
		return nil, remainder
	}
	windowType := WindowTypeSandboxVolumeByteHours
	if state.SubjectType == SubjectTypeRootFS {
		windowType = WindowTypeSandboxRootFSByteHours
	}
	return &Window{
		WindowID:    fmt.Sprintf("storage/%s/%s/%d/%d", state.SubjectType, state.SubjectID, start.UnixNano(), end.UnixNano()),
		Producer:    ProducerStorage,
		RegionID:    state.RegionID,
		WindowType:  windowType,
		SubjectType: state.SubjectType,
		SubjectID:   state.SubjectID,
		TeamID:      state.TeamID,
		UserID:      state.UserID,
		SandboxID:   state.SandboxID,
		VolumeID:    state.VolumeID,
		SnapshotID:  state.SnapshotID,
		ClusterID:   state.ClusterID,
		WindowStart: start,
		WindowEnd:   end,
		Value:       value,
		Unit:        WindowUnitByteHours,
		Data:        storageWindowData(state, end.Sub(start)),
	}, remainder
}

func storageByteHoursWithRemainder(sizeBytes int64, duration time.Duration, previousRemainder int64) (int64, int64) {
	remainder := normalizeStorageRemainder(previousRemainder)
	if sizeBytes <= 0 || duration <= 0 {
		return 0, remainder
	}
	accumulator := big.NewInt(remainder)
	var usage big.Int
	usage.Mul(big.NewInt(sizeBytes), big.NewInt(duration.Nanoseconds()))
	accumulator.Add(accumulator, &usage)

	hourNanos := big.NewInt(int64(time.Hour))
	var quotient big.Int
	var modulo big.Int
	quotient.QuoRem(accumulator, hourNanos, &modulo)
	if !quotient.IsInt64() {
		return math.MaxInt64, 0
	}
	return quotient.Int64(), modulo.Int64()
}

func normalizeStorageRemainder(value int64) int64 {
	if value <= 0 {
		return 0
	}
	return value % int64(time.Hour)
}

func storageWindowData(state *StorageProjectionState, duration time.Duration) json.RawMessage {
	data := map[string]any{
		"product":               state.Product,
		"size_bytes":            state.SizeBytes,
		"duration_milliseconds": duration.Milliseconds(),
	}
	if state.OwnerKind != "" {
		data["owner_kind"] = state.OwnerKind
	}
	raw, err := json.Marshal(data)
	if err != nil {
		return json.RawMessage(`{}`)
	}
	return raw
}
