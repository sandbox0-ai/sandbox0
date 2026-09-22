package migrationstaging

import (
	"testing"

	"github.com/containerd/errdefs"
	"github.com/stretchr/testify/require"
)

func TestStagingQuotaRejectsUnenforcedChangedAndExhaustedBudgets(t *testing.T) {
	limits := Limits{ProjectID: 123, Bytes: 8 << 20, Inodes: 64}
	require.NoError(t, limits.Validate())
	good := snapshot{project: limits.ProjectID, inherit: true, enforced: true, hardBlocks: uint64(limits.Bytes / 512), hardInodes: limits.Inodes, freeBytes: uint64(limits.Bytes)}
	require.NoError(t, validate(good, limits, true))
	for _, mutate := range []func(*snapshot){
		func(s *snapshot) { s.enforced = false }, func(s *snapshot) { s.inherit = false },
		func(s *snapshot) { s.project++ }, func(s *snapshot) { s.hardBlocks = 0 },
		func(s *snapshot) { s.hardBlocks++ }, func(s *snapshot) { s.hardInodes = 0 },
	} {
		changed := good
		mutate(&changed)
		require.ErrorIs(t, validate(changed, limits, true), errdefs.ErrFailedPrecondition)
	}
	for _, mutate := range []func(*snapshot){
		func(s *snapshot) { s.blocks = s.hardBlocks }, func(s *snapshot) { s.inodes = s.hardInodes },
		func(s *snapshot) { s.freeBytes = 0 },
	} {
		full := good
		mutate(&full)
		require.ErrorIs(t, validate(full, limits, true), errdefs.ErrResourceExhausted)
		require.NoError(t, validate(full, limits, false), "recovery of a full pool remains possible")
	}
	for _, bad := range []Limits{{}, {ProjectID: 1, Bytes: 1<<20 + 1, Inodes: 64}, {ProjectID: 1, Bytes: 1 << 20, Inodes: 0}} {
		require.Error(t, bad.Validate())
	}
}

func TestStagingBudgetChecksQuantityAndPhysicalCapacity(t *testing.T) {
	good := snapshot{hardBlocks: 16, blocks: 4, hardInodes: 10, inodes: 3, freeBytes: 8192}
	require.NoError(t, admitBudget(good, 6144, 7))
	require.ErrorIs(t, admitBudget(good, 6145, 7), errdefs.ErrResourceExhausted)
	require.ErrorIs(t, admitBudget(good, 4096, 8), errdefs.ErrResourceExhausted)
	physical := good
	physical.freeBytes = 4095
	require.ErrorIs(t, admitBudget(physical, 4096, 1), errdefs.ErrResourceExhausted, "quota cannot manufacture free filesystem blocks")
	for _, change := range []func(*snapshot){
		func(s *snapshot) { s.blocks = s.hardBlocks + 1 },
		func(s *snapshot) { s.inodes = s.hardInodes + 1 },
	} {
		over := good
		change(&over)
		require.ErrorIs(t, admitBudget(over, 1, 1), errdefs.ErrResourceExhausted, "over-limit accounting cannot underflow into available capacity")
	}
	require.ErrorIs(t, admitBudget(good, -1, 1), errdefs.ErrInvalidArgument)
	require.ErrorIs(t, admitBudget(good, 1, 0), errdefs.ErrInvalidArgument)
	require.ErrorIs(t, admitBudget(good, 1<<50+1, 1), errdefs.ErrInvalidArgument)
}
