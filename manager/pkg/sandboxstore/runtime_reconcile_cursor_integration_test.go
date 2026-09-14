package sandboxstore

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRuntimeSlotReconcileCursorAdvancesPastUnresolvedBatchIntegration(t *testing.T) {
	ctx := t.Context()
	pool := newSandboxStoreIntegrationPool(t)
	store := NewPGSandboxStore(pool)
	for i := range 105 {
		id := fmt.Sprintf("slot-%03d", i)
		_, err := registerRuntimeSlotWithTestCapacity(t, ctx, store, runtimeSlotTestRegistration(id, "alloc-"+id))
		require.NoError(t, err)
	}
	expired := time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)
	_, err := pool.Exec(ctx, `UPDATE manager.runtime_slots SET heartbeat_expires_at = $1`, expired)
	require.NoError(t, err)
	first, err := store.ListRuntimeSlotsForReconcileAfter(ctx, 100, nil)
	require.NoError(t, err)
	require.Len(t, first, 100)
	require.Equal(t, "slot-099", first[99].ID)

	// The cursor is an observed tuple, not a lookup of mutable current state.
	_, err = pool.Exec(ctx, `UPDATE manager.runtime_slots SET heartbeat_expires_at = NOW() + INTERVAL '1 minute' WHERE slot_id = $1`, first[99].ID)
	require.NoError(t, err)
	second, err := store.ListRuntimeSlotsForReconcileAfter(ctx, 100, &first[99])
	require.NoError(t, err)
	require.Len(t, second, 5)
	require.Equal(t, "slot-100", second[0].ID)
	require.Equal(t, "slot-104", second[4].ID)
	end, err := store.ListRuntimeSlotsForReconcileAfter(ctx, 100, &second[4])
	require.NoError(t, err)
	require.Empty(t, end)
	retry, err := store.ListRuntimeSlotsForReconcileAfter(ctx, 100, nil)
	require.NoError(t, err)
	require.Equal(t, first[0].ID, retry[0].ID)

	var terminal int
	require.NoError(t, pool.QueryRow(ctx, `SELECT COUNT(*) FROM manager.runtime_slots WHERE state = 'terminal'`).Scan(&terminal))
	require.Zero(t, terminal, "pagination cannot manufacture physical cleanup evidence")
}
