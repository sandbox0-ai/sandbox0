package nomadruntime

import (
	"context"
	"time"
)

// reclaimRetirementReserve advances only an already externally fenced writer
// that is blocking another retirement. Waiting for the next periodic scan can
// otherwise serialize a burst of deletions at the regional scan interval.
// The same journal exclusion, regional terminal authority and physical proof
// used by background recovery remain mandatory; this is only a scheduling hint.
func (d *nodeRuntime) reclaimRetirementReserve(ctx context.Context, parent string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if parent == "" || d.runtime == nil {
		return false, nil
	}
	sessions, err := d.runtime.RecoverySessions()
	if err != nil {
		return false, err
	}
	for _, session := range sessions {
		if session.Stage.Parent != parent {
			continue
		}
		if !session.ExternalCrash || session.BranchRemoved || session.Live || session.PressureOperationID != "" {
			return false, nil
		}
		key := recoveryInflightKey(session)
		if d.runtimeSlotCleanupPending(key) || d.reconciliationInFlight("pressure:"+parent) {
			return false, nil
		}
		// A failed authority request must not hold the caller's entire terminal
		// pass. At most one exact blocking owner is attempted per cleanup call.
		reclaimCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		if !d.beginReconciliation(key, cancel) {
			return false, nil
		}
		defer d.endReconciliation(key)
		return d.runtime.ReclaimExternallyRetired(reclaimCtx, session.Stage)
	}
	return false, nil
}
