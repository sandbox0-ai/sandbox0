package runtimeslotreconciler

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestMigrationReservationRecoveryDoesNotBlockIndependentTerminalCleanup(t *testing.T) {
	fixture := newReconcileFixture(t, true)
	recoveryErr := errors.New("one migration reservation is unavailable")
	fixture.store.recoverReservations = func(ctx context.Context, limit int) (int, error) {
		if ctx.Err() != nil || limit != 10 {
			t.Fatalf("unbounded or canceled recovery pass: limit=%d err=%v", limit, ctx.Err())
		}
		deadline, ok := ctx.Deadline()
		if !ok || time.Until(deadline) > 5*time.Second {
			t.Fatalf("migration recovery must leave a bounded budget for physical cleanup")
		}
		*fixture.order = append(*fixture.order, "recover-reservations")
		return 2, recoveryErr
	}
	result, err := fixture.reconciler.RunOnce(t.Context())
	if !errors.Is(err, recoveryErr) || result.MigrationReservationsReleased != 2 || result.Completed != 1 {
		t.Fatalf("recovery must report partial progress and continue cleanup: result=%+v err=%v", result, err)
	}
	if (*fixture.order)[0] != "recover-reservations" || (*fixture.order)[1] != "list" {
		t.Fatalf("released carriers must be visible in the same terminal pass: %v", *fixture.order)
	}
}
