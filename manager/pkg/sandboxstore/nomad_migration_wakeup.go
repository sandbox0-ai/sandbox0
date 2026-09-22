package sandboxstore

import (
	"context"
	"fmt"
	"time"
)

const migrationDrainChannel = "sandbox0_runtime_migration_drain"

// WatchNomadMigrationDrains holds one dedicated pool connection until canceled
// or disconnected. LISTEN is committed before the initial wake, closing the
// subscription/read race. Reconnects must rescan too: NOTIFY is only a hint.
func (s *PGSandboxStore) WatchNomadMigrationDrains(ctx context.Context, wake func()) error {
	if s == nil || s.pool == nil || wake == nil {
		return fmt.Errorf("migration drain listener requires a store and wake callback")
	}
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	defer func() {
		// Never return a listening session to unrelated pool users, including
		// after cancellation when UNLISTEN on the caller context cannot run.
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = conn.Conn().Close(closeCtx)
	}()
	if _, err := conn.Exec(ctx, "LISTEN "+migrationDrainChannel); err != nil {
		return err
	}
	wake()
	for {
		if _, err := conn.Conn().WaitForNotification(ctx); err != nil {
			return err
		}
		wake()
	}
}
