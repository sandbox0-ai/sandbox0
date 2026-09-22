package nomadmigration

import (
	"context"
	"fmt"
	"time"
)

// Drain notifications accelerate discovery only. The ordinary scan remains
// active during listener outages and revalidates every notified transition.
type drainWatcher interface {
	WatchNomadMigrationDrains(context.Context, func()) error
}

func (c *Coordinator) watchDrains(ctx context.Context, report func(Report)) {
	for ctx.Err() == nil {
		err := c.drainWatcher.WatchNomadMigrationDrains(ctx, c.progress.notify)
		if ctx.Err() != nil {
			return
		}
		if report != nil {
			if err == nil {
				err = fmt.Errorf("migration drain listener stopped unexpectedly")
			}
			report(Report{Error: fmt.Errorf("migration drain notification unavailable: %w", err)})
		}
		timer := time.NewTimer(time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}
