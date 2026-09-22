package nomadmigration

import (
	"context"
	"errors"
	"time"

	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
)

type prefetchStore interface {
	AuthorizeNomadSandboxMigrationImagePrefetch(context.Context, protocol.MigrationPublicationRequest, protocol.MigrationPublicationPlan) (*protocol.MigrationImagePrefetchRequest, error)
}

type prefetchNode interface {
	PlanMigrationPublication(context.Context, protocol.MigrationPublicationRequest) (*protocol.MigrationPublicationPlan, error)
	PrefetchMigrationImage(context.Context, protocol.MigrationImagePrefetchRequest) (*protocol.MigrationImagePrefetched, error)
}

// Once regional publication has completed, an optional cache acknowledgement
// must not consume the remaining operation deadline. Give an ordinary final
// cache sync time to finish, then let authorized preparation verify or recover
// the destination. This bounds only the observer; node-owned work retains its
// staging custody and is canceled/joined by normal preparation when necessary.
const migrationPrefetchCompletionGrace = 250 * time.Millisecond

// publishWithPrefetch overlaps a disposable target cache with the ordinary
// durable source publication. Only the latter's validated receipt is committed.
// Unsupported peers and failed cache fills retain the existing preparation path.
func (c *Coordinator) publishWithPrefetch(ctx context.Context, transfer *Transfer) (*protocol.MigrationPublication, error) {
	store, storeOK := c.store.(prefetchStore)
	node, nodeOK := c.node.(prefetchNode)
	if !storeOK || !nodeOK || transfer.Publication.DestinationPeerCertificateSHA256 == "" {
		return c.node.PublishMigration(ctx, transfer.Publication)
	}
	command := transfer.Prefetch
	if command == nil {
		plan, err := node.PlanMigrationPublication(ctx, transfer.Publication)
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			return c.node.PublishMigration(ctx, transfer.Publication)
		}
		if plan == nil || plan.ValidateFor(transfer.Publication) != nil {
			return nil, errors.New("source returned an invalid prospective image")
		}
		command, err = store.AuthorizeNomadSandboxMigrationImagePrefetch(ctx, transfer.Publication, *plan)
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			// Another replica may already have advanced or recovered publication.
			// The normal commit still validates its complete current authority.
			return c.node.PublishMigration(ctx, transfer.Publication)
		}
	}
	copy := *transfer
	copy.Prefetch = command
	if command == nil || copy.Validate() != nil {
		return nil, errors.New("prefetch command changed committed authority")
	}
	prefetchCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		// A cache acknowledgement is never evidence for a lifecycle transition.
		// Normal preparation revalidates local bytes or downloads the exact
		// published reference, including after this optional RPC fails.
		_, _ = node.PrefetchMigrationImage(prefetchCtx, *command)
	}()
	receipt, err := c.node.PublishMigration(ctx, transfer.Publication)
	if err == nil {
		timer := time.NewTimer(migrationPrefetchCompletionGrace)
		select {
		case <-done:
		case <-ctx.Done():
		case <-timer.C:
		}
		timer.Stop()
	}
	// Always join the observer before returning, including failed publication.
	// Its result never acknowledges publication or authorizes target execution.
	cancel()
	<-done
	return receipt, err
}
