package sandboxstore

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestClaimAdmissionTurnCancellationDoesNotBlockAnotherTeamOrLaterClaim(t *testing.T) {
	queue := &claimAdmissionTurns{}
	first, err := queue.acquire(t.Context(), "team-1")
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	waiter := make(chan error, 1)
	go func() {
		release, err := queue.acquire(ctx, "team-1")
		if release != nil {
			release()
		}
		waiter <- err
	}()
	other, err := queue.acquire(t.Context(), "team-2")
	require.NoError(t, err)
	other()
	cancel()
	require.ErrorIs(t, <-waiter, context.Canceled)
	first()
	ctx, cancel = context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	next, err := queue.acquire(ctx, "team-1")
	require.NoError(t, err)
	next()
}
