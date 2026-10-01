package session

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/containerd/errdefs"
	"github.com/stretchr/testify/require"
)

func TestLiveConsumerAdoptionHasOneWinnerAndCannotReviveExpiredLease(t *testing.T) {
	manager, _, stage := newTestManager(t, "adopt")
	_, err := manager.Ensure(t.Context(), stage)
	require.NoError(t, err)
	consumer := ConsumerRegistration{LeaseID: "old", ActiveKey: "task", ContainerID: "guest", StableMount: "/task/rootfs", HostMountNamespace: "mnt:[123]", RenewalProtocol: 1, OwnerProcess: "old-process", LeaseExpiresAt: time.Now().Add(time.Minute).Format(time.RFC3339Nano)}
	require.NoError(t, manager.RegisterConsumer(stage.Parent, stage.Identity, consumer))
	var won atomic.Int32
	var workers sync.WaitGroup
	for i := range 30 {
		workers.Go(func() {
			next := consumer
			next.LeaseID, next.OwnerProcess = fmt.Sprint(i), "successor-process"
			err := manager.AdoptConsumer(stage.Parent, stage.Identity, next, consumer.LeaseID)
			if err == nil {
				won.Add(1)
			} else {
				require.ErrorIs(t, err, errdefs.ErrFailedPrecondition)
			}
		})
	}
	workers.Wait()
	require.Equal(t, int32(1), won.Load())
	require.ErrorIs(t, manager.RenewConsumer(stage.Parent, stage.Identity, "old", time.Now().Add(time.Minute)), errdefs.ErrFailedPrecondition)
	current, err := manager.load(stage.Parent)
	require.NoError(t, err)
	require.ErrorIs(t, manager.RegisterConsumer(stage.Parent, stage.Identity, *current.Consumer), errdefs.ErrFailedPrecondition)
	current.Consumer.LeaseExpiresAt = time.Now().Add(-time.Second).Format(time.RFC3339Nano)
	require.NoError(t, manager.save(current))
	require.ErrorIs(t, manager.RenewConsumer(stage.Parent, stage.Identity, current.Consumer.LeaseID, time.Now().Add(time.Minute)), errdefs.ErrFailedPrecondition)
}
