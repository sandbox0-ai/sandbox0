package nomadruntime

import (
	"context"
	"errors"
	"testing"

	"github.com/sandbox0-ai/sandbox0/pkg/objectstore"
	"github.com/stretchr/testify/require"
)

type preparingRuntimeStore struct {
	objectstore.Store
	prepare func(context.Context) error
	create  func() error
}

func (s preparingRuntimeStore) PrepareCredentials(ctx context.Context) error { return s.prepare(ctx) }
func (s preparingRuntimeStore) Create() error                                { return s.create() }

func TestRuntimeObjectInitializationWaitsForCredentialsBeforeBucketAndReadiness(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	created := make(chan struct{}, 1)
	store := preparingRuntimeStore{
		prepare: func(ctx context.Context) error {
			close(entered)
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		},
		create: func() error { created <- struct{}{}; return nil },
	}
	done := make(chan error, 1)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go func() { done <- initializeRuntimeObjectStore(ctx, store) }()
	<-entered
	select {
	case <-created:
		t.Fatal("bucket initialization preceded credentials")
	default:
	}
	select {
	case <-done:
		t.Fatal("runtime initialization returned before credentials")
	default:
	}
	close(release)
	require.NoError(t, <-done)
	<-created
}

func TestRuntimeObjectInitializationFailsClosedBeforeBucketAccess(t *testing.T) {
	failure := errors.New("credential initialization failed")
	store := preparingRuntimeStore{
		prepare: func(context.Context) error { return failure },
		create:  func() error { t.Fatal("credential failure reached bucket initialization"); return nil },
	}
	require.ErrorIs(t, initializeRuntimeObjectStore(t.Context(), store), failure)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, initializeRuntimeObjectStore(ctx, store), context.Canceled)
	require.Error(t, initializeRuntimeObjectStore(nil, store))
	require.Error(t, initializeRuntimeObjectStore(t.Context(), nil))
}

func TestRuntimeObjectInitializationCancellationAfterPrepareStopsBucketAccess(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	store := preparingRuntimeStore{
		prepare: func(context.Context) error { cancel(); return nil },
		create:  func() error { t.Fatal("canceled initialization reached bucket"); return nil },
	}
	require.ErrorIs(t, initializeRuntimeObjectStore(ctx, store), context.Canceled)
}

func TestRuntimeObjectInitializationPreservesBucketErrors(t *testing.T) {
	failure := errors.New("bucket access denied")
	for _, err := range []error{nil, errors.New("BucketAlreadyOwnedByYou"), failure} {
		store := preparingRuntimeStore{prepare: func(context.Context) error { return nil }, create: func() error { return err }}
		got := initializeRuntimeObjectStore(t.Context(), store)
		if err == failure {
			require.ErrorIs(t, got, failure)
		} else {
			require.NoError(t, got)
		}
	}
}
