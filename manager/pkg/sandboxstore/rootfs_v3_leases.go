package sandboxstore

import (
	"context"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/rootfshead"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfslease"
)

func (s *PGSandboxStore) EnsureRootFSCaptureLease(ctx context.Context, sandboxID, teamID string, generation int64) error {
	if s == nil || s.pool == nil {
		return nil
	}
	_, err := rootfslease.NewRepository(s.pool).EnsureCapture(ctx, sandboxID, teamID, generation)
	return err
}

func (s *PGSandboxStore) ReleaseRootFSCaptureLease(ctx context.Context, sandboxID, teamID string, generation int64) error {
	if s == nil || s.pool == nil {
		return nil
	}
	return rootfslease.NewRepository(s.pool).ReleaseCapture(ctx, sandboxID, teamID, generation)
}

func (s *PGSandboxStore) BeginRootFSCapture(ctx context.Context, sandboxID, teamID string, generation int64) error {
	if s == nil || s.pool == nil {
		return nil
	}
	return rootfslease.NewRepository(s.pool).BeginCapture(ctx, sandboxID, teamID, generation)
}

func (s *PGSandboxStore) CheckpointRootFSCapture(ctx context.Context, sandboxID, teamID string, generation int64, objects []rootfshead.Object) error {
	if s == nil || s.pool == nil {
		return nil
	}
	return rootfslease.NewRepository(s.pool).CheckpointCapture(ctx, sandboxID, teamID, generation, objects)
}

func (s *PGSandboxStore) ResetRootFSCapture(ctx context.Context, sandboxID, teamID string, generation int64) error {
	if s == nil || s.pool == nil {
		return nil
	}
	return rootfslease.NewRepository(s.pool).ResetCapture(ctx, sandboxID, teamID, generation)
}

func (s *PGSandboxStore) AcquireRootFSWriteLease(ctx context.Context, leaseID, teamID string, ttl time.Duration) error {
	if s == nil || s.pool == nil {
		return nil
	}
	_, err := rootfslease.NewRepository(s.pool).AcquireWrite(ctx, leaseID, teamID, ttl)
	return err
}

func (s *PGSandboxStore) ReleaseRootFSWriteLease(ctx context.Context, leaseID, teamID string) error {
	if s == nil || s.pool == nil {
		return nil
	}
	return rootfslease.NewRepository(s.pool).ReleaseWrite(ctx, leaseID, teamID)
}

func (s *PGSandboxStore) CleanupStaleRootFSWriteLeases(ctx context.Context) (int64, error) {
	if s == nil || s.pool == nil {
		return 0, nil
	}
	return rootfslease.NewRepository(s.pool).CleanupStale(ctx)
}

func (s *PGSandboxStore) DeleteUnknownRootFSObject(ctx context.Context, key, prefix string, deleter RootFSObjectDeleter) (bool, error) {
	if s == nil || s.pool == nil {
		return false, nil
	}
	return rootfslease.NewRepository(s.pool).DeleteUnknown(ctx, key, prefix, deleter)
}
