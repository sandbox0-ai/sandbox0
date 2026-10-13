package nomadruntime

import (
	"context"
	"errors"
	"time"

	"github.com/containerd/errdefs"
	managerauthority "github.com/sandbox0-ai/sandbox0/manager/pkg/rootfswriterauthority"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	rootfssession "github.com/sandbox0-ai/sandbox0/pkg/rootfssession"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/rootfswriterauthority"
)

type liveCommitAuthorityClient interface {
	PublishLiveCommit(context.Context, rootfshandoff.StageRequest, managerauthority.PublishLiveCommitRequest) (protocol.LiveCommitProgress, error)
	InstallLiveCommit(context.Context, rootfshandoff.StageRequest, managerauthority.InstallLiveCommitRequest) (protocol.LiveCommitProgress, error)
	ObserveLiveCommit(context.Context, rootfshandoff.StageRequest) (protocol.LiveCommitObservation, error)
}

type liveCommitAuthorityAdapter struct{ client liveCommitAuthorityClient }

func (a liveCommitAuthorityAdapter) PublishLiveRootFS(ctx context.Context, stage rootfshandoff.StageRequest, p rootfssession.LiveCommitPublication) (protocol.LiveCommitProgress, error) {
	g := p.Generation
	return a.client.PublishLiveCommit(ctx, stage, managerauthority.PublishLiveCommitRequest{OperationID: p.OperationID, ExpectedCommittedGenerationID: p.ExpectedCommittedGenerationID,
		BaseGenerationID: p.BaseGenerationID, CutSequence: p.CutSequence, Generation: sandboxstore.RootFSGeneration{ID: g.GenerationID, FilesystemID: g.FilesystemID,
			ParentGenerationID: p.ExpectedCommittedGenerationID, SourceOCIDigest: g.SourceOCIDigest, BaseArtifactDigest: g.BaseArtifactDigest, BaseBlockRoot: g.BaseBlockRoot,
			CurrentBlockHead: g.CurrentBlockHead, WriterEpoch: g.WriterEpoch, FormatGeneration: g.FormatGeneration, DurabilityState: g.DurabilityState, LocatorVersion: g.LocatorVersion, Descriptor: g.Descriptor}})
}
func (a liveCommitAuthorityAdapter) InstallLiveRootFS(ctx context.Context, stage rootfshandoff.StageRequest, p rootfssession.LiveCommitInstallation) (protocol.LiveCommitProgress, error) {
	return a.client.InstallLiveCommit(ctx, stage, managerauthority.InstallLiveCommitRequest{OperationID: p.OperationID, GenerationID: p.GenerationID, CutSequence: p.CutSequence})
}
func (a liveCommitAuthorityAdapter) ObserveLiveCommit(ctx context.Context, stage rootfshandoff.StageRequest) (protocol.LiveCommitObservation, error) {
	return a.client.ObserveLiveCommit(ctx, stage)
}

type liveCommitRuntime interface {
	LiveCommitsEnabled() bool
	LiveCommitSessions(time.Time) ([]rootfssession.LiveCommitSession, error)
	CommitLiveRootFS(context.Context, rootfshandoff.StageRequest) error
}

func (r *rootfsRuntime) LiveCommitsEnabled() bool { return r != nil && r.liveCommits }
func (r *rootfsRuntime) LiveCommitSessions(now time.Time) ([]rootfssession.LiveCommitSession, error) {
	return r.sessions.LiveCommitSessions(now)
}
func (r *rootfsRuntime) CommitLiveRootFS(ctx context.Context, stage rootfshandoff.StageRequest) error {
	return r.sessions.CommitLive(ctx, stage)
}

const liveCommitConcurrency = 4

type liveCommitRetry struct {
	delay time.Duration
	next  time.Time
}

func (d *nodeRuntime) scanLiveCommits(ctx context.Context) {
	runtime, ok := d.runtime.(liveCommitRuntime)
	if !ok || !runtime.LiveCommitsEnabled() || ctx.Err() != nil {
		return
	}
	sessions, err := runtime.LiveCommitSessions(time.Now())
	if err != nil {
		d.logger.Error("list live RootFS commits", "error", err)
		return
	}
	d.mu.Lock()
	if d.liveCommitRetries == nil {
		d.liveCommitRetries = make(map[string]liveCommitRetry)
	}
	seen := make(map[string]bool, len(sessions))
	for _, session := range sessions {
		seen[session.Stage.Parent] = true
	}
	for parent := range d.liveCommitRetries {
		if !seen[parent] {
			delete(d.liveCommitRetries, parent)
		}
	}
	after := d.liveCommitAfter
	d.mu.Unlock()
	// Round-robin across the sorted snapshot so continuously dirty writers do
	// not monopolize the bounded worker lane.
	start := 0
	for i, session := range sessions {
		if session.Stage.Parent > after {
			start = i
			break
		}
	}
	for i := range sessions {
		session := sessions[(start+i)%len(sessions)]
		parent, key := session.Stage.Parent, "live-commit:"+session.Stage.Parent
		workCtx, cancel := context.WithTimeout(ctx, rootFSSessionReconcileTimeout)
		d.mu.Lock()
		if ctx.Err() != nil || d.liveCommitActive >= liveCommitConcurrency || time.Now().Before(d.liveCommitRetries[parent].next) || !d.beginReconciliationLocked(key, cancel) {
			d.mu.Unlock()
			cancel()
			continue
		}
		d.liveCommitActive++
		d.liveCommitAfter = parent
		d.wg.Add(1)
		d.mu.Unlock()
		go func() {
			defer d.wg.Done()
			defer cancel()
			err := runtime.CommitLiveRootFS(workCtx, session.Stage)
			d.mu.Lock()
			d.liveCommitActive--
			if err == nil {
				delete(d.liveCommitRetries, parent)
			} else {
				retry := d.liveCommitRetries[parent]
				retry.delay = min(rootfssession.LiveCommitInterval, max(time.Second, retry.delay*2))
				retry.next = time.Now().Add(retry.delay)
				d.liveCommitRetries[parent] = retry
			}
			d.endReconciliationLocked(key)
			d.mu.Unlock()
			if err != nil && !errors.Is(err, context.Canceled) && !errdefs.IsUnavailable(err) {
				d.logger.Error("commit live RootFS", "parent", parent, "error", err)
			}
		}()
	}
}
