package session

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/containerd/errdefs"
	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/rootfswriterauthority"
	"github.com/stretchr/testify/require"
)

type nodeLiveAuthority struct {
	mu               sync.Mutex
	progress         protocol.LiveCommitProgress
	state            string
	published        []LiveCommitPublication
	installed        []LiveCommitInstallation
	lostPublish      bool
	lostInstall      bool
	failObserve      bool
	beforePublish    func(LiveCommitPublication)
	beforeInstall    func(LiveCommitInstallation)
	descriptorDigest string
}

func (a *nodeLiveAuthority) ObserveLiveCommit(_ context.Context, _ rootfshandoff.StageRequest) (protocol.LiveCommitObservation, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.failObserve {
		return protocol.LiveCommitObservation{}, errdefs.ErrUnavailable
	}
	return protocol.LiveCommitObservation{WriterState: a.state, Progress: a.progress, CommittedDescriptorDigest: a.descriptorDigest}, nil
}
func (a *nodeLiveAuthority) PublishLiveRootFS(_ context.Context, _ rootfshandoff.StageRequest, p LiveCommitPublication) (protocol.LiveCommitProgress, error) {
	if a.beforePublish != nil {
		a.beforePublish(p)
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.published = append(a.published, p)
	a.progress.CommittedGenerationID, a.progress.CommittedSequence, a.progress.OperationID = p.Generation.GenerationID, p.CutSequence, p.OperationID
	a.descriptorDigest = digest.FromBytes(p.Generation.Descriptor).String()
	if a.lostPublish {
		a.lostPublish = false
		return protocol.LiveCommitProgress{}, errdefs.ErrUnavailable
	}
	return a.progress, nil
}
func (a *nodeLiveAuthority) InstallLiveRootFS(_ context.Context, _ rootfshandoff.StageRequest, p LiveCommitInstallation) (protocol.LiveCommitProgress, error) {
	if a.beforeInstall != nil {
		a.beforeInstall(p)
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.installed = append(a.installed, p)
	a.progress.InstalledGenerationID, a.progress.InstalledSequence = p.GenerationID, p.CutSequence
	if a.lostInstall {
		a.lostInstall = false
		return protocol.LiveCommitProgress{}, errdefs.ErrUnavailable
	}
	return a.progress, nil
}

func newNodeLiveFixture(t *testing.T, name string) (*Manager, *nodeLiveAuthority, rootfshandoff.StageRequest, *rootfsblock.Branch) {
	t.Helper()
	m, _, stage := newTestManager(t, name)
	_, err := m.Ensure(t.Context(), stage)
	require.NoError(t, err)
	a := &nodeLiveAuthority{state: "consumed", progress: protocol.LiveCommitProgress{CommittedGenerationID: stage.InitialGeneration, InstalledGenerationID: stage.InitialGeneration}}
	m.liveCommitAuthority = a
	return m, a, stage, m.live[stage.Parent].branch
}

func TestNodeLiveCommitLostRepliesPreserveSuffixAndDurableInstallation(t *testing.T) {
	m, a, stage, branch := newNodeLiveFixture(t, "live-replies")
	initial, err := m.load(stage.Parent)
	require.NoError(t, err)
	_, err = branch.WriteAt(bytes.Repeat([]byte{11}, rootfsblock.LogicalBlockSize), 0)
	require.NoError(t, err)
	a.lostPublish = true
	a.beforePublish = func(p LiveCommitPublication) {
		_, writeErr := branch.WriteAt(bytes.Repeat([]byte{22}, rootfsblock.LogicalBlockSize), 0)
		require.NoError(t, writeErr)
		_, writeErr = branch.WriteAt([]byte("suffix"), rootfsblock.LogicalBlockSize)
		require.NoError(t, writeErr)
	}
	require.ErrorIs(t, m.CommitLive(t.Context(), stage), errdefs.ErrUnavailable)
	pending, err := m.load(stage.Parent)
	require.NoError(t, err)
	require.False(t, pending.LiveCommit.Pending.RemoteConfirmed)
	require.Equal(t, initial.BranchPath, pending.BranchPath)
	payload := append([]byte(nil), pending.LiveCommit.Pending.Generation.Descriptor...)
	a.beforeInstall = func(p LiveCommitInstallation) {
		stored, loadErr := m.load(stage.Parent)
		require.NoError(t, loadErr)
		require.Equal(t, p.CutSequence, stored.BranchSequenceBase)
		require.Equal(t, stored.BranchSequenceBase, branch.Identity().SequenceBase)
		require.Equal(t, p.GenerationID, stored.LiveCommit.InstalledGenerationID)
	}
	a.lostInstall = true
	require.ErrorIs(t, m.CommitLive(t.Context(), stage), errdefs.ErrUnavailable)
	rotated, err := m.load(stage.Parent)
	require.NoError(t, err)
	require.Equal(t, uint64(1), rotated.BranchSequenceBase)
	require.Equal(t, payload, rotated.BaseDescriptor)
	require.Len(t, a.published, 1, "a lost publication reply is resolved by observation, never rebuilt/re-uploaded")
	require.Equal(t, int64(2*rootfsblock.LogicalBlockSize), m.NodeDirtyTailUsage().UsedBytes)
	require.ErrorIs(t, m.BeginRetire(stage.Parent, stage.Identity, "retire"), errdefs.ErrUnavailable, "installation acknowledgement must precede terminal publication")
	require.NoError(t, m.CommitLive(t.Context(), stage))
	settled, err := m.load(stage.Parent)
	require.NoError(t, err)
	require.Nil(t, settled.LiveCommit.Pending)
	require.Nil(t, settled.LiveCommit.ObsoleteBranch)
	require.Equal(t, stage.InitialGeneration, settled.GenerationID)
	require.Equal(t, stage.InitialGeneration, settled.Stage.InitialGeneration)
	actual := make([]byte, 2*rootfsblock.LogicalBlockSize)
	_, err = branch.ReadAt(actual, 0)
	require.NoError(t, err)
	require.Equal(t, bytes.Repeat([]byte{22}, rootfsblock.LogicalBlockSize), actual[:rootfsblock.LogicalBlockSize])
	require.Equal(t, []byte("suffix"), actual[rootfsblock.LogicalBlockSize:rootfsblock.LogicalBlockSize+6])
	// The remotely acknowledged snapshot still contains the original cut.
	descriptor, err := rootfsblock.DecodeDescriptor(payload)
	require.NoError(t, err)
	reader, err := rootfsblock.NewReader(m.source, descriptor, 0)
	require.NoError(t, err)
	_, err = reader.ReadAt(actual[:rootfsblock.LogicalBlockSize], 0)
	require.NoError(t, err)
	require.Equal(t, bytes.Repeat([]byte{11}, rootfsblock.LogicalBlockSize), actual[:rootfsblock.LogicalBlockSize])
	a.beforePublish = nil
	require.NoError(t, m.CommitLive(t.Context(), stage))
	final, err := m.load(stage.Parent)
	require.NoError(t, err)
	require.Equal(t, uint64(3), final.BranchSequenceBase)
	require.Zero(t, m.NodeDirtyTailUsage().UsedBytes)
	require.NoError(t, m.BeginRetire(stage.Parent, stage.Identity, "retire"))
	require.NoError(t, m.Release(t.Context(), stage.Identity))
	require.NoError(t, m.ReclaimTerminalArtifacts(stage.Parent, stage.Identity))
	require.NoError(t, m.ForgetVerifiedTerminal(stage.Parent, stage.Identity))
}

func TestNodeLiveCommitUploadRetryReadsOriginalCutAndRetirementSettlesIntent(t *testing.T) {
	for _, accepted := range []bool{false, true} {
		t.Run(map[bool]string{false: "unaccepted cut", true: "lost accepted reply"}[accepted], func(t *testing.T) {
			m, a, stage, branch := newNodeLiveFixture(t, "live-retire")
			_, err := branch.WriteAt([]byte("cut"), 0)
			require.NoError(t, err)
			if accepted {
				a.lostPublish = true
			} else {
				a.failObserve = true
			}
			require.ErrorIs(t, m.CommitLive(t.Context(), stage), errdefs.ErrUnavailable)
			_, err = branch.WriteAt([]byte("later"), 0)
			require.NoError(t, err)
			a.failObserve = false
			a.state = "retiring"
			require.NoError(t, m.CommitLive(t.Context(), stage))
			stored, err := m.load(stage.Parent)
			require.NoError(t, err)
			require.Nil(t, stored.LiveCommit.Pending)
			require.Zero(t, stored.BranchSequenceBase, "retirement can seal the full WAL without an unnecessary rotation")
			if accepted {
				require.Equal(t, uint64(1), stored.LiveCommit.CommittedSequence)
			} else {
				require.Zero(t, stored.LiveCommit.CommittedSequence)
			}
			require.NoError(t, m.BeginRetire(stage.Parent, stage.Identity, "retire"))
			require.NoError(t, m.Release(t.Context(), stage.Identity))
			result, err := m.RetireResult(stage.Parent, stage.Identity, "retire")
			require.NoError(t, err)
			descriptor, err := rootfsblock.DecodeDescriptor(result.Descriptor)
			require.NoError(t, err)
			reader, err := rootfsblock.NewReader(m.source, descriptor, 0)
			require.NoError(t, err)
			actual := make([]byte, 5)
			_, err = reader.ReadAt(actual, 0)
			require.NoError(t, err)
			require.Equal(t, []byte("later"), actual)
		})
	}
}

func TestNodeLiveCommitUploadsWithoutBlockingRenewalAndPreservesConcurrentConsumer(t *testing.T) {
	m, a, stage, branch := newNodeLiveFixture(t, "live-renewal")
	current, err := m.load(stage.Parent)
	require.NoError(t, err)
	current.Consumer = &ConsumerRegistration{LeaseID: "lease", ActiveKey: "key", ContainerID: "container", StableMount: "/stable", HostMountNamespace: "mnt:[1]", LeaseExpiresAt: time.Now().Add(time.Minute).UTC().Format(time.RFC3339Nano)}
	require.NoError(t, m.save(current))
	_, err = branch.WriteAt([]byte("cut"), 0)
	require.NoError(t, err)
	entered, release := make(chan struct{}), make(chan struct{})
	a.beforePublish = func(LiveCommitPublication) { close(entered); <-release }
	done := make(chan error, 1)
	go func() { done <- m.CommitLive(t.Context(), stage) }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("upload did not start")
	}
	deadline := time.Now().Add(2 * time.Minute).UTC()
	require.NoError(t, m.RenewConsumer(stage.Parent, stage.Identity, "lease", deadline))
	_, err = branch.WriteAt([]byte("suffix"), rootfsblock.LogicalBlockSize)
	require.NoError(t, err)
	require.ErrorIs(t, m.BeginRetire(stage.Parent, stage.Identity, "retire"), errdefs.ErrUnavailable)
	close(release)
	select {
	case err = <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("commit did not finish")
	}
	stored, err := m.load(stage.Parent)
	require.NoError(t, err)
	require.Equal(t, deadline.Format(time.RFC3339Nano), stored.Consumer.LeaseExpiresAt)
	require.Equal(t, uint64(1), stored.BranchSequenceBase)
}

func TestNodeLiveCommitUploadFailureRetriesOriginalFlushedCut(t *testing.T) {
	m, a, stage, branch := newNodeLiveFixture(t, "live-upload-retry")
	m.publisher = &failOncePublisher{next: m.publisher}
	_, err := branch.WriteAt(bytes.Repeat([]byte{11}, rootfsblock.LogicalBlockSize), 0)
	require.NoError(t, err)
	require.ErrorContains(t, m.CommitLive(t.Context(), stage), "injected immutable publication failure")
	stored, err := m.load(stage.Parent)
	require.NoError(t, err)
	require.Nil(t, stored.LiveCommit.Pending.Generation)
	_, err = branch.WriteAt(bytes.Repeat([]byte{22}, rootfsblock.LogicalBlockSize), 0)
	require.NoError(t, err)
	require.NoError(t, m.CommitLive(t.Context(), stage))
	require.Len(t, a.published, 1)
	require.Equal(t, uint64(1), a.published[0].CutSequence)
	descriptor, err := rootfsblock.DecodeDescriptor(a.published[0].Generation.Descriptor)
	require.NoError(t, err)
	reader, err := rootfsblock.NewReader(m.source, descriptor, 0)
	require.NoError(t, err)
	actual := make([]byte, rootfsblock.LogicalBlockSize)
	_, err = reader.ReadAt(actual, 0)
	require.NoError(t, err)
	require.Equal(t, bytes.Repeat([]byte{11}, rootfsblock.LogicalBlockSize), actual)
	_, err = branch.ReadAt(actual, 0)
	require.NoError(t, err)
	require.Equal(t, bytes.Repeat([]byte{22}, rootfsblock.LogicalBlockSize), actual)
	stored, err = m.load(stage.Parent)
	require.NoError(t, err)
	require.Equal(t, uint64(1), stored.BranchSequenceBase)
	// Recovery opens the durably selected successor with its monotonic base.
	require.NoError(t, branch.Close())
	reopened, err := m.reopenBranch(stored)
	require.NoError(t, err)
	defer reopened.Close()
	_, err = reopened.ReadAt(actual, 0)
	require.NoError(t, err)
	require.Equal(t, bytes.Repeat([]byte{22}, rootfsblock.LogicalBlockSize), actual)
	require.NoError(t, reopened.Flush())
	sequence, err := reopened.DurableSequence()
	require.NoError(t, err)
	require.Equal(t, uint64(2), sequence)
}

func TestNodeLiveCommitCrashCleanupRetainsPendingReceiptUntilRegionalTerminal(t *testing.T) {
	m, a, stage, branch := newNodeLiveFixture(t, "live-crash")
	_, err := branch.WriteAt([]byte("cut"), 0)
	require.NoError(t, err)
	a.lostInstall = true
	require.Error(t, m.CommitLive(t.Context(), stage))
	stored, err := m.load(stage.Parent)
	require.NoError(t, err)
	require.NotNil(t, stored.LiveCommit.Pending)
	// Unplanned physical detach remains possible when regional replies are
	// unavailable. It never claims a new final filesystem publication.
	require.NoError(t, m.Release(t.Context(), stage.Identity))
	tombstone, err := m.load(stage.Parent)
	require.NoError(t, err)
	require.Equal(t, stateTombstoned, tombstone.State)
	require.NotNil(t, tombstone.LiveCommit.Pending)
	_, err = os.Stat(tombstone.BranchPath)
	require.NoError(t, err)
	// Caller still needs the existing exact crash proof and regional terminal
	// verification before this cleanup entry point becomes authorized.
	require.ErrorIs(t, m.ReclaimTerminalArtifacts(stage.Parent, stage.Identity), errdefs.ErrFailedPrecondition)
	// Rotated paths cannot escape their owning session, including in a
	// terminal journal tampered to name another session's WAL.
	tombstone.BranchPath = filepath.Join(m.branchRoot, "foreign.wal")
	_, err = m.ownedSessionBranches(tombstone)
	require.Error(t, err)
}

func TestNodeLiveCommitRecutsWhenDuplicatedSuffixCannotFit(t *testing.T) {
	m, a, stage, branch := newNodeLiveFixture(t, "live-recut")
	// Reuse the same node budget held by the branch, with just three payload
	// blocks available. Three selected records plus two copied suffix records
	// cannot fit, but a new full cut with an empty suffix can reclaim them.
	budget, err := rootfsblock.NewDirtyTailBudgetWithReserve(4*rootfsblock.LogicalBlockSize, rootfsblock.LogicalBlockSize)
	require.NoError(t, err)
	m.nodeDirty = budget
	initial, err := m.load(stage.Parent)
	require.NoError(t, err)
	require.NoError(t, branch.Close())
	branch, err = m.reopenBranch(initial)
	require.NoError(t, err)
	m.live[stage.Parent].branch = branch
	_, err = branch.WriteAt([]byte("first"), 0)
	require.NoError(t, err)
	a.beforePublish = func(LiveCommitPublication) {
		for range 2 {
			_, writeErr := branch.WriteAt([]byte("latest"), 0)
			require.NoError(t, writeErr)
		}
	}
	err = m.CommitLive(t.Context(), stage)
	var capacity *rootfsblock.DirtyTailCapacityError
	require.ErrorAs(t, err, &capacity)
	stored, err := m.load(stage.Parent)
	require.NoError(t, err)
	require.Nil(t, stored.LiveCommit.Pending)
	require.Equal(t, uint64(1), stored.LiveCommit.CommittedSequence)
	require.Equal(t, initial.BranchPath, stored.BranchPath)
	require.Equal(t, int64(3*rootfsblock.LogicalBlockSize), m.NodeDirtyTailUsage().UsedBytes)
	a.beforePublish = nil
	require.NoError(t, m.CommitLive(t.Context(), stage))
	require.Len(t, a.published, 2)
	require.Equal(t, a.published[0].Generation.GenerationID, a.published[1].ExpectedCommittedGenerationID)
	require.Equal(t, stage.InitialGeneration, a.published[1].BaseGenerationID)
	require.Equal(t, uint64(3), a.published[1].CutSequence)
	require.Zero(t, m.NodeDirtyTailUsage().UsedBytes)
	actual := make([]byte, 6)
	_, err = branch.ReadAt(actual, 0)
	require.NoError(t, err)
	require.Equal(t, []byte("latest"), actual)
}

func TestNodeLiveCommitRecoversAmbiguousJournalSelectionUnderSameBackend(t *testing.T) {
	for _, chooseNew := range []bool{false, true} {
		t.Run(map[bool]string{false: "old selected", true: "successor selected"}[chooseNew], func(t *testing.T) {
			m, a, stage, branch := newNodeLiveFixture(t, "live-local-ambiguity")
			_, err := branch.WriteAt([]byte("cut"), 0)
			require.NoError(t, err)
			a.lostPublish = true
			require.ErrorIs(t, m.CommitLive(t.Context(), stage), errdefs.ErrUnavailable)
			_, err = branch.WriteAt([]byte("suffix"), rootfsblock.LogicalBlockSize)
			require.NoError(t, err)
			current, err := m.load(stage.Parent)
			require.NoError(t, err)
			pending := current.LiveCommit.Pending
			pending.RemoteConfirmed = true
			current.LiveCommit.CommittedGenerationID, current.LiveCommit.CommittedSequence = pending.GenerationID, pending.Cut.Sequence
			require.NoError(t, m.saveLiveCommit(current))
			descriptor, err := rootfsblock.DecodeDescriptor(pending.Generation.Descriptor)
			require.NoError(t, err)
			reader, err := rootfsblock.NewReader(m.source, descriptor, 0)
			require.NoError(t, err)
			destination := sessionBranchPath(m.branchRoot, m.mountRoot, stage.Parent, pending.Cut.Sequence)
			injected := errors.New("local journal acknowledgement lost")
			err = branch.RotateCommitted(pending.Cut, reader, descriptor.MappingRoot.RootDigest, destination, func(identity rootfsblock.BranchIdentity) error {
				if chooseNew {
					current.LiveCommit.ObsoleteBranch = &obsoleteBranch{Path: current.BranchPath, SequenceBase: current.BranchSequenceBase}
					current.BranchPath, current.BranchSequenceBase = destination, identity.SequenceBase
					current.BaseDescriptor = append([]byte(nil), pending.Generation.Descriptor...)
					current.LiveCommit.InstalledGenerationID = pending.GenerationID
					require.NoError(t, m.saveLiveCommit(current))
				}
				return injected
			})
			require.ErrorIs(t, err, injected)
			require.NotNil(t, branch.RotationFailure())
			require.NoError(t, m.CommitLive(t.Context(), stage))
			require.Nil(t, branch.RotationFailure())
			stored, err := m.load(stage.Parent)
			require.NoError(t, err)
			require.Nil(t, stored.LiveCommit.Pending)
			require.Equal(t, destination, stored.BranchPath)
			require.Same(t, branch, m.live[stage.Parent].branch)
			require.Equal(t, int64(rootfsblock.LogicalBlockSize), m.NodeDirtyTailUsage().UsedBytes)
			actual := make([]byte, 6)
			_, err = branch.ReadAt(actual, rootfsblock.LogicalBlockSize)
			require.NoError(t, err)
			require.Equal(t, []byte("suffix"), actual)
			_, err = branch.WriteAt([]byte("resumed"), 0)
			require.NoError(t, err)
			require.NoError(t, branch.Flush())
			sequence, err := branch.DurableSequence()
			require.NoError(t, err)
			require.Equal(t, uint64(3), sequence)
		})
	}
}

func TestNodeLiveCommitObservationCannotAcknowledgeDifferentDescriptor(t *testing.T) {
	m, a, stage, branch := newNodeLiveFixture(t, "live-descriptor-receipt")
	_, err := branch.WriteAt([]byte("cut"), 0)
	require.NoError(t, err)
	a.lostPublish = true
	require.ErrorIs(t, m.CommitLive(t.Context(), stage), errdefs.ErrUnavailable)
	stored, err := m.load(stage.Parent)
	require.NoError(t, err)
	a.descriptorDigest = digest.FromString("different-payload").String()
	require.ErrorIs(t, m.CommitLive(t.Context(), stage), errdefs.ErrFailedPrecondition)
	retained, err := m.load(stage.Parent)
	require.NoError(t, err)
	require.Equal(t, stored.BranchPath, retained.BranchPath)
	require.False(t, retained.LiveCommit.Pending.RemoteConfirmed)
	require.FileExists(t, stored.BranchPath)
	require.Empty(t, a.installed)
	a.descriptorDigest = digest.FromBytes(a.published[0].Generation.Descriptor).String()
	require.NoError(t, m.CommitLive(t.Context(), stage))
}
