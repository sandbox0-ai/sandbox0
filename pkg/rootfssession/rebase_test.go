package session

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/containerd/errdefs"
	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsrebase"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

type fakeRebaseEngine struct {
	calls  atomic.Int64
	result rootfsrebase.ApplyResult
	err    error
}

func (e *fakeRebaseEngine) Apply(
	_ context.Context,
	_ rootfsrebase.WorkerRequest,
	_, _, _ string,
	dirtyBlocks []uint64,
) (*rootfsrebase.ApplyResult, error) {
	e.calls.Add(1)
	if e.err != nil {
		return nil, e.err
	}
	if len(dirtyBlocks) == 0 {
		return nil, errdefs.ErrFailedPrecondition
	}
	result := e.result
	return &result, nil
}

func TestManagerExecuteRebaseJournalsExactResultAndCleansResources(t *testing.T) {
	base := t.TempDir()
	objects, request := testRebaseRequest(t, "exact")
	runtime := newFakeHostRuntime(objects)
	runtime.devicePaths = []string{"/dev/fake0", "/dev/fake1", "/dev/fake2"}
	engine := &fakeRebaseEngine{result: testRebaseApplyResult("exact")}
	manager := newRebaseTestManager(t, base, objects, runtime, engine)
	defer manager.Close()

	result, err := manager.ExecuteRebase(t.Context(), request)
	require.NoError(t, err)
	require.NoError(t, result.ValidateFor(request))
	require.EqualValues(t, 1, engine.calls.Load())
	require.Equal(t, 3, runtime.count("attach"))
	require.Equal(t, 3, runtime.count("mount-xfs"))
	require.Equal(t, 3, runtime.count("mount-overlay"))
	require.Equal(t, 3, runtime.count("close-device"))
	require.Equal(t, 3, runtime.count("unmount-xfs"))
	require.Equal(t, 3, runtime.count("unmount-overlay"))
	require.Equal(t, []bool{true, true, false, false, false, false}, runtime.unmountSyncSnapshot())
	for _, path := range runtime.devicePaths {
		require.Empty(t, runtime.reservationOwner(path))
	}
	branchRoot, mountRoot := rebaseOperationRoots(manager.branchRoot, manager.mountRoot, request.OperationID)
	_, err = os.Stat(branchRoot)
	require.ErrorIs(t, err, os.ErrNotExist)
	_, err = os.Stat(mountRoot)
	require.ErrorIs(t, err, os.ErrNotExist)
	require.Equal(t, rootfsblock.NodeDirtyTailUsage{
		ReservedBytes: DefaultDirtyTailRetirementReserveBytes,
		MaxBytes:      DefaultMaxNodeDirtyTailBytes,
	}, manager.NodeDirtyTailUsage())

	calls := runtime.callsSnapshot()
	retry, err := manager.ExecuteRebase(t.Context(), request)
	require.NoError(t, err)
	require.Equal(t, result, retry)
	require.Equal(t, calls, runtime.callsSnapshot())
	require.EqualValues(t, 1, engine.calls.Load())

	changed := request
	changed.MaxChangedBlocks--
	_, err = manager.ExecuteRebase(t.Context(), changed)
	require.ErrorIs(t, err, errdefs.ErrAlreadyExists)
	require.ErrorIs(t, manager.AcknowledgeRebase(request, digest.FromString("wrong-proof").String()), errdefs.ErrFailedPrecondition)
	require.NoError(t, manager.AcknowledgeRebase(request, result.ProofDigest))
	_, err = manager.loadRebase(request.OperationID)
	require.ErrorIs(t, err, errdefs.ErrNotFound)
	require.NoError(t, manager.AcknowledgeRebase(request, result.ProofDigest),
		"exact acknowledgement retries must use the compact tombstone")
	require.ErrorIs(t, manager.AcknowledgeRebase(request, digest.FromString("changed-proof").String()),
		errdefs.ErrFailedPrecondition)
	require.ErrorIs(t, manager.AcknowledgeRebase(changed, result.ProofDigest), errdefs.ErrFailedPrecondition)
	_, err = manager.ExecuteRebase(t.Context(), request)
	require.ErrorIs(t, err, errdefs.ErrFailedPrecondition)
	require.EqualValues(t, 1, engine.calls.Load())
}

func TestManagerRebaseResultSurvivesRestartAndResponseLoss(t *testing.T) {
	base := t.TempDir()
	objects, request := testRebaseRequest(t, "restart")
	firstRuntime := newFakeHostRuntime(objects)
	firstRuntime.devicePaths = []string{"/dev/fake0", "/dev/fake1", "/dev/fake2"}
	firstEngine := &fakeRebaseEngine{result: testRebaseApplyResult("restart")}
	first := newRebaseTestManager(t, base, objects, firstRuntime, firstEngine)
	result, err := first.ExecuteRebase(t.Context(), request)
	require.NoError(t, err)
	require.NoError(t, first.Close())

	secondRuntime := newFakeHostRuntime(objects)
	secondRuntime.devicePaths = []string{"/dev/fake0", "/dev/fake1", "/dev/fake2"}
	secondEngine := &fakeRebaseEngine{result: testRebaseApplyResult("must-not-run")}
	second := newRebaseTestManager(t, base, objects, secondRuntime, secondEngine)
	require.NoError(t, second.ReconcileRebases(t.Context()))
	retry, err := second.ExecuteRebase(t.Context(), request)
	require.NoError(t, err)
	require.Equal(t, result, retry)
	require.Zero(t, secondEngine.calls.Load())
	require.Empty(t, secondRuntime.callsSnapshot())
	require.NoError(t, second.AcknowledgeRebase(request, result.ProofDigest))
	require.NoError(t, second.Close())
	third := newRebaseTestManager(t, base, objects, newFakeHostRuntime(objects), &fakeRebaseEngine{})
	defer third.Close()
	require.NoError(t, third.AcknowledgeRebase(request, result.ProofDigest))
}

func TestManagerRejectRebaseFencesExecutionWithoutCreatingFullJournal(t *testing.T) {
	base := t.TempDir()
	objects, request := testRebaseRequest(t, "reject-before-execute")
	manager := newRebaseTestManager(t, base, objects, newFakeHostRuntime(objects), &fakeRebaseEngine{})
	rejection, err := manager.RejectRebase(t.Context(), request)
	require.NoError(t, err)
	require.NoError(t, rejection.ValidateFor(request))
	require.Nil(t, rejection.Result)
	_, err = manager.loadRebase(request.OperationID)
	require.ErrorIs(t, err, errdefs.ErrNotFound)
	_, err = manager.ExecuteRebase(t.Context(), request)
	require.ErrorIs(t, err, errdefs.ErrFailedPrecondition)
	require.NoError(t, manager.Close())

	restarted := newRebaseTestManager(t, base, objects, newFakeHostRuntime(objects), &fakeRebaseEngine{})
	defer restarted.Close()
	retry, err := restarted.RejectRebase(t.Context(), request)
	require.NoError(t, err)
	require.Equal(t, rejection, retry)
	changed := request
	changed.MaxChangedBlocks--
	_, err = restarted.RejectRebase(t.Context(), changed)
	require.ErrorIs(t, err, errdefs.ErrFailedPrecondition)
	require.NoError(t, restarted.AcknowledgeRebase(request, rejection.ProofDigest))
}

func TestManagerRejectRebaseRetainsExecutedResultUntilExactAck(t *testing.T) {
	base := t.TempDir()
	objects, request := testRebaseRequest(t, "reject-after-execute")
	runtime := newFakeHostRuntime(objects)
	runtime.devicePaths = []string{"/dev/fake0", "/dev/fake1", "/dev/fake2"}
	manager := newRebaseTestManager(t, base, objects, runtime,
		&fakeRebaseEngine{result: testRebaseApplyResult("reject-after-execute")})
	defer manager.Close()
	result, err := manager.ExecuteRebase(t.Context(), request)
	require.NoError(t, err)
	rejection, err := manager.RejectRebase(t.Context(), request)
	require.NoError(t, err)
	require.NoError(t, rejection.ValidateFor(request))
	require.NotNil(t, rejection.Result)
	require.Equal(t, result, *rejection.Result)
	_, err = manager.loadRebase(request.OperationID)
	require.NoError(t, err)
	require.ErrorIs(t, manager.AcknowledgeRebase(request, digest.FromString("wrong-rejection").String()),
		errdefs.ErrFailedPrecondition)
	require.NoError(t, manager.AcknowledgeRebase(request, rejection.ProofDigest))
	_, err = manager.loadRebase(request.OperationID)
	require.ErrorIs(t, err, errdefs.ErrNotFound)
}

func TestManagerPrunesCompactRebaseAcknowledgementsByAgeAndCardinality(t *testing.T) {
	base := t.TempDir()
	objects, _ := testRebaseRequest(t, "ack-prune")
	manager := newRebaseTestManager(t, base, objects, newFakeHostRuntime(objects), &fakeRebaseEngine{})
	defer manager.Close()
	now := time.Now().UTC()
	require.NoError(t, manager.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(rebaseAckBucket)
		for index := 0; index < maxRebaseAcknowledgements+17; index++ {
			requestDigest, err := parseRebaseSHA256Digest(digest.FromString(fmt.Sprintf("request-%d", index)).String())
			if err != nil {
				return err
			}
			proofDigest, err := parseRebaseSHA256Digest(digest.FromString(fmt.Sprintf("proof-%d", index)).String())
			if err != nil {
				return err
			}
			acknowledgedAt := now.Add(time.Duration(index) * time.Nanosecond).UnixNano()
			if index == 0 {
				acknowledgedAt = now.Add(-rebaseAcknowledgementTTL - time.Second).UnixNano()
			}
			if err := bucket.Put(rebaseAcknowledgementKey(fmt.Sprintf("operation-%d", index)),
				encodeRebaseAcknowledgement(rebaseAcknowledgement{
					AcknowledgedAt: acknowledgedAt, RequestDigest: requestDigest, ProofDigest: proofDigest,
				})); err != nil {
				return err
			}
		}
		return nil
	}))
	require.NoError(t, manager.pruneRebaseAcknowledgements(now))
	require.NoError(t, manager.db.View(func(tx *bolt.Tx) error {
		count := 0
		require.NoError(t, tx.Bucket(rebaseAckBucket).ForEach(func(_, payload []byte) error {
			if payload != nil {
				count++
			}
			return nil
		}))
		require.Equal(t, maxRebaseAcknowledgements, count)
		require.Nil(t, tx.Bucket(rebaseAckBucket).Get(rebaseAcknowledgementKey("operation-0")))
		return nil
	}))
}

func TestManagerRebaseAttachFailureCleansAndCanRetry(t *testing.T) {
	base := t.TempDir()
	objects, request := testRebaseRequest(t, "attach-failure")
	runtime := newFakeHostRuntime(objects)
	runtime.devicePaths = []string{"/dev/fake0", "/dev/fake1", "/dev/fake2"}
	runtime.failAt = "attach"
	engine := &fakeRebaseEngine{result: testRebaseApplyResult("attach-failure")}
	manager := newRebaseTestManager(t, base, objects, runtime, engine)
	defer manager.Close()

	_, err := manager.ExecuteRebase(t.Context(), request)
	require.ErrorContains(t, err, "injected attach failure")
	_, err = manager.loadRebase(request.OperationID)
	require.ErrorIs(t, err, errdefs.ErrNotFound)
	require.Equal(t, 1, runtime.orphanRecoveries)
	for _, path := range runtime.devicePaths {
		require.Empty(t, runtime.reservationOwner(path))
	}
	branchRoot, mountRoot := rebaseOperationRoots(manager.branchRoot, manager.mountRoot, request.OperationID)
	_, err = os.Stat(branchRoot)
	require.ErrorIs(t, err, os.ErrNotExist)
	_, err = os.Stat(mountRoot)
	require.ErrorIs(t, err, os.ErrNotExist)
	require.Zero(t, manager.NodeDirtyTailUsage().Owners)

	runtime.failAt = ""
	result, err := manager.ExecuteRebase(t.Context(), request)
	require.NoError(t, err)
	require.NoError(t, result.ValidateFor(request))
}

func TestManagerRebaseRequiresThreeDeviceReservationsWithoutLeaking(t *testing.T) {
	base := t.TempDir()
	objects, request := testRebaseRequest(t, "device-capacity")
	runtime := newFakeHostRuntime(objects)
	runtime.devicePaths = []string{"/dev/fake0", "/dev/fake1"}
	engine := &fakeRebaseEngine{result: testRebaseApplyResult("device-capacity")}
	manager := newRebaseTestManager(t, base, objects, runtime, engine)
	defer manager.Close()

	_, err := manager.ExecuteRebase(t.Context(), request)
	require.ErrorContains(t, err, "no usable NBD device")
	require.Equal(t, 2, runtime.count("attach"))
	require.Zero(t, engine.calls.Load())
	for _, path := range runtime.devicePaths {
		require.Empty(t, runtime.reservationOwner(path))
	}
	_, err = manager.loadRebase(request.OperationID)
	require.ErrorIs(t, err, errdefs.ErrNotFound)

	runtime.mu.Lock()
	runtime.devicePaths = append(runtime.devicePaths, "/dev/fake2")
	runtime.mu.Unlock()
	result, err := manager.ExecuteRebase(t.Context(), request)
	require.NoError(t, err)
	require.NoError(t, result.ValidateFor(request))
}

func TestManagerReconcileRebaseAdoptsAndCleansInterruptedResources(t *testing.T) {
	base := t.TempDir()
	objects, request := testRebaseRequest(t, "startup-cleanup")
	firstRuntime := newFakeHostRuntime(objects)
	firstRuntime.devicePaths = []string{"/dev/fake0", "/dev/fake1", "/dev/fake2"}
	first := newRebaseTestManager(t, base, objects, firstRuntime, &fakeRebaseEngine{})
	digest, err := request.Digest()
	require.NoError(t, err)
	current := newRebaseRecord(first.branchRoot, first.mountRoot, request, digest)
	for index := range current.Resources {
		current.Resources[index].DevicePath = firstRuntime.devicePaths[index]
		current.Resources[index].XFSMountIntent = true
		current.Resources[index].OverlayMountIntent = true
	}
	current.State = rebaseStateApplying
	require.NoError(t, first.saveNewRebase(current))
	require.NoError(t, first.Close())

	secondRuntime := newFakeHostRuntime(objects)
	secondRuntime.devicePaths = []string{"/dev/fake0", "/dev/fake1", "/dev/fake2"}
	second := newRebaseTestManager(t, base, objects, secondRuntime, &fakeRebaseEngine{})
	defer second.Close()
	require.NoError(t, second.ReconcileRebases(t.Context()))
	require.Equal(t, 3, secondRuntime.orphanRecoveries)
	require.Equal(t, 3, secondRuntime.count("unmount-overlay"))
	require.Equal(t, 3, secondRuntime.count("unmount-xfs"))
	for _, path := range secondRuntime.devicePaths {
		require.Empty(t, secondRuntime.reservationOwner(path))
	}
	_, err = second.loadRebase(request.OperationID)
	require.ErrorIs(t, err, errdefs.ErrNotFound)
}

func TestManagerRejectsExpiredOrExcessiveRebaseAuthorityBeforeSideEffects(t *testing.T) {
	for name, deadline := range map[string]time.Time{
		"expired":   time.Now().Add(-time.Minute),
		"excessive": time.Now().Add(rootfsrebase.MaxWorkerRollbackRetention + time.Hour),
	} {
		t.Run(name, func(t *testing.T) {
			base := t.TempDir()
			objects, request := testRebaseRequest(t, name)
			request.RollbackExpiresAt = deadline.UTC().Format(time.RFC3339Nano)
			runtime := newFakeHostRuntime(objects)
			runtime.devicePaths = []string{"/dev/fake0", "/dev/fake1", "/dev/fake2"}
			manager := newRebaseTestManager(t, base, objects, runtime, &fakeRebaseEngine{})
			defer manager.Close()
			_, err := manager.ExecuteRebase(t.Context(), request)
			require.Error(t, err)
			require.Empty(t, runtime.callsSnapshot())
			_, err = manager.loadRebase(request.OperationID)
			require.ErrorIs(t, err, errdefs.ErrNotFound)
		})
	}
}

func newRebaseTestManager(
	t *testing.T,
	base string,
	objects *sessionObjectStore,
	runtime *fakeHostRuntime,
	engine RebaseEngine,
) *Manager {
	t.Helper()
	manager, err := New(Config{
		StatePath:  filepath.Join(base, "state", "sessions.db"),
		BranchRoot: filepath.Join(base, "branches"), MountRoot: filepath.Join(base, "mounts"),
		MaxDirtyTailBytes: 16 * rootfsblock.LogicalBlockSize,
		Source:            objects, Publisher: objects, Runtime: runtime, RebaseEngine: engine,
	})
	require.NoError(t, err)
	return manager
}

func testRebaseRequest(t *testing.T, name string) (*sessionObjectStore, rootfsrebase.WorkerRequest) {
	t.Helper()
	objects := newSessionObjectStore()
	logicalSize := int64(4 * rootfsblock.LogicalBlockSize)
	options := rootfsblock.BuildOptions{
		DataRangeBytes: rootfsblock.LogicalBlockSize,
		PackBytes:      rootfsblock.LogicalBlockSize,
		PageEntries:    16,
	}
	zero := make([]byte, logicalSize)
	source := append([]byte(nil), zero...)
	copy(source, bytes.Repeat([]byte{0x41}, rootfsblock.LogicalBlockSize))
	target := append([]byte(nil), zero...)
	copy(target[rootfsblock.LogicalBlockSize:], bytes.Repeat([]byte{0x42}, rootfsblock.LogicalBlockSize))
	sourceBase := buildRebaseTestGeneration(t, objects, zero, options)
	sourceGeneration := buildRebaseTestGeneration(t, objects, source, options)
	targetBase := buildRebaseTestGeneration(t, objects, target, options)
	return objects, rootfsrebase.WorkerRequest{
		Version: rootfsrebase.WorkerProtocolVersion, OperationID: "rebase-operation-" + name,
		SandboxID: "sandbox-" + name, TeamID: "team-" + name, FilesystemID: "filesystem-" + name,
		SourceGenerationID:       "source-generation-" + name,
		SourceOCIDigest:          digest.FromString("source-oci-" + name).String(),
		SourceBaseArtifactDigest: digest.FromString("source-artifact-" + name).String(),
		SourceBaseBlockRoot:      sourceBase.Descriptor.MappingRoot.RootDigest,
		SourceCurrentBlockHead:   sourceGeneration.Descriptor.MappingRoot.RootDigest,
		SourceFormatGeneration:   1, SourceLocatorVersion: 1,
		SourceBaseDescriptor: sourceBase.Payload, SourceGenerationDescriptor: sourceGeneration.Payload,
		TargetGenerationID:       "target-generation-" + name,
		TargetSourceOCIDigest:    digest.FromString("target-oci-" + name).String(),
		TargetBaseArtifactDigest: digest.FromString("target-artifact-" + name).String(),
		TargetBaseBlockRoot:      targetBase.Descriptor.MappingRoot.RootDigest,
		TargetFormatGeneration:   1, TargetWriterEpoch: 2, TargetBaseDescriptor: targetBase.Payload,
		RollbackExpiresAt: time.Now().Add(time.Hour).UTC().Format(time.RFC3339Nano),
		MaxChangedBlocks:  16,
	}
}

func buildRebaseTestGeneration(
	t *testing.T,
	objects *sessionObjectStore,
	payload []byte,
	options rootfsblock.BuildOptions,
) rootfsblock.BuildResult {
	t.Helper()
	built, err := rootfsblock.BuildMaterializedGeneration(
		t.Context(), bytes.NewReader(payload), int64(len(payload)), objects, options,
	)
	require.NoError(t, err)
	return built
}

func testRebaseApplyResult(name string) rootfsrebase.ApplyResult {
	return rootfsrebase.ApplyResult{
		Version: rootfsrebase.ApplyResultVersion, AppliedChanges: 1, TargetNodeCount: 1,
		OldManifestDigest:    rebaseTestHex(name + "-old"),
		SourceManifestDigest: rebaseTestHex(name + "-source"),
		DiffDigest:           rebaseTestHex(name + "-diff"),
		TargetManifestDigest: rebaseTestHex(name + "-target"),
		HealthProof:          rebaseTestHex(name + "-health"),
	}
}

func rebaseTestHex(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}
