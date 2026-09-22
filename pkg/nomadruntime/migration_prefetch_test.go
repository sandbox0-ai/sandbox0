package nomadruntime

import (
	"context"
	"encoding/json"
	"io"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/containerd/errdefs"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecheckpoint"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
)

func reservePrefetchTarget(t *testing.T, target *nodeRuntime, image protocol.MigrationImagePrepareRequest, plan protocol.MigrationPublicationPlan) protocol.MigrationImagePrefetchRequest {
	t.Helper()
	digest, err := image.Resources.Digest()
	require.NoError(t, err)
	staging := protocol.MigrationStagingRequest{Target: image.Target, Destination: image.Target, Source: image.Publication.Capture.Request,
		DestinationResourceLeaseDigest: strings.TrimPrefix(digest, "sha256:"), Bytes: runtimecheckpoint.ChunkBytes, Inodes: 64}
	_, err = target.ReserveMigrationStaging(t.Context(), staging)
	require.NoError(t, err)
	request := protocol.MigrationImagePrefetchRequest{Staging: staging, Publication: image.Publication, Plan: plan}
	require.NoError(t, request.Validate())
	return request
}

func migrationPrefetchFixture(t *testing.T) (*nodeRuntime, *nodeRuntime, protocol.MigrationImagePrefetchRequest, protocol.MigrationImagePrepareRequest, *migrationImageDownloadTestRuntime, *httptest.Server) {
	t.Helper()
	source, target, image, downloads, server := migrationPeerFixture(t)
	p := image.Receipt
	plan := protocol.MigrationPublicationPlan{RequestDigest: p.RequestDigest, Binding: p.Binding, Reference: p.Reference, Peer: p.Peer}
	return source, target, reservePrefetchTarget(t, target, image, plan), image, downloads, server
}

func TestMigrationPrefetchReusesVerifiedCacheAcrossRestart(t *testing.T) {
	_, target, request, image, downloads, server := migrationPrefetchFixture(t)
	filled, err := target.PrefetchMigrationImage(t.Context(), request)
	require.NoError(t, err)
	require.NoError(t, filled.ValidateFor(request))
	record, err := target.journal.Get(image.Target.SlotID)
	require.NoError(t, err)
	require.Equal(t, runtimeSlotPrefetchJournalVersion, record.Version)
	require.Nil(t, record.MigrationDestination, "cache fill is not target preparation")
	cached := record.MigrationStaging.Prefetch.ImageDirectory
	_, err = target.journal.BeginCleanup(testRuntimeSlotJournalCleanup(record.Registration))
	require.ErrorIs(t, err, errdefs.ErrFailedPrecondition)
	path := target.journal.db.Path()
	require.NoError(t, target.journal.Close())
	target.journal, err = newRuntimeSlotJournal(path, time.Hour)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, target.journal.Close()) })
	identity, err := runtimecheckpoint.NewPeerIdentity()
	require.NoError(t, err)
	target.migrationPeer.identity = identity
	again, err := target.PrefetchMigrationImage(t.Context(), request)
	require.NoError(t, err)
	require.Equal(t, filled, again, "a historical fill receipt does not need another peer connection")
	server.Close()
	// The existing cache may fill the pool; reusing it needs no new allocation.
	target.migrationStaging = testMigrationStagingGuard{admit: func(string) error { return errdefs.ErrResourceExhausted }}
	prepared, err := target.PrepareMigrationImage(t.Context(), image)
	require.NoError(t, err)
	require.NoError(t, prepared.ValidateFor(image))
	require.Equal(t, 1, downloads.verifyCalls)
	require.Zero(t, downloads.downloadCalls)
	_, err = os.Stat(cached)
	require.ErrorIs(t, err, os.ErrNotExist)
	record, err = target.journal.Get(image.Target.SlotID)
	require.NoError(t, err)
	require.True(t, record.MigrationStaging.Prefetch.Removed)
	data, err := os.ReadFile(filepath.Join(record.MigrationDestination.ImageDirectory, "checkpoint.img"))
	require.NoError(t, err)
	require.Equal(t, "retained-memory", string(data))
	_, err = target.PrefetchMigrationImage(t.Context(), request)
	require.ErrorIs(t, err, errdefs.ErrFailedPrecondition, "a delayed cache fill cannot overwrite prepared custody")
	require.ErrorIs(t, target.ReleaseMigrationStaging(t.Context(), request.Staging), errdefs.ErrFailedPrecondition,
		"cache promotion cannot release the normal destination image")
}

func TestMigrationPrefetchDiscardsIncompleteCorruptAndInterruptedPromotion(t *testing.T) {
	for _, fault := range []string{"partial", "corrupt", "renamed-before-journal"} {
		t.Run(fault, func(t *testing.T) {
			_, target, request, image, downloads, server := migrationPrefetchFixture(t)
			if fault == "partial" {
				require.NoError(t, target.journal.recordMigrationPrefetch(request, nil))
			} else {
				_, err := target.PrefetchMigrationImage(t.Context(), request)
				require.NoError(t, err)
			}
			record, err := target.journal.Get(image.Target.SlotID)
			require.NoError(t, err)
			cache := record.MigrationStaging.Prefetch.ImageDirectory
			switch fault {
			case "partial":
				require.NoError(t, os.MkdirAll(cache, 0o700))
				require.NoError(t, os.WriteFile(filepath.Join(cache, "partial.img"), []byte("partial"), 0o600))
			case "corrupt":
				require.NoError(t, os.WriteFile(filepath.Join(cache, "checkpoint.img"), []byte("corrupt!-memory"), 0o600))
			case "renamed-before-journal":
				require.NoError(t, target.journal.recordMigrationDestination(image, nil))
				record, err = target.journal.Get(image.Target.SlotID)
				require.NoError(t, err)
				require.NoError(t, os.Rename(cache, record.MigrationDestination.ImageDirectory))
			}
			server.Close()
			prepared, err := target.PrepareMigrationImage(t.Context(), image)
			require.NoError(t, err)
			require.NoError(t, prepared.ValidateFor(image))
			require.Equal(t, 1, downloads.downloadCalls)
			record, err = target.journal.Get(image.Target.SlotID)
			require.NoError(t, err)
			require.True(t, record.MigrationStaging.Prefetch.Removed)
			data, err := os.ReadFile(filepath.Join(record.MigrationDestination.ImageDirectory, "checkpoint.img"))
			require.NoError(t, err)
			require.Equal(t, "retained-memory", string(data))
			_, err = os.Stat(cache)
			require.ErrorIs(t, err, os.ErrNotExist)
		})
	}
}

func TestMigrationPrefetchReleaseRequiresPhysicalCacheAbsence(t *testing.T) {
	_, target, request, image, _, _ := migrationPrefetchFixture(t)
	_, err := target.PrefetchMigrationImage(t.Context(), request)
	require.NoError(t, err)
	require.ErrorIs(t, target.journal.releaseMigrationStaging(request.Staging), errdefs.ErrFailedPrecondition)
	target.migrationStaging = nil
	require.ErrorIs(t, target.ReleaseMigrationStaging(t.Context(), request.Staging), errdefs.ErrUnavailable)
	record, err := target.journal.Get(image.Target.SlotID)
	require.NoError(t, err)
	require.False(t, record.MigrationStaging.Released)
	require.False(t, record.MigrationStaging.Prefetch.Removed)
	target.migrationStaging = testMigrationStagingGuard{}
	require.NoError(t, target.ReleaseMigrationStaging(t.Context(), request.Staging))
	require.NoError(t, target.ReleaseMigrationStaging(t.Context(), request.Staging))
	record, err = target.journal.Get(image.Target.SlotID)
	require.NoError(t, err)
	require.True(t, record.MigrationStaging.Released)
	require.True(t, record.MigrationStaging.Prefetch.Removed)
	_, err = os.Stat(record.MigrationStaging.Prefetch.ImageDirectory)
	require.ErrorIs(t, err, os.ErrNotExist)
	_, err = target.PrefetchMigrationImage(t.Context(), request)
	require.ErrorIs(t, err, errdefs.ErrFailedPrecondition)
}

type waitingMigrationPrefetchRuntime struct {
	nodeRuntimeBackend
	started  chan struct{}
	fallback migrationPeerRuntime
	calls    atomic.Int32
}

func (r *waitingMigrationPrefetchRuntime) WriteMigrationPeerImage(ctx context.Context, binding runtimecheckpoint.Binding, ref runtimecheckpoint.Reference, directory string, out io.Writer) error {
	if r.calls.Add(1) > 1 && r.fallback != nil {
		return r.fallback.WriteMigrationPeerImage(ctx, binding, ref, directory, out)
	}
	close(r.started)
	<-ctx.Done()
	return ctx.Err()
}

func TestMigrationPrefetchNormalPreparationPreemptsLostFillResponse(t *testing.T) {
	source, target, request, image, _, _ := migrationPrefetchFixture(t)
	block := &waitingMigrationPrefetchRuntime{nodeRuntimeBackend: source.runtime, fallback: source.runtime.(migrationPeerRuntime), started: make(chan struct{})}
	source.runtime = block
	filled := make(chan error, 1)
	go func() { _, err := target.PrefetchMigrationImage(t.Context(), request); filled <- err }()
	select {
	case <-block.started:
	case <-time.After(time.Second):
		t.Fatal("prefetch did not enter its blocked peer read")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	prepared, err := target.PrepareMigrationImage(ctx, image)
	require.NoError(t, err)
	require.NoError(t, prepared.ValidateFor(image))
	require.Error(t, <-filled)
	record, err := target.journal.Get(image.Target.SlotID)
	require.NoError(t, err)
	require.True(t, record.MigrationStaging.Prefetch.Removed)
	require.Equal(t, prepared, record.MigrationDestination.Prepared)
}

func TestMigrationPrefetchReleaseCancelsAndJoinsActiveFill(t *testing.T) {
	source, target, request, image, _, _ := migrationPrefetchFixture(t)
	block := &waitingMigrationPrefetchRuntime{nodeRuntimeBackend: source.runtime, started: make(chan struct{})}
	source.runtime = block
	filled := make(chan error, 1)
	go func() { _, err := target.PrefetchMigrationImage(t.Context(), request); filled <- err }()
	select {
	case <-block.started:
	case <-time.After(time.Second):
		t.Fatal("target did not request its peer image")
	}
	wrong := request.Staging
	wrong.Source.LifecycleEpoch++
	require.ErrorIs(t, target.ReleaseMigrationStaging(t.Context(), wrong), errdefs.ErrUnavailable)
	select {
	case <-filled:
		t.Fatal("a changed release canceled the active fill")
	default:
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	require.NoError(t, target.ReleaseMigrationStaging(ctx, request.Staging))
	require.Error(t, <-filled)
	record, err := target.journal.Get(image.Target.SlotID)
	require.NoError(t, err)
	require.True(t, record.MigrationStaging.Released)
	require.True(t, record.MigrationStaging.Prefetch.Removed)
	require.Nil(t, record.MigrationStaging.Prefetch.Receipt)
}

func TestMigrationPrefetchRejectsChangedReservationQuotaAndExecution(t *testing.T) {
	for _, fault := range []string{"reservation", "quota", "execution"} {
		t.Run(fault, func(t *testing.T) {
			_, target, request, image, _, _ := migrationPrefetchFixture(t)
			switch fault {
			case "reservation":
				request.Staging.Bytes++
			case "quota":
				target.migrationStaging = testMigrationStagingGuard{budget: func(string, int64, uint64) error { return errdefs.ErrResourceExhausted }}
			case "execution":
				target.runner.(*migrationSourceTestRunsc).stateErr = nil
			}
			_, err := target.PrefetchMigrationImage(t.Context(), request)
			require.Error(t, err)
			record, err := target.journal.Get(image.Target.SlotID)
			require.NoError(t, err)
			if c := record.MigrationStaging.Prefetch; c != nil {
				require.Nil(t, c.Receipt)
			}
			require.Nil(t, record.MigrationDestination)
		})
	}
}

func TestMigrationPrefetchJournalRejectsDowngradeAndRetainsCustody(t *testing.T) {
	_, target, request, image, _, _ := migrationPrefetchFixture(t)
	_, err := target.PrefetchMigrationImage(t.Context(), request)
	require.NoError(t, err)
	record, err := target.journal.Get(image.Target.SlotID)
	require.NoError(t, err)
	for _, version := range []int{RuntimeSlotJournalVersion, runtimeSlotStagingJournalVersion, runtimeSlotMigrationJournalVersion} {
		changed := record
		changed.Version = version
		payload, err := json.Marshal(changed)
		require.NoError(t, err)
		_, err = decodeRuntimeSlotJournalRecord(payload)
		require.ErrorIs(t, err, errdefs.ErrFailedPrecondition)
	}
	count, err := target.journal.Prune(time.Now().Add(100 * time.Hour))
	require.NoError(t, err)
	require.Zero(t, count)
	require.True(t, record.hasMigrationImageCustody())
}

func TestMigrationPrefetchCompletesBeforeSourcePublicationWithoutRestoreAuthority(t *testing.T) {
	source := newPlannedMigrationPeerFixture(t, nil, true)
	plan, err := source.source.PlanMigrationPublication(t.Context(), source.request)
	require.NoError(t, err)
	registration := testRuntimeSlotJournalRegistration(t, "prefetch-target")
	registration.NodeID, registration.NodeBootID, registration.AllocationID = "target-node", "target-boot", "target-allocation"
	journal, err := newRuntimeSlotJournal(filepath.Join(t.TempDir(), "target.db"), time.Hour)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, journal.Close()) })
	require.NoError(t, journal.Register(registration))
	resources, err := protocol.NewRuntimeResourceLease(source.request.Assignment.OperationID, "target-claim", registration.SlotID,
		registration.ClusterID, registration.NodeID, "target-uid", registration.NodeBootID,
		protocol.RuntimeResourceRequest{Version: protocol.RuntimeResourceRequestVersion, CPUMillicores: 1000, MemoryBytes: 128 << 20, PIDsLimit: 1024}, "0", "0")
	require.NoError(t, err)
	image := protocol.MigrationImagePrepareRequest{Target: protocol.NodeChannelTarget{SlotID: registration.SlotID, ClusterID: registration.ClusterID,
		NodeID: registration.NodeID, NodeUID: resources.NodeUID, NodeBootID: registration.NodeBootID, AllocationID: registration.AllocationID,
		ControlEndpoint: "unix:///private/target.sock"}, Publication: source.request, Resources: resources}
	downloads := &migrationImageDownloadTestRuntime{fakeRootFSRuntime: &fakeRootFSRuntime{}, store: source.runtime.store}
	runner := &migrationSourceTestRunsc{fakeRunsc: newFakeRunsc()}
	runner.stateErr = errdefs.ErrNotFound
	ctx, cancel := context.WithCancel(t.Context())
	target := &nodeRuntime{migrationStaging: testMigrationStagingGuard{}, runtime: downloads, runner: runner, journal: journal,
		migrationContext: ctx, migrationPeer: &migrationPeer{identity: source.client}, clusterID: registration.ClusterID,
		nodeID: registration.NodeID, nodeUID: resources.NodeUID}
	t.Cleanup(func() { cancel(); target.wg.Wait() })
	request := reservePrefetchTarget(t, target, image, *plan)
	filled, err := target.PrefetchMigrationImage(t.Context(), request)
	require.NoError(t, err)
	require.NoError(t, filled.ValidateFor(request))
	record, err := target.journal.Get(image.Target.SlotID)
	require.NoError(t, err)
	require.Nil(t, record.MigrationDestination)
	retained, err := source.source.GetMigrationCapture(t.Context(), source.request.Capture.Request.Target.SlotID)
	require.NoError(t, err)
	require.Nil(t, retained.Publication, "source upload remains blocked after target cache completion")
	_, err = target.PrepareMigrationImage(t.Context(), image)
	require.Error(t, err, "prefetch cannot supply the missing regional publication receipt")
	_, err = source.runtime.store.Download(t.Context(), plan.Binding, plan.Reference, filepath.Join(t.TempDir(), "not-published"))
	require.Error(t, err)
	source.release()
	require.NoError(t, <-source.result)
	retained, err = source.source.GetMigrationCapture(t.Context(), source.request.Capture.Request.Target.SlotID)
	require.NoError(t, err)
	image.Receipt = *retained.Publication
	prepared, err := target.PrepareMigrationImage(t.Context(), image)
	require.NoError(t, err)
	require.NoError(t, prepared.ValidateFor(image))
	require.Equal(t, 1, downloads.verifyCalls)
	require.Zero(t, downloads.downloadCalls)
}

type completingMigrationPrefetchRuntime struct {
	nodeRuntimeBackend
	fallback                   migrationPeerRuntime
	started, proceed, canceled chan struct{}
	calls                      atomic.Int32
}

func (r *completingMigrationPrefetchRuntime) WriteMigrationPeerImage(ctx context.Context, binding runtimecheckpoint.Binding, ref runtimecheckpoint.Reference, directory string, out io.Writer) error {
	if r.calls.Add(1) == 1 {
		close(r.started)
		select {
		case <-ctx.Done():
			close(r.canceled)
			return ctx.Err()
		case <-r.proceed:
		}
	}
	return r.fallback.WriteMigrationPeerImage(ctx, binding, ref, directory, out)
}

func TestMigrationPreparationLetsExactPrefetchFinishBeforePreemption(t *testing.T) {
	source, target, request, image, downloads, _ := migrationPrefetchFixture(t)
	block := &completingMigrationPrefetchRuntime{nodeRuntimeBackend: source.runtime,
		fallback: source.runtime.(migrationPeerRuntime), started: make(chan struct{}), proceed: make(chan struct{}), canceled: make(chan struct{})}
	source.runtime = block
	filled := make(chan error, 1)
	go func() { _, err := target.PrefetchMigrationImage(t.Context(), request); filled <- err }()
	select {
	case <-block.started:
	case <-time.After(time.Second):
		t.Fatal("prefetch did not enter the controlled transfer")
	}
	prepared := make(chan error, 1)
	go func() { _, err := target.PrepareMigrationImage(t.Context(), image); prepared <- err }()
	select {
	case <-block.canceled:
		t.Fatal("normal preparation canceled an exact active fill before its grace")
	case <-time.After(30 * time.Millisecond):
	}
	close(block.proceed)
	require.NoError(t, <-filled)
	require.NoError(t, <-prepared)
	require.EqualValues(t, 1, block.calls.Load(), "complete prefetch must not be retransmitted")
	require.Equal(t, 1, downloads.verifyCalls, "published bytes must still be verified")
	require.Zero(t, downloads.downloadCalls)
}

func TestMigrationPrefetchGraceMatchesExactWorkerAndHonorsCancellation(t *testing.T) {
	_, target, request, image, _, _ := migrationPrefetchFixture(t)
	record, err := target.journal.Get(image.Target.SlotID)
	require.NoError(t, err)
	digest, err := request.Digest()
	require.NoError(t, err)
	worker := &migrationPrefetchWorker{digest: digest, done: make(chan struct{})}
	target.migrationPrefetches = map[string]*migrationPrefetchWorker{image.Target.SlotID: worker}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, target.awaitMigrationPrefetch(ctx, image, record), context.Canceled)
	worker.digest = "different authorized image"
	require.NoError(t, target.awaitMigrationPrefetch(ctx, image, record), "unrelated worker must not be observed")
	select {
	case <-worker.done:
		t.Fatal("observer changed worker lifetime")
	default:
	}
}
