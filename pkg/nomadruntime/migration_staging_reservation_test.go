package nomadruntime

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/containerd/errdefs"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecheckpoint"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
)

func TestMigrationStagingPeerIdentitySurvivesRestartAndConfigChanges(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(fmt.Sprintf("initially_enabled_%t", enabled), func(t *testing.T) {
			daemon, request := migrationStagingReservationFixture(t)
			if enabled {
				cert, err := runtimecheckpoint.NewPeerIdentity()
				require.NoError(t, err)
				endpoint, err := runtimecheckpoint.NewPeerEndpoint("127.0.0.1:19001", cert)
				require.NoError(t, err)
				daemon.migrationPeer = &migrationPeer{identity: cert, endpoint: endpoint}
			}
			first, err := daemon.ReserveMigrationStaging(t.Context(), request)
			require.NoError(t, err)
			require.Equal(t, enabled, first.PeerCertificateSHA256 != "")
			require.Equal(t, enabled, first.Peer.Address != "")
			record, err := daemon.journal.Get(request.Target.SlotID)
			require.NoError(t, err)
			if enabled {
				require.Equal(t, runtimeSlotCapturePeerJournalVersion, record.Version)
				for _, version := range []int{runtimeSlotStagingJournalVersion, runtimeSlotCaptureUploadJournalVersion} {
					changed := record
					changed.Version = version
					payload, err := json.Marshal(changed)
					require.NoError(t, err)
					_, err = decodeRuntimeSlotJournalRecord(payload)
					require.ErrorContains(t, err, "non-downgradable")
				}
			}
			path := daemon.journal.db.Path()
			require.NoError(t, daemon.journal.Close())
			daemon.journal, err = newRuntimeSlotJournal(path, time.Hour)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, daemon.journal.Close()) })
			cert, err := runtimecheckpoint.NewPeerIdentity()
			require.NoError(t, err)
			endpoint, err := runtimecheckpoint.NewPeerEndpoint("127.0.0.1:19002", cert)
			require.NoError(t, err)
			daemon.migrationPeer = &migrationPeer{identity: cert, endpoint: endpoint}
			again, err := daemon.ReserveMigrationStaging(t.Context(), request)
			require.NoError(t, err)
			require.Equal(t, first, again, "a replacement daemon cannot replace a committed peer pin or enable an old reservation")
			daemon.migrationPeer = nil
			again, err = daemon.ReserveMigrationStaging(t.Context(), request)
			require.NoError(t, err)
			require.Equal(t, first, again)
			require.NoError(t, daemon.ReleaseMigrationStaging(t.Context(), request))
			record, err = daemon.journal.Get(request.Target.SlotID)
			require.NoError(t, err)
			require.True(t, record.MigrationStaging.Released)
			require.Equal(t, first.Peer, record.MigrationStaging.Peer)
		})
	}
}

func migrationStagingSourceRequest(t *testing.T, registration RuntimeSlotRegistration) protocol.MigrationStagingRequest {
	t.Helper()
	source := migrationJournalRequest(t, registration).Request
	return protocol.MigrationStagingRequest{Target: source.Target, Source: source,
		Destination: protocol.NodeChannelTarget{SlotID: "destination-slot", ClusterID: source.Target.ClusterID,
			NodeID: "destination-node", NodeUID: "destination-uid", NodeBootID: "destination-boot", AllocationID: "destination-allocation",
			ControlEndpoint: "unix:///private/destination.sock"}, DestinationResourceLeaseDigest: strings.Repeat("12", 32), Bytes: 8 << 20, Inodes: 64}
}

func migrationStagingReservationFixture(t *testing.T) (*nodeRuntime, protocol.MigrationStagingRequest) {
	t.Helper()
	j, err := newRuntimeSlotJournal(filepath.Join(t.TempDir(), "slots.db"), time.Hour)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, j.Close()) })
	r := testRuntimeSlotJournalRegistration(t, "staging-source")
	require.NoError(t, j.Register(r))
	request := migrationStagingSourceRequest(t, r)
	d := &nodeRuntime{journal: j, clusterID: r.ClusterID, nodeID: r.NodeID, nodeUID: request.Target.NodeUID, migrationStaging: testMigrationStagingGuard{}}
	return d, request
}

func stagingCapture(t *testing.T, request protocol.MigrationStagingRequest) protocol.MigrationCapture {
	t.Helper()
	digest, err := request.Source.Digest()
	require.NoError(t, err)
	return protocol.MigrationCapture{Request: request.Source, RequestDigest: digest, State: protocol.MigrationCaptureIntent}
}

func TestMigrationStagingReservationRecoversIntentAndLostReply(t *testing.T) {
	d, request := migrationStagingReservationFixture(t)
	d.migrationStaging = nil
	_, err := d.ReserveMigrationStaging(t.Context(), request)
	require.ErrorIs(t, err, errdefs.ErrUnavailable)
	record, err := d.journal.Get(request.Target.SlotID)
	require.NoError(t, err)
	require.Equal(t, runtimeSlotStagingJournalVersion, record.Version)
	require.False(t, record.MigrationStaging.Ready)
	require.ErrorIs(t, d.journal.RecordMigrationCapture(stagingCapture(t, request)), errdefs.ErrFailedPrecondition)

	ctx, cancel := context.WithCancel(t.Context())
	d.migrationStaging = testMigrationStagingGuard{budget: func(_ string, bytes int64, inodes uint64) error {
		require.Equal(t, request.Bytes, bytes)
		require.Equal(t, request.Inodes, inodes)
		cancel()
		return nil
	}}
	_, err = d.ReserveMigrationStaging(ctx, request)
	require.ErrorIs(t, err, context.Canceled)
	record, err = d.journal.Get(request.Target.SlotID)
	require.NoError(t, err)
	require.False(t, record.MigrationStaging.Ready)

	d.migrationStaging = testMigrationStagingGuard{}
	proof, err := d.ReserveMigrationStaging(t.Context(), request)
	require.NoError(t, err)
	require.NoError(t, proof.ValidateFor(request))
	path := d.journal.db.Path()
	require.NoError(t, d.journal.Close())
	d.journal, err = newRuntimeSlotJournal(path, time.Hour)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, d.journal.Close()) })
	d.migrationStaging = testMigrationStagingGuard{budget: func(string, int64, uint64) error {
		t.Fatal("lost reply must not charge the budget twice")
		return nil
	}}
	again, err := d.ReserveMigrationStaging(t.Context(), request)
	require.NoError(t, err)
	require.Equal(t, proof, again)
	pruned, err := d.journal.Prune(time.Now().Add(100 * time.Hour))
	require.NoError(t, err)
	require.Zero(t, pruned, "wall-clock expiry is not evidence of unused staging")

	changed := request
	changed.Bytes += 4096
	_, err = d.ReserveMigrationStaging(t.Context(), changed)
	require.ErrorIs(t, err, errdefs.ErrAlreadyExists)
	require.ErrorIs(t, d.ReleaseMigrationStaging(t.Context(), changed), errdefs.ErrFailedPrecondition)
	changed = request
	changed.Target.NodeUID = "different-uid"
	changed.Source.Target = changed.Target
	_, err = d.ReserveMigrationStaging(t.Context(), changed)
	require.ErrorIs(t, err, errdefs.ErrPermissionDenied)
	changed = request
	changed.Target.NodeBootID = "new-boot"
	changed.Source.Target = changed.Target
	_, err = d.ReserveMigrationStaging(t.Context(), changed)
	require.ErrorIs(t, err, errdefs.ErrFailedPrecondition)

	d.migrationStaging = nil
	require.NoError(t, d.ReleaseMigrationStaging(t.Context(), request), "release does not need new quota admission")
	require.NoError(t, d.ReleaseMigrationStaging(t.Context(), request))
	_, err = d.ReserveMigrationStaging(t.Context(), request)
	require.ErrorIs(t, err, errdefs.ErrFailedPrecondition, "a late reserve cannot revive canceled authority")
	require.ErrorIs(t, d.journal.RecordMigrationCapture(stagingCapture(t, request)), errdefs.ErrFailedPrecondition)

	next := request
	next.Source.OperationID = "next-migration"
	next.Source.LifecycleEpoch++
	d.migrationStaging = testMigrationStagingGuard{}
	_, err = d.ReserveMigrationStaging(t.Context(), next)
	require.NoError(t, err)
	require.ErrorIs(t, d.journal.RecordMigrationCapture(stagingCapture(t, request)), errdefs.ErrFailedPrecondition)
	require.NoError(t, d.journal.RecordMigrationCapture(stagingCapture(t, next)))
	require.ErrorIs(t, d.ReleaseMigrationStaging(t.Context(), next), errdefs.ErrFailedPrecondition, "capture custody pins exclusive staging")
}

func TestMigrationStagingReservationSerializesCompetingOperations(t *testing.T) {
	d, first := migrationStagingReservationFixture(t)
	requests := []protocol.MigrationStagingRequest{first}
	for i := range 11 {
		r := testRuntimeSlotJournalRegistration(t, fmt.Sprintf("competitor-%d", i))
		require.NoError(t, d.journal.Register(r))
		request := migrationStagingSourceRequest(t, r)
		request.Source.OperationID = fmt.Sprintf("operation-%d", i)
		requests = append(requests, request)
	}
	var wg sync.WaitGroup
	results := make(chan int, len(requests))
	failures := make(chan error, len(requests))
	start := make(chan struct{})
	for i, request := range requests {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, err := d.ReserveMigrationStaging(t.Context(), request)
			if err == nil {
				results <- i
			} else {
				failures <- err
			}
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	close(failures)
	require.Len(t, results, 1)
	winner := <-results
	for err := range failures {
		require.ErrorIs(t, err, errdefs.ErrResourceExhausted)
	}
	for i, request := range requests {
		if i != winner {
			require.ErrorIs(t, d.journal.RecordMigrationCapture(stagingCapture(t, request)), errdefs.ErrResourceExhausted, "legacy capture cannot bypass reserved admission")
		}
	}
	require.NoError(t, d.ReleaseMigrationStaging(t.Context(), requests[winner]))
	other := (winner + 1) % len(requests)
	require.NoError(t, d.journal.RecordMigrationCapture(stagingCapture(t, requests[other])))
	_, err := d.ReserveMigrationStaging(t.Context(), requests[other])
	require.ErrorIs(t, err, errdefs.ErrFailedPrecondition, "reservation cannot be backfilled after capture")
	third := (other + 1) % len(requests)
	if third == winner {
		third = (third + 1) % len(requests)
	}
	_, err = d.ReserveMigrationStaging(t.Context(), requests[third])
	require.ErrorIs(t, err, errdefs.ErrResourceExhausted)
}

func TestMigrationStagingReservationPinsDestinationUntilImageAbsence(t *testing.T) {
	d, image, _ := migrationImageDestinationFixture(t)
	resources, err := image.Resources.Digest()
	require.NoError(t, err)
	request := protocol.MigrationStagingRequest{Target: image.Target, Source: image.Publication.Capture.Request,
		Destination: image.Target, DestinationResourceLeaseDigest: strings.TrimPrefix(resources, "sha256:"), Bytes: 8 << 20, Inodes: 64}
	_, err = d.ReserveMigrationStaging(t.Context(), request)
	require.NoError(t, err)
	changed := image
	changed.Resources.MemoryBytes += 4096
	require.ErrorIs(t, d.journal.recordMigrationDestination(changed, nil), errdefs.ErrFailedPrecondition)
	prepared, err := d.PrepareMigrationImage(t.Context(), image)
	require.NoError(t, err)
	require.NoError(t, prepared.ValidateFor(image))
	require.ErrorIs(t, d.ReleaseMigrationStaging(t.Context(), request), errdefs.ErrFailedPrecondition)
	_, err = d.ReserveMigrationStaging(t.Context(), request)
	require.NoError(t, err, "exact reservation stays readable while downloaded bytes consume quota")
	record, err := d.journal.Get(image.Target.SlotID)
	require.NoError(t, err)
	require.Equal(t, runtimeSlotStagingJournalVersion, record.Version)
	for _, mutate := range []func(*runtimeSlotJournalRecord){
		func(r *runtimeSlotJournalRecord) { r.Version = runtimeSlotMigrationJournalVersion },
		func(r *runtimeSlotJournalRecord) { r.MigrationStaging.Ready = false },
		func(r *runtimeSlotJournalRecord) { r.MigrationStaging.Released = true },
		func(r *runtimeSlotJournalRecord) { r.MigrationStaging.Request.Bytes += 4096 },
		func(r *runtimeSlotJournalRecord) { r.MigrationStaging.Request.Source.ProcdInstanceID = "replacement" },
	} {
		copy := record
		staging := *record.MigrationStaging
		copy.MigrationStaging = &staging
		mutate(&copy)
		payload, err := json.Marshal(copy)
		require.NoError(t, err)
		_, err = decodeRuntimeSlotJournalRecord(payload)
		require.ErrorIs(t, err, errdefs.ErrFailedPrecondition)
	}
}

func TestMigrationStagingReservationReleaseFollowsSourcePhysicalFinalization(t *testing.T) {
	var reservation protocol.MigrationStagingRequest
	d, finalization, _, _ := migrationSourceFinalizeNodeFixture(t, func(d *nodeRuntime, capture protocol.MigrationCaptureRequest) {
		record, err := d.journal.Get(capture.Target.SlotID)
		require.NoError(t, err)
		reservation = migrationStagingSourceRequest(t, record.Registration)
		reservation.Source, reservation.Target = capture, capture.Target
		_, err = d.ReserveMigrationStaging(t.Context(), reservation)
		require.NoError(t, err)
	})
	require.ErrorIs(t, d.ReleaseMigrationStaging(t.Context(), reservation), errdefs.ErrFailedPrecondition)
	proof, err := d.FinalizeMigrationSource(t.Context(), finalization)
	require.NoError(t, err)
	require.True(t, proof.ImageAbsent)
	require.NoError(t, d.ReleaseMigrationStaging(t.Context(), reservation))
	record, err := d.journal.Get(reservation.Target.SlotID)
	require.NoError(t, err)
	require.Equal(t, runtimeSlotStagingJournalVersion, record.Version)
	require.True(t, record.MigrationStaging.Released)
	require.NotNil(t, record.Migration.Finalization)
	require.NotNil(t, record.Proof)
	receipt, err := d.GetMigrationSourceFinalization(t.Context(), reservation.Target.SlotID)
	require.NoError(t, err)
	require.Equal(t, *proof, receipt.Proof)
	_, err = d.ReserveMigrationStaging(t.Context(), reservation)
	require.ErrorIs(t, err, errdefs.ErrFailedPrecondition)
}

func TestMigrationStagingReservationSurvivesAdoptionAndSubsequentMigration(t *testing.T) {
	var reservation protocol.MigrationStagingRequest
	d, adoption, _, _ := migrationAdoptionNodeFixture(t, func(d *nodeRuntime, image protocol.MigrationImagePrepareRequest) {
		resources, err := image.Resources.Digest()
		require.NoError(t, err)
		reservation = protocol.MigrationStagingRequest{Target: image.Target, Source: image.Publication.Capture.Request,
			Destination: image.Target, DestinationResourceLeaseDigest: strings.TrimPrefix(resources, "sha256:"), Bytes: 8 << 20, Inodes: 64}
		_, err = d.ReserveMigrationStaging(t.Context(), reservation)
		require.NoError(t, err)
	})
	require.ErrorIs(t, d.ReleaseMigrationStaging(t.Context(), reservation), errdefs.ErrFailedPrecondition)
	_, err := d.AdoptMigrationDestination(t.Context(), adoption)
	require.NoError(t, err)
	require.NoError(t, d.ReleaseMigrationStaging(t.Context(), reservation))
	record, err := d.journal.Get(reservation.Target.SlotID)
	require.NoError(t, err)
	require.Equal(t, runtimeSlotStagingJournalVersion, record.Version)
	require.True(t, record.MigrationStaging.Released)
	require.True(t, record.MigrationDestination.Adopted())
	restore := record.MigrationDestination.Restore.Request
	binding, err := restore.Stage.BindingDigest()
	require.NoError(t, err)
	revision, err := restore.Image.Publication.Assignment.Target.Revision()
	require.NoError(t, err)
	resources, err := restore.Image.Resources.Digest()
	require.NoError(t, err)
	next := migrationStagingSourceRequest(t, record.Registration)
	next.Source = protocol.MigrationCaptureRequest{Target: adoption.Target, OperationID: "next-migration", LifecycleEpoch: 4,
		SandboxID: adoption.SandboxID, SourceGeneration: adoption.RuntimeGeneration, ProcdInstanceID: adoption.ProcdInstanceID,
		AssignmentRevision: revision, BindingDigest: fmt.Sprintf("%x", binding), ResourceLeaseDigest: strings.TrimPrefix(resources, "sha256:")}
	next.Target = next.Source.Target
	_, err = d.ReserveMigrationStaging(t.Context(), next)
	require.NoError(t, err)
	require.NoError(t, d.journal.RecordMigrationCapture(stagingCapture(t, next)))
	require.NoError(t, d.ReleaseMigrationStaging(t.Context(), reservation), "previous release is acknowledged without changing newer custody")
	_, err = d.PrepareMigrationImage(t.Context(), restore.Image)
	require.Error(t, err, "late previous download cannot revive staging")
	record, err = d.journal.Get(next.Target.SlotID)
	require.NoError(t, err)
	require.Equal(t, next.Source, record.Migration.Capture.Request)
	require.True(t, record.MigrationDestination.Adopted())
}

func TestMigrationStagingReservationCannotReleaseDuringDownload(t *testing.T) {
	d, image, runtime := migrationImageDestinationFixture(t)
	resources, err := image.Resources.Digest()
	require.NoError(t, err)
	request := protocol.MigrationStagingRequest{Target: image.Target, Source: image.Publication.Capture.Request,
		Destination: image.Target, DestinationResourceLeaseDigest: strings.TrimPrefix(resources, "sha256:"), Bytes: 8 << 20, Inodes: 64}
	_, err = d.ReserveMigrationStaging(t.Context(), request)
	require.NoError(t, err)
	runtime.started, runtime.resume = make(chan struct{}), make(chan struct{})
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { _, err := d.PrepareMigrationImage(ctx, image); done <- err }()
	select {
	case <-runtime.started:
	case <-time.After(5 * time.Second):
		t.Fatal("image writer did not start")
	}
	require.ErrorIs(t, d.ReleaseMigrationStaging(t.Context(), request), errdefs.ErrUnavailable)
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
	require.ErrorIs(t, d.ReleaseMigrationStaging(t.Context(), request), errdefs.ErrUnavailable, "caller disconnect cannot release an active writer")
	close(runtime.resume)
	d.wg.Wait()
	require.ErrorIs(t, d.ReleaseMigrationStaging(t.Context(), request), errdefs.ErrFailedPrecondition)
}

func TestMigrationStagingCancellationOvertakesReserveWithoutRevival(t *testing.T) {
	d, request := migrationStagingReservationFixture(t)
	d.migrationStaging = nil
	require.NoError(t, d.ReleaseMigrationStaging(t.Context(), request))
	record, err := d.journal.Get(request.Target.SlotID)
	require.NoError(t, err)
	require.True(t, record.MigrationStaging.Released)
	require.False(t, record.MigrationStaging.Ready)
	require.Equal(t, runtimeSlotStagingJournalVersion, record.Version)
	path := d.journal.db.Path()
	require.NoError(t, d.journal.Close())
	d.journal, err = newRuntimeSlotJournal(path, time.Hour)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, d.journal.Close()) })
	_, err = d.ReserveMigrationStaging(t.Context(), request)
	require.ErrorIs(t, err, errdefs.ErrFailedPrecondition)
	require.ErrorIs(t, d.journal.RecordMigrationCapture(stagingCapture(t, request)), errdefs.ErrFailedPrecondition)
	next := request
	next.Source.OperationID = "next-migration"
	for _, epoch := range []int64{request.Source.LifecycleEpoch - 1, request.Source.LifecycleEpoch} {
		next.Source.LifecycleEpoch = epoch
		_, err = d.ReserveMigrationStaging(t.Context(), next)
		require.ErrorIs(t, err, errdefs.ErrAlreadyExists)
		if epoch < request.Source.LifecycleEpoch {
			require.NoError(t, d.ReleaseMigrationStaging(t.Context(), next), "superseded release is safe to acknowledge")
		} else {
			require.ErrorIs(t, d.ReleaseMigrationStaging(t.Context(), next), errdefs.ErrFailedPrecondition)
		}
	}
	next.Source.LifecycleEpoch = request.Source.LifecycleEpoch + 1
	require.NoError(t, d.ReleaseMigrationStaging(t.Context(), next), "new cancellation may also arrive before its reserve")
	_, err = d.ReserveMigrationStaging(t.Context(), request)
	require.ErrorIs(t, err, errdefs.ErrAlreadyExists, "old reserve must not replace the newer tombstone")
	require.NoError(t, d.ReleaseMigrationStaging(t.Context(), request))
	latest := next
	latest.Source.OperationID = "latest-migration"
	latest.Source.LifecycleEpoch++
	d.migrationStaging = testMigrationStagingGuard{}
	_, err = d.ReserveMigrationStaging(t.Context(), latest)
	require.NoError(t, err)
	require.NoError(t, d.ReleaseMigrationStaging(t.Context(), next))
	record, err = d.journal.Get(latest.Target.SlotID)
	require.NoError(t, err)
	require.True(t, record.MigrationStaging.Ready)
	require.False(t, record.MigrationStaging.Released)
	require.Equal(t, latest, record.MigrationStaging.Request)
}

func TestMigrationStagingDestinationCancellationRejectsDelayedDownload(t *testing.T) {
	d, image, _ := migrationImageDestinationFixture(t)
	resources, err := image.Resources.Digest()
	require.NoError(t, err)
	request := protocol.MigrationStagingRequest{Target: image.Target, Source: image.Publication.Capture.Request,
		Destination: image.Target, DestinationResourceLeaseDigest: strings.TrimPrefix(resources, "sha256:"), Bytes: 8 << 20, Inodes: 64}
	require.NoError(t, d.ReleaseMigrationStaging(t.Context(), request))
	_, err = d.ReserveMigrationStaging(t.Context(), request)
	require.ErrorIs(t, err, errdefs.ErrFailedPrecondition)
	_, err = d.PrepareMigrationImage(t.Context(), image)
	require.ErrorIs(t, err, errdefs.ErrFailedPrecondition)
	record, err := d.journal.Get(image.Target.SlotID)
	require.NoError(t, err)
	require.Nil(t, record.MigrationDestination)
	require.True(t, record.MigrationStaging.Released)
	next := request
	next.Source.OperationID = "new-destination-migration"
	next.Source.LifecycleEpoch++
	_, err = d.ReserveMigrationStaging(t.Context(), next)
	require.ErrorIs(t, err, errdefs.ErrAlreadyExists, "a canceled destination carrier is one-shot")
}

func TestMigrationStagingReleaseAfterUnusedCarrierCleanup(t *testing.T) {
	d, request := migrationStagingReservationFixture(t)
	record, err := d.journal.Get(request.Target.SlotID)
	require.NoError(t, err)
	cleanup := testRuntimeSlotJournalCleanup(record.Registration)
	_, err = d.journal.BeginCleanup(cleanup)
	require.NoError(t, err)
	require.ErrorIs(t, d.ReleaseMigrationStaging(t.Context(), request), errdefs.ErrFailedPrecondition, "cleanup intent alone is not physical absence")
	require.NoError(t, d.journal.CompleteCleanup(cleanup, testRuntimeSlotJournalProof(t, cleanup)))
	require.NoError(t, d.ReleaseMigrationStaging(t.Context(), request))
	_, err = d.ReserveMigrationStaging(t.Context(), request)
	require.ErrorIs(t, err, errdefs.ErrFailedPrecondition)
	record, err = d.journal.Get(request.Target.SlotID)
	require.NoError(t, err)
	require.Nil(t, record.MigrationStaging)
	require.NotNil(t, record.Proof)
}
