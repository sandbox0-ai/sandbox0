package nomadruntime

import (
	"context"
	"crypto/x509"
	"io"
	"sync"
	"time"

	"github.com/containerd/errdefs"
	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecheckpoint"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
)

type migrationPlannedImageRuntime interface {
	PlanMigrationImage(context.Context, runtimecheckpoint.Binding, string) (runtimecheckpoint.LocalImagePlan, error)
	PublishPlannedMigrationImage(context.Context, runtimecheckpoint.Binding, runtimecheckpoint.LocalImagePlan, string) (runtimecheckpoint.Reference, error)
	WritePlannedMigrationPeerImage(context.Context, runtimecheckpoint.Binding, runtimecheckpoint.LocalImagePlan, string, io.Writer) error
}

type MigrationPublicationPlanner interface {
	PlanMigrationPublication(context.Context, protocol.MigrationPublicationRequest) (*protocol.MigrationPublicationPlan, error)
}

// PlanMigrationPublication starts or joins the same authorized upload used by
// PublishMigration, but returns its prospective reference as soon as the local
// image is hashed. Disconnecting this observer does not abort durable work.
func (d *nodeRuntime) PlanMigrationPublication(ctx context.Context, request protocol.MigrationPublicationRequest) (*protocol.MigrationPublicationPlan, error) {
	if d == nil || d.migrationPeer == nil || request.DestinationPeerCertificateSHA256 == "" {
		return nil, errdefs.ErrFailedPrecondition
	}
	if _, ok := d.runtime.(migrationPlannedImageRuntime); !ok {
		return nil, errdefs.ErrFailedPrecondition
	}
	worker, err := d.startMigrationPublication(ctx, request)
	if err != nil {
		return nil, err
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-worker.planReady:
		copy := *worker.plan
		return &copy, copy.ValidateFor(request)
	case <-worker.done:
		if worker.err != nil {
			return nil, worker.err
		}
		if worker.result == nil {
			return nil, errdefs.ErrUnavailable
		}
		// An acknowledged upload can be observed after worker replacement or
		// node restart. It still returns only a plan on this operation kind.
		p := worker.result
		plan := &protocol.MigrationPublicationPlan{RequestDigest: p.RequestDigest,
			Binding: p.Binding, Reference: p.Reference, Peer: p.Peer}
		return plan, plan.ValidateFor(request)
	}
}

func (r *rootfsRuntime) PlanMigrationImage(ctx context.Context, binding runtimecheckpoint.Binding, directory string) (runtimecheckpoint.LocalImagePlan, error) {
	if r == nil || r.checkpoints == nil {
		return runtimecheckpoint.LocalImagePlan{}, errdefs.ErrUnavailable
	}
	if err := ctx.Err(); err != nil {
		return runtimecheckpoint.LocalImagePlan{}, err
	}
	inventory, found := r.takeMigrationInventory(directory)
	if found {
		return r.checkpoints.BindLocalInventory(binding, inventory)
	}
	return r.checkpoints.PlanLocal(ctx, binding, directory)
}

func (r *rootfsRuntime) takeMigrationInventory(directory string) (runtimecheckpoint.LocalImageInventory, bool) {
	r.migrationInventoryMu.Lock()
	defer r.migrationInventoryMu.Unlock()
	inventory, found := r.migrationInventories[directory]
	delete(r.migrationInventories, directory)
	return inventory, found
}

// PrimeMigrationImageInventory is only a disposable local optimization. The
// enclosing node reconciliation owns stopped source custody while it hashes.
// A miss, eviction or daemon restart uses the normal full planning scan.
func (r *rootfsRuntime) PrimeMigrationImageInventory(ctx context.Context, directory string) error {
	if r == nil || r.checkpoints == nil {
		return errdefs.ErrUnavailable
	}
	inventory, err := r.checkpoints.InspectLocal(ctx, directory)
	if err != nil {
		return err
	}
	return r.retainMigrationImageInventory(ctx, directory, inventory)
}

// PrimeMigrationCaptureInventory uses the existing regional capture budget to
// stage final chunks while the independent RootFS cut is being made durable.
// Neither the inventory nor these chunks can authorize target execution.
func (r *rootfsRuntime) PrimeMigrationCaptureInventory(ctx context.Context, stage *runtimecheckpoint.CaptureStager, directory string) error {
	if r == nil || r.checkpoints == nil || stage == nil {
		return errdefs.ErrUnavailable
	}
	inventory, err := stage.InspectAndStageLocal(ctx, directory)
	if err != nil {
		return err
	}
	return r.retainMigrationImageInventory(ctx, directory, inventory)
}

func (r *rootfsRuntime) retainMigrationImageInventory(ctx context.Context, directory string, inventory runtimecheckpoint.LocalImageInventory) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	r.migrationInventoryMu.Lock()
	defer r.migrationInventoryMu.Unlock()
	if len(r.migrationInventories) >= maxMigrationImageCustodies {
		r.migrationInventories = nil
	}
	if r.migrationInventories == nil {
		r.migrationInventories = make(map[string]runtimecheckpoint.LocalImageInventory)
	}
	r.migrationInventories[directory] = inventory
	return nil
}

func (r *rootfsRuntime) PublishPlannedMigrationImage(ctx context.Context, binding runtimecheckpoint.Binding, plan runtimecheckpoint.LocalImagePlan, directory string) (runtimecheckpoint.Reference, error) {
	if r == nil || r.checkpoints == nil {
		return runtimecheckpoint.Reference{}, errdefs.ErrUnavailable
	}
	return r.checkpoints.PublishPlanned(ctx, binding, plan, directory)
}

func (r *rootfsRuntime) WritePlannedMigrationPeerImage(ctx context.Context, binding runtimecheckpoint.Binding, plan runtimecheckpoint.LocalImagePlan, directory string, output io.Writer) error {
	if r == nil || r.checkpoints == nil {
		return errdefs.ErrUnavailable
	}
	return r.checkpoints.WritePlannedPeerImage(ctx, binding, plan, directory, output)
}

// A read scope borrows the publication worker's exclusive slot custody. All
// fields except readers are immutable after exposure under nodeRuntime.mu.
// Clearing worker.peer under that same mutex closes admission before Wait.
type migrationPublicationPeerRead struct {
	ctx         context.Context
	plan        runtimecheckpoint.LocalImagePlan
	directory   string
	certificate string
	readers     sync.WaitGroup
}

func (d *nodeRuntime) shareMigrationPublication(ctx context.Context, worker *migrationPublicationWorker, request protocol.MigrationPublicationRequest, plan runtimecheckpoint.LocalImagePlan, directory string) func(bool) {
	ctx, cancel := context.WithCancel(ctx)
	scope := &migrationPublicationPeerRead{ctx: ctx, plan: plan, directory: directory,
		certificate: request.DestinationPeerCertificateSHA256}
	d.mu.Lock()
	slot := request.Capture.Request.Target.SlotID
	custody := d.inflight[slot]
	worker.peer = scope
	worker.plan = &protocol.MigrationPublicationPlan{RequestDigest: worker.digest,
		Binding: plan.Manifest.Binding, Reference: plan.Reference, Peer: d.migrationPeer.endpoint}
	close(worker.planReady)
	d.mu.Unlock()
	return func(published bool) {
		d.mu.Lock()
		worker.peer = nil
		// Once durable publication has finished, only disposable readers can
		// keep the slot busy. Let authorized cleanup preempt them, preserving
		// the existing cancel-and-join reconciliation protocol.
		if published && custody != nil && d.inflight[slot] == custody {
			custody.cancel = cancel
		}
		d.mu.Unlock()
		// Failed publication must promptly interrupt even a non-reading peer.
		// Successful publication lets an already admitted stream finish within
		// its bounded deadline before the caller releases the source slot.
		if !published {
			cancel()
		}
		scope.readers.Wait()
		cancel()
	}
}

func (d *nodeRuntime) borrowMigrationPublication(read migrationPeerRead, certificate *x509.Certificate) (*migrationPublicationPeerRead, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	worker := d.migrationPublications[read.SlotID]
	if worker == nil || worker.peer == nil {
		return nil, errdefs.ErrNotFound
	}
	if worker.digest != read.RequestDigest {
		return nil, errdefs.ErrPermissionDenied
	}
	scope := worker.peer
	now := time.Now()
	if scope.ctx.Err() != nil || scope.certificate != digest.FromBytes(certificate.Raw).String() ||
		now.Before(certificate.NotBefore) || now.After(certificate.NotAfter) {
		return nil, errdefs.ErrPermissionDenied
	}
	scope.readers.Add(1)
	return scope, nil
}
