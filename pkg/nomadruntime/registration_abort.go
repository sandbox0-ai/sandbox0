package nomadruntime

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/containerd/errdefs"
	authority "github.com/sandbox0-ai/sandbox0/manager/pkg/runtimeslotauthority"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
)

type registrationAbortAuthority interface {
	AbortRegistration(context.Context, protocol.RegistrationAbortRequest) (protocol.RegistrationAbortResponse, error)
}

func newRegistrationAbortAuthority(config Config) (registrationAbortAuthority, error) {
	return authority.NewClient(authority.ClientConfig{
		BaseURL: config.RootFSAuthorityURL, CAFile: config.RootFSAuthorityCAFile,
		ClientCertFile: config.RootFSAuthorityClientCertFile, ClientKeyFile: config.RootFSAuthorityClientKeyFile,
		TokenFile: config.RootFSAuthorityTokenFile, Timeout: 2 * time.Second,
	})
}

// reconcileRegistrationLoop owns one bounded lane independent of writer
// recovery. Unknown regional state never authorizes local cleanup. The region
// fences insertion first; normal slot cleanup remains with its existing owner.
func (d *nodeRuntime) reconcileRegistrationLoop(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			pass, cancel := context.WithTimeout(ctx, 20*time.Second)
			completed, err := d.reconcileRegistrations(pass, now)
			cancel()
			if err != nil && ctx.Err() == nil {
				d.logger.Error("reconcile runtime slot registration journals", "completed", completed, "error", err)
			} else if completed > 0 {
				d.logger.Info("reconciled runtime slot registration journals", "completed", completed)
			}
		}
	}
}

func (d *nodeRuntime) reconcileRegistrations(ctx context.Context, now time.Time) (int, error) {
	if d.registrationAuthority == nil || d.journal == nil {
		return 0, errdefs.ErrUnavailable
	}
	rows, next, err := d.journal.registrationAbortCandidates(d.registrationAfter, now)
	if err != nil {
		return 0, err
	}
	d.registrationAfter = next
	completed := 0
	var failures []error
	for _, record := range rows {
		if err := ctx.Err(); err != nil {
			return completed, errors.Join(append(failures, err)...)
		}
		r := record.Registration
		if r.ClusterID != d.clusterID || r.NodeID != d.nodeID {
			failures = append(failures, fmt.Errorf("registration journal belongs to another node: %w", errdefs.ErrFailedPrecondition))
			continue
		}
		request := protocol.RegistrationAbortRequest{SlotID: r.SlotID, ClusterID: r.ClusterID, AllocationID: r.AllocationID, NodeID: r.NodeID, NodeUID: d.nodeUID, NodeBootID: r.NodeBootID, NetNSIdentity: r.NetNSIdentity}
		if err := d.reconcileRegistration(ctx, record, request); err != nil {
			failures = append(failures, fmt.Errorf("reconcile registration %s: %w", r.SlotID, err))
			continue
		}
		completed++
	}
	return completed, errors.Join(failures...)
}

func (d *nodeRuntime) reconcileRegistration(ctx context.Context, record runtimeSlotJournalRecord, request protocol.RegistrationAbortRequest) error {
	response, err := d.registrationAuthority.AbortRegistration(ctx, request)
	if err != nil {
		return err
	}
	if err := response.ValidateFor(request); err != nil {
		return err
	}
	if response.Registered {
		return d.journal.acknowledgeRegionalRegistration(record.Registration)
	}
	cleanupCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	proof, err := d.CleanupRuntimeSlot(cleanupCtx, *response.Cleanup)
	cancel()
	if err != nil {
		return err
	}
	request.Proof = &proof
	response, err = d.registrationAuthority.AbortRegistration(ctx, request)
	if err != nil {
		return err
	}
	if err := response.ValidateFor(request); err != nil {
		return err
	}
	return d.journal.acknowledgeRegistrationAbort(proof)
}
