// Package runtimeslotnomad implements plugin-independent Nomad allocation
// retirement for the runtime-slot reconciler.
package runtimeslotnomad

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/containerd/errdefs"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/runtimeslotreconciler"
)

const allocationObservationVersion = 1

// Allocation is the bounded server-catalog identity needed before a direct
// Nomad client operation is allowed.
type Allocation struct {
	ID            string `json:"ID"`
	Namespace     string `json:"Namespace"`
	NodeID        string `json:"NodeID"`
	DesiredStatus string `json:"DesiredStatus"`
}

// API separates Nomad protocol details from reconciliation ordering. The
// implementation must address the exact client node instead of accepting a
// server-catalog record as evidence of physical absence.
type API interface {
	ServerAllocation(context.Context, runtimeslotreconciler.AllocationTarget) (*Allocation, error)
	ClientAllocationPresent(context.Context, runtimeslotreconciler.AllocationTarget) (bool, error)
	StopAllocation(context.Context, runtimeslotreconciler.AllocationTarget, string) error
	GarbageCollectAllocation(context.Context, runtimeslotreconciler.AllocationTarget) error
}

// Controller implements the terminal reconciler's Nomad boundary.
type Controller struct {
	api API
}

var _ runtimeslotreconciler.AllocationController = (*Controller)(nil)

// New constructs a direct Nomad allocation controller.
func New(api API) (*Controller, error) {
	if api == nil {
		return nil, errors.New("Nomad allocation API is required")
	}
	return &Controller{api: api}, nil
}

// Observe requires both a matching server identity, when one still exists,
// and a direct client allocation-directory observation. A stopped server
// record does not keep the allocation physically present once exact client
// state is absent.
func (c *Controller) Observe(
	ctx context.Context,
	target runtimeslotreconciler.AllocationTarget,
) (runtimeslotreconciler.AllocationObservation, error) {
	if err := validateTarget(target); err != nil {
		return runtimeslotreconciler.AllocationObservation{}, err
	}
	allocation, err := c.api.ServerAllocation(ctx, target)
	if err != nil {
		return runtimeslotreconciler.AllocationObservation{}, fmt.Errorf("read Nomad server allocation: %w", err)
	}
	serverOwnsAllocation := false
	if allocation != nil {
		if err := validateAllocation(*allocation, target); err != nil {
			return runtimeslotreconciler.AllocationObservation{}, err
		}
		serverOwnsAllocation = allocation.DesiredStatus == "run"
	}
	clientPresent, err := c.api.ClientAllocationPresent(ctx, target)
	if err != nil {
		return runtimeslotreconciler.AllocationObservation{}, fmt.Errorf("observe direct Nomad client allocation: %w", err)
	}
	present := serverOwnsAllocation || clientPresent
	proof, err := allocationProof(target, present)
	if err != nil {
		return runtimeslotreconciler.AllocationObservation{}, err
	}
	return runtimeslotreconciler.AllocationObservation{
		Target: target, PhysicalPresent: present, ProofDigest: proof,
	}, nil
}

// Purge first disables server scheduling ownership with an idempotency token,
// then invokes the exact client's synchronous allocation GC endpoint. A retry
// after successful client GC observes an already absent allocation as success.
func (c *Controller) Purge(
	ctx context.Context,
	request runtimeslotreconciler.AllocationPurgeRequest,
) error {
	if err := c.Stop(ctx, request); err != nil {
		return err
	}
	if err := c.api.GarbageCollectAllocation(ctx, request.Target); err != nil {
		return fmt.Errorf("garbage collect direct Nomad client allocation: %w", err)
	}
	return nil
}

// Stop durably removes Nomad server scheduling ownership without forcing
// client GC. Planned pause uses this boundary so the task driver can publish
// its sealed RootFS generation before terminal reconciliation purges the
// allocation directory.
func (c *Controller) Stop(
	ctx context.Context,
	request runtimeslotreconciler.AllocationPurgeRequest,
) error {
	if err := validateOperationID(request.OperationID); err != nil {
		return err
	}
	if err := validateTarget(request.Target); err != nil {
		return err
	}
	allocation, err := c.api.ServerAllocation(ctx, request.Target)
	if err != nil {
		return fmt.Errorf("read Nomad server allocation before purge: %w", err)
	}
	if allocation != nil {
		if err := validateAllocation(*allocation, request.Target); err != nil {
			return err
		}
		if allocation.DesiredStatus == "run" {
			if err := c.api.StopAllocation(ctx, request.Target, request.OperationID); err != nil {
				return fmt.Errorf("stop Nomad server allocation: %w", err)
			}
		}
	}
	return nil
}

func validateTarget(target runtimeslotreconciler.AllocationTarget) error {
	fields := []struct{ name, value string }{
		{name: "cluster_id", value: target.ClusterID},
		{name: "allocation_id", value: target.AllocationID},
		{name: "allocation_namespace", value: target.AllocationNamespace},
		{name: "node_id", value: target.NodeID},
	}
	for _, field := range fields {
		if err := validateID(field.name, field.value); err != nil {
			return err
		}
	}
	return nil
}

func validateOperationID(operationID string) error {
	return validateID("operation_id", operationID)
}

func validateID(name, value string) error {
	if value == "" || strings.TrimSpace(value) != value || len(value) > 512 || strings.ContainsAny(value, "/\\?#\x00") {
		return fmt.Errorf("%s is required, canonical, and at most 512 bytes: %w", name, errdefs.ErrInvalidArgument)
	}
	return nil
}

func validateAllocation(allocation Allocation, target runtimeslotreconciler.AllocationTarget) error {
	if allocation.ID != target.AllocationID || allocation.Namespace != target.AllocationNamespace ||
		allocation.NodeID != target.NodeID {
		return fmt.Errorf("Nomad server allocation belongs to another target: %w", errdefs.ErrFailedPrecondition)
	}
	switch allocation.DesiredStatus {
	case "run", "stop", "evict":
		return nil
	default:
		return fmt.Errorf("Nomad server allocation has invalid desired status %q: %w", allocation.DesiredStatus, errdefs.ErrFailedPrecondition)
	}
}

func allocationProof(target runtimeslotreconciler.AllocationTarget, present bool) ([]byte, error) {
	payload, err := json.Marshal(struct {
		Version             int    `json:"version"`
		Evidence            string `json:"evidence"`
		ClusterID           string `json:"cluster_id"`
		AllocationID        string `json:"allocation_id"`
		AllocationNamespace string `json:"allocation_namespace"`
		NodeID              string `json:"node_id"`
		PhysicalPresent     bool   `json:"physical_present"`
	}{
		Version: allocationObservationVersion, Evidence: "direct-nomad-client-allocation-directory",
		ClusterID: target.ClusterID, AllocationID: target.AllocationID,
		AllocationNamespace: target.AllocationNamespace, NodeID: target.NodeID,
		PhysicalPresent: present,
	})
	if err != nil {
		return nil, fmt.Errorf("encode Nomad allocation observation: %w", err)
	}
	digest := sha256.Sum256(payload)
	return digest[:], nil
}
