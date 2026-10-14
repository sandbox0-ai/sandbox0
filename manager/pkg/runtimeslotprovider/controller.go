// Package runtimeslotprovider recovers a fenced consumed writer after the
// cloud proves its original compute instance is gone.
package runtimeslotprovider

import (
	"context"
	"errors"
	"strings"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/runtimeslotreconciler"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/pkg/providerinstance"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
)

type Store interface {
	GetRootFSWriterGrant(context.Context, string) (*sandboxstore.RootFSWriterGrant, error)
	RecordRuntimeProviderInstanceObservation(context.Context, sandboxstore.RuntimeProviderTarget, providerinstance.Observation) (*sandboxstore.RuntimeProviderAbsenceProof, error)
	CompleteRuntimeSlotProviderAbandon(context.Context, *sandboxstore.CompleteRuntimeSlotProviderAbandonRequest) (*sandboxstore.RuntimeSlot, error)
}

type Nomad interface {
	RetireProviderAbsent(context.Context, runtimeslotreconciler.AllocationPurgeRequest, []byte) ([]byte, error)
}

type Controller struct {
	store             Store
	writer            runtimeslotreconciler.WriterController
	nomad             Nomad
	observer          providerinstance.Observer
	accountID, region string
}

func New(store Store, writer runtimeslotreconciler.WriterController, nomad Nomad, observer providerinstance.Observer, accountID, region string) (*Controller, error) {
	if store == nil || writer == nil || nomad == nil || observer == nil || accountID == "" || region == "" {
		return nil, errors.New("provider recovery requires pinned observer and regional writer/store/Nomad authorities")
	}
	return &Controller{store: store, writer: writer, nomad: nomad, observer: observer, accountID: accountID, region: region}, nil
}

func (c *Controller) Recover(ctx context.Context, slot sandboxstore.RuntimeSlot) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if slot.State != sandboxstore.RuntimeSlotStateQuiescing && slot.State != sandboxstore.RuntimeSlotStateOrphaned {
		return false, nil
	}
	parts := strings.Split(slot.NodeUID, "/")
	if len(parts) != 3 || parts[0] != "ecs" || parts[1] != c.region {
		return false, nil
	}
	if slot.WriterGrantID == "" {
		return false, nil
	} // Unclaimed warm recovery remains separate.
	scope := providerinstance.Scope{Provider: "aliyun", AccountID: c.accountID, RegionID: c.region, InstanceID: parts[2]}
	observation, err := c.observer.Observe(ctx, scope)
	if err != nil {
		return false, err
	}
	if err = observation.ValidateFor(scope); err != nil {
		return false, err
	}
	target := sandboxstore.RuntimeProviderTarget{ClusterID: slot.ClusterID, NodeID: slot.NodeID, NodeUID: slot.NodeUID, NodeBootID: slot.NodeBootID}
	proof, err := c.store.RecordRuntimeProviderInstanceObservation(ctx, target, observation)
	if err != nil {
		return false, err
	}
	if proof == nil {
		return false, nil
	}
	if proof.Target != target || proof.Scope != scope {
		return false, errors.New("persisted provider proof changed the requested node scope")
	}
	digest, err := proof.Digest()
	if err != nil {
		return false, err
	}
	grant, err := c.store.GetRootFSWriterGrant(ctx, slot.WriterGrantID)
	if err != nil {
		return false, err
	}
	if grant == nil || grant.SlotID != slot.ID || grant.NodeUID != slot.NodeUID || grant.NodeBootID != slot.NodeBootID ||
		grant.SandboxID != slot.SandboxID || grant.ClaimID != slot.ClaimID || grant.FilesystemID != slot.FilesystemID ||
		grant.InitialGenerationID != slot.SourceGenerationID || grant.RuntimeNamespace != slot.AllocationNamespace || grant.RuntimeIncarnationID != slot.AllocationID {
		return false, errors.New("provider recovery changed the exact writer/slot incarnation")
	}
	operation := rootfshandoff.CrashRetireOperationID(grant.GateParent, grant.ID, grant.WriterEpoch)
	if _, err = c.writer.Fence(ctx, runtimeslotreconciler.WriterFenceRequest{OperationID: operation, IssueOperationID: grant.IssueOperationID,
		SlotID: slot.ID, SandboxID: slot.SandboxID, ClaimID: slot.ClaimID, GrantID: grant.ID, WriterEpoch: grant.WriterEpoch,
		BindingVersion: grant.BindingVersion, BindingDigest: grant.BindingDigest, NodeUID: grant.NodeUID, NodeBootID: grant.NodeBootID, InitialGenerationID: grant.InitialGenerationID}); err != nil {
		return false, err
	}
	allocationDigest, err := c.nomad.RetireProviderAbsent(ctx, runtimeslotreconciler.AllocationPurgeRequest{OperationID: operation,
		Target: runtimeslotreconciler.AllocationTarget{ClusterID: slot.ClusterID, AllocationID: slot.AllocationID, AllocationNamespace: slot.AllocationNamespace, NodeID: slot.NodeID}}, digest)
	if err != nil {
		return false, err
	}
	terminal, err := c.store.CompleteRuntimeSlotProviderAbandon(ctx, &sandboxstore.CompleteRuntimeSlotProviderAbandonRequest{SlotID: slot.ID, Proof: *proof, AllocationObservationDigest: allocationDigest,
		Writer: sandboxstore.CompleteRootFSWriterCrashAbandonRequest{LifecycleTxnID: operation, GrantID: grant.ID, WriterEpoch: grant.WriterEpoch, OperationID: operation,
			BindingVersion: grant.BindingVersion, BindingDigest: grant.BindingDigest, ProofVersion: sandboxstore.RootFSWriterProviderAbandonProofVersion, ProofDigest: digest,
			NodeUID: slot.NodeUID, NodeBootID: slot.NodeBootID, ExpectedOldGenerationID: grant.InitialGenerationID}})
	if err != nil {
		return false, err
	}
	if terminal == nil || terminal.ID != slot.ID || terminal.State != sandboxstore.RuntimeSlotStateTerminal {
		return false, errors.New("provider recovery did not persist exact terminal slot")
	}
	return true, nil
}
