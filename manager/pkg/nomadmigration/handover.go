package nomadmigration

import (
	"context"
	"errors"

	"github.com/sandbox0-ai/sandbox0/pkg/procdapi"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
)

// Handover joins the completed restore with its immutable procd command.
// Receipt is present only after the region commits procd's acknowledgement.
type Handover struct {
	Restored protocol.MigrationRestoreObservation
	Command  procdapi.RuntimeMigrationRequest
	Receipt  *procdapi.RuntimeMigrationResponse
}

func (h Handover) Validate() error {
	if h.Restored.Validate() != nil || h.Restored.State != protocol.MigrationRestoreComplete {
		return errors.New("handover requires completed restore evidence")
	}
	if _, err := h.Command.Digest(); err != nil {
		return err
	}
	image := h.Restored.Request.Image
	want, _ := image.Publication.Assignment.Digest()
	actual, _ := h.Command.Assignment.Digest()
	if want != actual || h.Command.Action != procdapi.MigrationRestore ||
		h.Command.InstanceID != image.Publication.Capture.Request.ProcdInstanceID ||
		h.Command.LifecycleEpoch != image.Publication.Capture.Request.LifecycleEpoch {
		return errors.New("handover changed restored process authority")
	}
	if h.Receipt != nil {
		return h.Receipt.ValidateFor(h.Command)
	}
	return nil
}

type HandoverStore interface {
	ListNomadMigrationHandovers(context.Context, string, int) ([]string, error)
	GetNomadMigrationHandover(context.Context, string) (*Handover, error)
	CommitNomadSandboxMigrationHandover(context.Context, procdapi.RuntimeMigrationRequest, procdapi.RuntimeMigrationResponse) error
}

type Procd interface {
	MigrateRuntime(context.Context, string, procdapi.RuntimeMigrationRequest, string) (*procdapi.RuntimeMigrationResponse, error)
	ProbeCommandReady(context.Context, string, string) (*procdapi.CommandReadyProbeResult, error)
}

type HandoverTokens interface {
	GenerateMigrationToken(procdapi.RuntimeMigrationRequest) (string, error)
	GenerateToken(string, string, string) (string, error)
}

type ReadyNode interface {
	CommandReady(context.Context, protocol.NodeChannelTarget, protocol.CommandReadyControlRequest) (protocol.NodeControlResponse, error)
}

// NewHandover resumes procd delivery and then the existing authenticated probe
// and node readiness protocol. Generation publication and adoption remain the
// existing database/node transactions; a probe alone never changes routing.
func NewHandover(store HandoverStore, procd Procd, tokens HandoverTokens, node ReadyNode) (*Coordinator, error) {
	if store == nil || procd == nil || tokens == nil || node == nil {
		return nil, errors.New("migration handover authorities are required")
	}
	return &Coordinator{list: store.ListNomadMigrationHandovers, step: func(ctx context.Context, id string) (bool, error) {
		h, err := store.GetNomadMigrationHandover(ctx, id)
		if err != nil || h == nil {
			return false, err
		}
		if err := h.Validate(); err != nil {
			return false, err
		}
		if h.Command.Assignment.OperationID != id {
			return false, errors.New("handover changed operation")
		}
		address, err := protocol.NomadProcdAddress(h.Restored.Request.Stage.ExpectedPolicyToken.SourceIP)
		if err != nil {
			return false, err
		}
		if h.Receipt == nil {
			token, err := tokens.GenerateMigrationToken(h.Command)
			if err != nil {
				return false, err
			}
			if token == "" {
				return false, errors.New("empty migration handover token")
			}
			receipt, err := procd.MigrateRuntime(ctx, address, h.Command, token)
			if err != nil {
				return false, err
			}
			if receipt == nil || receipt.ValidateFor(h.Command) != nil {
				return false, errors.New("invalid procd handover acknowledgement")
			}
			err = store.CommitNomadSandboxMigrationHandover(ctx, h.Command, *receipt)
			return err == nil, err
		}
		assignment := h.Command.Assignment.Target
		token, err := tokens.GenerateToken(assignment.TeamID, "", assignment.SandboxID)
		if err != nil {
			return false, err
		}
		if token == "" {
			return false, errors.New("empty migration readiness token")
		}
		probe, err := procd.ProbeCommandReady(ctx, address, token)
		if err != nil {
			return false, err
		}
		if probe == nil || probe.Status != "ready" || probe.InstanceID != h.Command.InstanceID {
			return false, errors.New("migration readiness changed procd instance")
		}
		image, identity := h.Restored.Request.Image, h.Restored.Request.Stage.Identity
		request := protocol.CommandReadyControlRequest{Proof: protocol.CommandReadyProof{
			Version: protocol.CommandReadyProofVersion, SlotID: image.Target.SlotID,
			OperationID: id, ClaimID: image.Resources.ClaimID, LaunchAttempt: identity.LaunchAttempt,
			RunscContainerID: protocol.NomadRunscContainerID(image.Target.SlotID),
			ProcdInstanceID:  probe.InstanceID, ProcdAddress: address, RequestMethod: "PUT",
			RequestPath: protocol.ProcdCommandReadyProbePath, ResponseStatus: 200, ResponseBodyDigest: probe.ResponseBodyDigest,
		}}
		if err := request.Proof.Validate(); err != nil {
			return false, err
		}
		response, err := node.CommandReady(ctx, image.Target, request)
		if err != nil {
			return false, err
		}
		if response.ValidateCommandReadyResult(request) != nil || response.MigrationAdoption == nil || response.MigrationAdoption.Request.ValidateFor(h.Restored) != nil {
			return false, errors.New("migration readiness lacks exact adoption receipt")
		}
		return true, nil
	}}, nil
}
