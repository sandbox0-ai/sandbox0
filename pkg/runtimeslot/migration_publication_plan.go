package runtimeslot

import (
	"fmt"

	"github.com/sandbox0-ai/sandbox0/pkg/runtimecheckpoint"
)

// MigrationPublicationPlan identifies a completed source image before regional
// upload finishes. This separate response type is never a publication receipt,
// source-fence proof or destination execution grant. Callers must independently
// obtain and commit the matching MigrationPublication before normal preparation.
type MigrationPublicationPlan struct {
	RequestDigest string                         `json:"request_digest"`
	Binding       runtimecheckpoint.Binding      `json:"binding"`
	Reference     runtimecheckpoint.Reference    `json:"reference"`
	Peer          runtimecheckpoint.PeerEndpoint `json:"peer"`
}

func (p MigrationPublicationPlan) ValidateFor(request MigrationPublicationRequest) error {
	want, err := request.Digest()
	if err != nil {
		return err
	}
	binding, err := request.Binding()
	if err != nil {
		return err
	}
	if p.RequestDigest != want || p.Binding != binding || request.DestinationPeerCertificateSHA256 == "" {
		return fmt.Errorf("planned publication changed its source or reserved destination")
	}
	if err := p.Reference.ValidateFor(binding); err != nil {
		return err
	}
	return p.Peer.Validate()
}

func NewNodeChannelMigrationPublicationPlanCommand(request MigrationPublicationRequest) (NodeChannelCommand, error) {
	return sealNodeChannelCommand(NodeChannelCommand{Version: NodeChannelVersion, Kind: NodeChannelCommandMigrationPublicationPlan,
		Target: request.Capture.Request.Target, MigrationPublicationPlan: &request})
}
