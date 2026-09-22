package runtimeslot

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/sandbox0-ai/sandbox0/pkg/runtimecheckpoint"
)

// MigrationCapturePeerRequest is regional authorization to exchange tentative
// execution-image bytes between two already reserved placements. The source
// additionally needs its durable capture intent; the destination still needs
// final regional publication and restore authority before it may execute.
// Both receipts are immutable evidence obtained through the node channel, not
// metadata supplied by a peer connection. Staging.Target selects the recipient.
type MigrationCapturePeerRequest struct {
	Staging     MigrationStagingRequest  `json:"staging"`
	Source      MigrationStagingReserved `json:"source"`
	Destination MigrationStagingReserved `json:"destination"`
}

func (r MigrationCapturePeerRequest) Validate() error {
	if err := r.Staging.Validate(); err != nil {
		return err
	}
	if r.Staging.CaptureUpload.Version == 0 {
		return fmt.Errorf("early peer requires a reserved capture scope")
	}
	source, destination := r.Staging, r.Staging
	source.Target, destination.Target = source.Source.Target, destination.Destination
	if err := r.Source.ValidateFor(source); err != nil {
		return err
	}
	if err := r.Destination.ValidateFor(destination); err != nil {
		return err
	}
	if r.Source.Peer == (runtimecheckpoint.PeerEndpoint{}) || r.Destination.Peer == (runtimecheckpoint.PeerEndpoint{}) ||
		r.Source.Peer.Address == r.Destination.Peer.Address || r.Source.PeerCertificateSHA256 == r.Destination.PeerCertificateSHA256 {
		return fmt.Errorf("early peer requires distinct pinned node endpoints")
	}
	return nil
}

func (r MigrationCapturePeerRequest) Digest() (string, error) {
	if err := r.Validate(); err != nil {
		return "", err
	}
	payload, err := json.Marshal(r)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:]), nil
}

// LocalReceipt is compared with the exact journaled reservation before any
// cache directory or source stream is acquired. A rotated key cannot adopt an
// earlier primary's grant; the ordinary regional download remains available.
func (r MigrationCapturePeerRequest) LocalReceipt() MigrationStagingReserved {
	if r.Staging.IsSource() {
		return r.Source
	}
	return r.Destination
}

type MigrationCapturePeerPrepared struct {
	RequestDigest string `json:"request_digest"`
}

func (r MigrationCapturePeerPrepared) ValidateFor(request MigrationCapturePeerRequest) error {
	want, err := request.Digest()
	if err != nil {
		return err
	}
	if r.RequestDigest != want {
		return fmt.Errorf("early peer receipt changed its regional grant")
	}
	return nil
}

func NewNodeChannelMigrationCapturePeerCommand(request MigrationCapturePeerRequest) (NodeChannelCommand, error) {
	return sealNodeChannelCommand(NodeChannelCommand{Version: NodeChannelVersion, Kind: NodeChannelCommandMigrationCapturePeer,
		Target: request.Staging.Target, MigrationCapturePeer: &request})
}
