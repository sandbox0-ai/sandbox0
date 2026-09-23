package runtimeslot

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecheckpoint"
)

// MigrationStagingRequest reserves exclusive use of one node's migration
// image pool before source preparation. Bytes and Inodes are admission floors,
// not a prediction of checkpoint size or a per-image kernel quota. The node's
// configured XFS project quota remains the hard limit. No host path is supplied.
// Source identity here does not authorize capture or destination execution.
type MigrationStagingRequest struct {
	// CaptureOnly retains the same bounded source pool and custody journal
	// while allowing memory pause to defer destination placement until resume.
	CaptureOnly                    bool                    `json:"capture_only,omitempty"`
	Target                         NodeChannelTarget       `json:"target"`
	Source                         MigrationCaptureRequest `json:"source"`
	Destination                    NodeChannelTarget       `json:"destination"`
	DestinationResourceLeaseDigest string                  `json:"destination_resource_lease_digest"`
	Bytes                          int64                   `json:"bytes"`
	Inodes                         uint64                  `json:"inodes"`
	CaptureUpload                  MigrationCaptureUpload  `json:"capture_upload,omitzero"`
}

func (r MigrationStagingRequest) IsSource() bool { return r.Target == r.Source.Target }

func (r MigrationStagingRequest) Validate() error {
	if err := r.Source.Validate(); err != nil {
		return err
	}
	if r.CaptureOnly {
		if !r.IsSource() || r.Destination != (NodeChannelTarget{}) || r.DestinationResourceLeaseDigest != "" {
			return fmt.Errorf("capture-only staging requires only its exact source")
		}
	} else {
		if err := r.Destination.validate(true); err != nil {
			return err
		}
		if r.Target != r.Source.Target && r.Target != r.Destination {
			return fmt.Errorf("staging reservation must target its exact source or destination")
		}
		s, d := r.Source.Target, r.Destination
		if s.ClusterID != d.ClusterID || s.NodeID == d.NodeID || s.NodeUID == d.NodeUID || s.SlotID == d.SlotID || s.AllocationID == d.AllocationID {
			return fmt.Errorf("staging reservation requires distinct nodes in one cluster")
		}
		if _, err := DecodeProof("destination_resource_lease_digest", r.DestinationResourceLeaseDigest); err != nil {
			return err
		}
	}
	if r.Bytes < 1<<20 || r.Bytes > 1<<50 || r.Bytes%4096 != 0 || r.Inodes < 2 || r.Inodes > 16384 {
		return fmt.Errorf("staging reservation requires aligned 1 MiB–1 PiB capacity and 2–16384 inodes")
	}
	if r.CaptureUpload != (MigrationCaptureUpload{}) {
		if err := r.CaptureUpload.ValidateFor(r.Source, r.Bytes); err != nil {
			return err
		}
	}
	return nil
}

func (r MigrationStagingRequest) Digest() (string, error) {
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

// MigrationStagingReserved acknowledges durable exclusive admission. It is not
// a physical block allocation or an authorization to execute either placement.
type MigrationStagingReserved struct {
	RequestDigest         string                         `json:"request_digest"`
	PeerCertificateSHA256 string                         `json:"peer_certificate_sha256,omitempty"`
	Peer                  runtimecheckpoint.PeerEndpoint `json:"peer,omitzero"`
}

type MigrationStagingReleased struct {
	RequestDigest string `json:"request_digest"`
}

func (r MigrationStagingReleased) ValidateFor(request MigrationStagingRequest) error {
	return (MigrationStagingReserved{RequestDigest: r.RequestDigest}).ValidateFor(request)
}

func NewNodeChannelMigrationStagingReserveCommand(request MigrationStagingRequest) (NodeChannelCommand, error) {
	return sealNodeChannelCommand(NodeChannelCommand{Version: NodeChannelVersion, Kind: NodeChannelCommandMigrationStagingReserve,
		Target: request.Target, MigrationStaging: &request})
}

func NewNodeChannelMigrationStagingReleaseCommand(request MigrationStagingRequest) (NodeChannelCommand, error) {
	return sealNodeChannelCommand(NodeChannelCommand{Version: NodeChannelVersion, Kind: NodeChannelCommandMigrationStagingRelease,
		Target: request.Target, MigrationStaging: &request})
}

func (r MigrationStagingReserved) ValidateFor(request MigrationStagingRequest) error {
	want, err := request.Digest()
	if err != nil {
		return err
	}
	if r.RequestDigest != want {
		return fmt.Errorf("staging receipt changed reservation")
	}
	if r.PeerCertificateSHA256 != "" {
		if err := runtimecheckpoint.ValidatePeerCertificateDigest(r.PeerCertificateSHA256); err != nil {
			return err
		}
	}
	if r.Peer != (runtimecheckpoint.PeerEndpoint{}) {
		pin, err := r.Peer.CertificateDigest()
		if err != nil || pin != r.PeerCertificateSHA256 {
			return fmt.Errorf("staging endpoint changed reserved certificate")
		}
	}
	return nil
}
