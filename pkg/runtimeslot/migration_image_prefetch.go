package runtimeslot

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/sandbox0-ai/sandbox0/pkg/runtimecheckpoint"
)

// MigrationImagePrefetchRequest authorizes only a bounded cache in the exact
// destination staging reservation. It cannot substitute for ImagePrepare or
// grant execution, writer ownership, fencing or resource release.
type MigrationImagePrefetchRequest struct {
	Staging     MigrationStagingRequest     `json:"staging"`
	Publication MigrationPublicationRequest `json:"publication"`
	Plan        MigrationPublicationPlan    `json:"plan"`
}

// MigrationImagePrefetched is a historical cache-fill acknowledgement, not
// durable regional publication or target execution authority. Normal image
// preparation must independently verify the files against the published image.
type MigrationImagePrefetched struct {
	RequestDigest  string `json:"request_digest"`
	ManifestDigest string `json:"manifest_digest"`
	TotalBytes     int64  `json:"total_bytes"`
}

func (p MigrationImagePrefetched) ValidateFor(request MigrationImagePrefetchRequest) error {
	want, err := request.Digest()
	if err != nil {
		return err
	}
	if p.RequestDigest != want || p.ManifestDigest != request.Plan.Reference.ManifestDigest || p.TotalBytes <= 0 ||
		p.TotalBytes > request.Staging.Bytes || p.TotalBytes > runtimecheckpoint.MaxImageBytes {
		return fmt.Errorf("prefetched migration image changed its identity or size")
	}
	return nil
}

func (r MigrationImagePrefetchRequest) Validate() error {
	if err := r.Staging.Validate(); err != nil {
		return err
	}
	if err := r.Plan.ValidateFor(r.Publication); err != nil {
		return err
	}
	if r.Staging.IsSource() || r.Staging.Target != r.Staging.Destination || r.Staging.Source != r.Publication.Capture.Request {
		return fmt.Errorf("migration prefetch changed its source or reserved destination")
	}
	return nil
}

func (r MigrationImagePrefetchRequest) Digest() (string, error) {
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

func NewNodeChannelMigrationImagePrefetchCommand(request MigrationImagePrefetchRequest) (NodeChannelCommand, error) {
	return sealNodeChannelCommand(NodeChannelCommand{Version: NodeChannelVersion, Kind: NodeChannelCommandMigrationImagePrefetch,
		Target: request.Staging.Target, MigrationImagePrefetch: &request})
}
