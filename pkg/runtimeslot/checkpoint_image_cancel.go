package runtimeslot

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
)

// CheckpointImageCancelRequest closes one independent image preparation before
// restore execution is authorized. It releases only the destination's cache;
// the retained regional image, writer and resource lease have separate custody.
type CheckpointImageCancelRequest struct {
	Image MigrationImagePrepareRequest `json:"image"`
}

func (r CheckpointImageCancelRequest) Digest() (string, error) {
	if r.Image.Checkpoint == nil {
		return "", fmt.Errorf("image cancellation requires independent checkpoint authority")
	}
	if err := r.Image.Validate(); err != nil {
		return "", err
	}
	payload, err := json.Marshal(r)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:]), nil
}

type CheckpointImageCancelProof struct {
	RequestDigest string `json:"request_digest"`
	ImageAbsent   bool   `json:"image_absent"`
}

func (p CheckpointImageCancelProof) ValidateFor(r CheckpointImageCancelRequest) error {
	want, err := r.Digest()
	if err != nil {
		return err
	}
	if p.RequestDigest != want || !p.ImageAbsent {
		return fmt.Errorf("checkpoint image cancellation requires exact absence evidence")
	}
	return nil
}

func NewNodeChannelCheckpointImageCancelCommand(request CheckpointImageCancelRequest) (NodeChannelCommand, error) {
	return sealNodeChannelCommand(NodeChannelCommand{Version: NodeChannelVersion, Kind: NodeChannelCommandCheckpointImageCancel,
		Target: request.Image.Target, CheckpointImageCancel: &request})
}
