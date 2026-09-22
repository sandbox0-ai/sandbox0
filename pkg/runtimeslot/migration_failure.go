package runtimeslot

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
)

// MigrationFailureRequest permanently closes an uncommitted destination's
// execution path. It retains the original restore, including both incarnations
// and the source fence. This intent is not proof of physical cleanup and grants
// no authority to replay the image, publish target writes, or release capacity.
type MigrationFailureRequest struct {
	Restore MigrationRestoreRequest `json:"restore"`
	Reason  string                  `json:"reason"`
}

const (
	MigrationFailureTermination            = "termination"
	MigrationFailureDestinationUnavailable = "destination_unavailable"
)

func (r MigrationFailureRequest) Digest() (string, error) {
	if r.Reason != MigrationFailureTermination && r.Reason != MigrationFailureDestinationUnavailable {
		return "", fmt.Errorf("unsupported migration failure reason")
	}
	if err := r.Restore.Validate(); err != nil {
		return "", err
	}
	payload, err := json.Marshal(r)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(payload)
	return hex.EncodeToString(digest[:]), nil
}

// MigrationFailureStopProof proves only that the exact destination container
// was removed after its durable execution fence. RootFS, image, network,
// allocation and resource-cgroup cleanup remain separate obligations.
type MigrationFailureStopProof struct {
	RequestDigest   string `json:"request_digest"`
	ContainerID     string `json:"container_id"`
	ContainerAbsent bool   `json:"container_absent"`
}

// MigrationFailureStopReceipt retains the regional decision and exact node
// execution-stop evidence. It grants no destination storage cleanup authority.
type MigrationFailureStopReceipt struct {
	Request MigrationFailureRequest   `json:"request"`
	Proof   MigrationFailureStopProof `json:"proof"`
}

func (p MigrationFailureStopProof) ValidateFor(r MigrationFailureRequest) error {
	want, err := r.Digest()
	if err != nil {
		return err
	}
	if p.RequestDigest != want || p.ContainerID != NomadRunscContainerID(r.Restore.Image.Target.SlotID) || !p.ContainerAbsent {
		return fmt.Errorf("migration failure stop lacks exact destination absence")
	}
	return nil
}

func NewNodeChannelMigrationFailureStopCommand(request MigrationFailureRequest) (NodeChannelCommand, error) {
	return sealNodeChannelCommand(NodeChannelCommand{Version: NodeChannelVersion, Kind: NodeChannelCommandMigrationFailureStop,
		Target: request.Restore.Image.Target, MigrationFailureStop: &request})
}
