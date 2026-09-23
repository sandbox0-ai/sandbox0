package runtimeslot

import (
	"fmt"

	"github.com/sandbox0-ai/sandbox0/pkg/runtimecheckpoint"
)

// CheckpointRetained is issued by the authenticated regional authority only
// after committing a live reference to both the memory image and its matching
// RootFS cut. Object-store publication alone cannot authorize source cleanup.
// This receipt grants no destination lease or permission to restore.
type CheckpointRetained struct {
	CheckpointID             string                      `json:"checkpoint_id"`
	PublicationRequestDigest string                      `json:"publication_request_digest"`
	Reference                runtimecheckpoint.Reference `json:"reference"`
}

func (r CheckpointRetained) ValidateFor(request MigrationPublicationRequest, publication MigrationPublication) error {
	if err := validateRequiredID("checkpoint_id", r.CheckpointID); err != nil {
		return err
	}
	if request.CheckpointSource == nil {
		return fmt.Errorf("checkpoint retention cannot replace a migration destination outcome")
	}
	if err := publication.ValidateFor(request); err != nil {
		return err
	}
	if r.PublicationRequestDigest != publication.RequestDigest || r.Reference != publication.Reference {
		return fmt.Errorf("checkpoint retention changed the exact published memory and RootFS cut")
	}
	return nil
}
