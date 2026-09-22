package runtimeslot

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
)

// MigrationSourceFenceRequest consumes the exact regionally committed image.
// This is a physical source fence, not a target claim or final resource release.
type MigrationSourceFenceRequest struct {
	PublicationRequest MigrationPublicationRequest `json:"publication_request"`
	Publication        MigrationPublication        `json:"publication"`
}

func (r MigrationSourceFenceRequest) Digest() (string, error) {
	if err := r.Publication.ValidateFor(r.PublicationRequest); err != nil {
		return "", err
	}
	payload, err := json.Marshal(r)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:]), nil
}

func (r MigrationSourceFenceRequest) RootFSRequest() (rootfshandoff.MigrationRootFSDetachRequest, error) {
	digest, err := r.Digest()
	if err != nil {
		return rootfshandoff.MigrationRootFSDetachRequest{}, err
	}
	return rootfshandoff.MigrationRootFSDetachRequest{OperationID: r.PublicationRequest.Assignment.OperationID,
		CutDigest: r.PublicationRequest.Capture.RootFS.Digest, AuthorizationDigest: digest}, nil
}

type MigrationSourceFenceProof struct {
	RequestDigest     string                                   `json:"request_digest"`
	RootFS            rootfshandoff.MigrationRootFSDetachProof `json:"rootfs"`
	ContainerID       string                                   `json:"container_id"`
	MountNamespaceID  string                                   `json:"mount_namespace_id"`
	ContainerAbsent   bool                                     `json:"container_absent"`
	StableMountAbsent bool                                     `json:"stable_mount_absent"`
	Digest            string                                   `json:"digest"`
}

func (p MigrationSourceFenceProof) ProofDigest() (string, error) {
	p.Digest = ""
	payload, err := json.Marshal(p)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:]), nil
}

func (p MigrationSourceFenceProof) ValidateFor(request MigrationSourceFenceRequest) error {
	want, err := request.Digest()
	if err != nil {
		return err
	}
	detach, err := request.RootFSRequest()
	if err != nil {
		return err
	}
	if err := p.RootFS.Validate(); err != nil {
		return err
	}
	capture := request.PublicationRequest.Capture.Request
	if p.RequestDigest != want || p.RootFS.Request != detach || p.RootFS.Session.BindingDigest != capture.BindingDigest ||
		p.RootFS.Session.RootFSID != request.PublicationRequest.Capture.RootFS.Generation.FilesystemID ||
		p.RootFS.Session.WriterEpoch != request.PublicationRequest.Capture.RootFS.Generation.WriterEpoch ||
		p.ContainerID != NomadRunscContainerID(capture.Target.SlotID) || !p.ContainerAbsent || !p.StableMountAbsent ||
		validateRequiredID("mount_namespace_id", p.MountNamespaceID) != nil {
		return fmt.Errorf("migration source fence changed the exact captured source")
	}
	d, err := p.ProofDigest()
	if err != nil || d != p.Digest {
		return fmt.Errorf("migration source fence digest changed")
	}
	return nil
}

func NewNodeChannelMigrationFenceCommand(request MigrationSourceFenceRequest) (NodeChannelCommand, error) {
	return sealNodeChannelCommand(NodeChannelCommand{Version: NodeChannelVersion, Kind: NodeChannelCommandMigrationFence,
		Target: request.PublicationRequest.Capture.Request.Target, MigrationFence: &request})
}
