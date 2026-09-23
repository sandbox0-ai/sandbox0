package runtimeslot

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"strconv"

	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
)

// MigrationRestoreRequest joins immutable execution state, source physical
// fencing and the new destination writer. The region persists this tokenless
// command before allowing the exact target launch. It is not a public API.
type MigrationRestoreRequest struct {
	Image    MigrationImagePrepareRequest `json:"image"`
	Prepared MigrationImagePrepared       `json:"prepared"`
	Fence    MigrationSourceFenceRequest  `json:"fence"`
	Proof    MigrationSourceFenceProof    `json:"proof"`
	Stage    rootfshandoff.StageRequest   `json:"stage"`
}

type MigrationRestoreState string

const (
	MigrationRestoreIntent    MigrationRestoreState = "intent"
	MigrationRestoreExecuting MigrationRestoreState = "restoring"
	MigrationRestoreComplete  MigrationRestoreState = "restored"
	MigrationRestoreUncertain MigrationRestoreState = "uncertain"
)

// MigrationRestoreObservation journals the one-shot execution boundary.
// An interrupted restoring state can never be retried by replaying the image.
type MigrationRestoreObservation struct {
	Request       MigrationRestoreRequest `json:"request"`
	RequestDigest string                  `json:"request_digest"`
	State         MigrationRestoreState   `json:"state"`
}

func (o MigrationRestoreObservation) Validate() error {
	want, err := o.Request.Digest()
	if err != nil {
		return err
	}
	if want != o.RequestDigest {
		return fmt.Errorf("restore observation changed its request")
	}
	switch o.State {
	case MigrationRestoreIntent, MigrationRestoreExecuting, MigrationRestoreComplete, MigrationRestoreUncertain:
		return nil
	default:
		return fmt.Errorf("unsupported migration restore state")
	}
}

func (r MigrationRestoreRequest) Validate() error {
	if err := r.Prepared.ValidateFor(r.Image); err != nil {
		return err
	}
	if err := r.Proof.ValidateFor(r.Fence); err != nil {
		return err
	}
	image, err := r.Image.Publication.Digest()
	if err != nil {
		return err
	}
	fenced, err := r.Fence.PublicationRequest.Digest()
	if err != nil || image != fenced || r.Fence.Publication != r.Image.Receipt {
		return fmt.Errorf("restore image is not the physically fenced source")
	}
	if err := r.Stage.ValidateDurableBinding(); err != nil {
		return err
	}
	if r.Stage.Identity.WriterGrantToken != "" || r.Stage.Generation == nil {
		return fmt.Errorf("restore authority requires a tokenless destination descriptor")
	}
	target, lease, identity := r.Image.Target, r.Image.Resources, r.Stage.Identity
	cut := r.Image.Publication.Capture.RootFS.Generation
	// Paused forks share the exact immutable generation while attaching it
	// through the child's independently fenced filesystem and writer.
	if r.Image.Checkpoint != nil && r.Image.Checkpoint.Assignment.Kind == runtimecontrol.CheckpointFork {
		cut.FilesystemID = r.Image.RuntimeAssignment().SandboxID
	}
	actual, err := json.Marshal(r.Stage.Generation)
	if err != nil {
		return err
	}
	expected, err := json.Marshal(cut)
	if err != nil {
		return err
	}
	validWriterEpoch := cut.WriterEpoch != math.MaxInt64 && identity.WriterEpoch == cut.WriterEpoch+1
	if r.Image.Checkpoint != nil {
		// Failed restore attempts can consume and fence writer epochs without
		// changing the retained image. The region binds the exact current grant;
		// the portable image contract only requires advancement beyond its cut.
		validWriterEpoch = identity.WriterEpoch > cut.WriterEpoch
	}
	if !bytes.Equal(actual, expected) || r.Stage.InitialGeneration != cut.GenerationID ||
		identity.RootFSID != cut.FilesystemID || identity.SourceOCIDigest != cut.SourceOCIDigest ||
		!validWriterEpoch ||
		identity.NodeUID != target.NodeUID || identity.BootID != target.NodeBootID || identity.AllocationID != target.AllocationID ||
		identity.SlotNonce != target.SlotID || identity.ClaimID != lease.ClaimID || identity.TaskName != NomadTaskName ||
		identity.RuntimeClass != "sandbox0-gvisor" || identity.RootFSDriver != "nomad-driver" ||
		identity.RuntimeGeneration != strconv.FormatInt(r.Image.RuntimeAssignment().RuntimeGeneration, 10) {
		return fmt.Errorf("restore changed destination or the captured filesystem cut")
	}
	revision, _ := r.Image.RuntimeAssignment().Revision()
	resourceDigest, _ := lease.Digest()
	if r.Stage.Labels[RuntimeAssignmentRevisionLabel] != revision || r.Stage.Labels[RuntimeResourceLeaseDigestLabel] != resourceDigest {
		return fmt.Errorf("restore changed assignment or resource lease")
	}
	return nil
}

func (r MigrationRestoreRequest) Digest() (string, error) {
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

// ValidateClaim is called after the ordinary claim has checked its bearer
// token, network policy and resource contract. Only launch behavior changes.
func (r MigrationRestoreRequest) ValidateClaim(claim NodeClaimControlRequest) error {
	if err := r.Validate(); err != nil {
		return err
	}
	if claim.Stage == nil || claim.Runtime == nil || claim.OperationID != r.Image.OperationID() ||
		claim.Resources != r.Image.Resources {
		return fmt.Errorf("restore claim changed its migration authority")
	}
	actual, err := claim.Stage.BindingDigest()
	if err != nil {
		return err
	}
	expected, err := r.Stage.BindingDigest()
	if err != nil || actual != expected {
		return fmt.Errorf("restore claim changed its writer binding")
	}
	revision, err := claim.Runtime.Revision()
	if err != nil {
		return err
	}
	want, _ := r.Image.RuntimeAssignment().Revision()
	if revision != want {
		return fmt.Errorf("restore claim changed its target assignment")
	}
	return nil
}

// ValidateClaimResult prevents an ordinary active response from being treated
// as restoration evidence, or an image from another operation being accepted.
func (r NodeControlResponse) ValidateClaimResult(request NodeClaimControlRequest) error {
	if r.MigrationAdoption != nil {
		return fmt.Errorf("claim cannot return adoption authority")
	}
	if err := r.Validate(); err != nil {
		return err
	}
	if request.MigrationRestore == nil {
		if r.MigrationRestore != nil {
			return fmt.Errorf("ordinary claim returned migration execution evidence")
		}
		return nil
	}
	want, err := request.MigrationRestore.Digest()
	if err != nil {
		return err
	}
	if r.MigrationRestore == nil || r.MigrationRestore.RequestDigest != want {
		return fmt.Errorf("restore result changed its execution authority")
	}
	return nil
}
