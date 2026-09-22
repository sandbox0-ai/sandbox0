package runtimeslot

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
)

// MigrationAdoptionRequest is issued only after the region atomically publishes
// the restored generation. It releases destination image custody, not the live
// writer or resource lease. RestoreDigest binds every prior physical fence.
type MigrationAdoptionRequest struct {
	Target             NodeChannelTarget `json:"target"`
	OperationID        string            `json:"operation_id"`
	ClaimID            string            `json:"claim_id"`
	SandboxID          string            `json:"sandbox_id"`
	RuntimeGeneration  int64             `json:"runtime_generation"`
	ProcdInstanceID    string            `json:"procd_instance_id"`
	RestoreDigest      string            `json:"restore_digest"`
	CommandReadyDigest string            `json:"command_ready_digest"`
}

func (r MigrationAdoptionRequest) Validate() error {
	if err := r.Target.validate(true); err != nil {
		return err
	}
	for name, value := range map[string]string{"operation_id": r.OperationID, "claim_id": r.ClaimID, "sandbox_id": r.SandboxID, "procd_instance_id": r.ProcdInstanceID} {
		if err := validateRequiredID(name, value); err != nil {
			return err
		}
	}
	if r.RuntimeGeneration <= 1 {
		return fmt.Errorf("adoption requires a restored generation")
	}
	for name, value := range map[string]string{"restore_digest": r.RestoreDigest, "command_ready_digest": r.CommandReadyDigest} {
		if _, err := DecodeProof(name, value); err != nil {
			return err
		}
	}
	return nil
}

func (r MigrationAdoptionRequest) ValidateFor(restored MigrationRestoreObservation) error {
	if err := r.Validate(); err != nil {
		return err
	}
	if err := restored.Validate(); err != nil {
		return err
	}
	image := restored.Request.Image
	if restored.State != MigrationRestoreComplete || r.RestoreDigest != restored.RequestDigest ||
		r.Target != image.Target || r.OperationID != image.Publication.Assignment.OperationID || r.ClaimID != image.Resources.ClaimID ||
		r.SandboxID != image.Publication.Assignment.Target.SandboxID || r.RuntimeGeneration != image.Publication.Assignment.Target.RuntimeGeneration ||
		r.ProcdInstanceID != image.Publication.Capture.Request.ProcdInstanceID {
		return fmt.Errorf("adoption changed restored execution authority")
	}
	return nil
}

func (r MigrationAdoptionRequest) Digest() (string, error) {
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

// MigrationAdoptionProof acknowledges durable removal of the target's private
// checkpoint image. It is not a proof of source cleanup or resource release.
type MigrationAdoptionProof struct {
	RequestDigest string `json:"request_digest"`
	ImageAbsent   bool   `json:"image_absent"`
}

func (p MigrationAdoptionProof) ValidateFor(request MigrationAdoptionRequest) error {
	want, err := request.Digest()
	if err != nil {
		return err
	}
	if p.RequestDigest != want || !p.ImageAbsent {
		return fmt.Errorf("adoption lacks exact image cleanup proof")
	}
	return nil
}

// MigrationAdoptionReceipt is the exact regional command and node completion
// returned by the existing command-ready channel.
type MigrationAdoptionReceipt struct {
	Request MigrationAdoptionRequest `json:"request"`
	Proof   MigrationAdoptionProof   `json:"proof"`
}

func (r MigrationAdoptionReceipt) Validate() error { return r.Proof.ValidateFor(r.Request) }

// MigrationAdoptionAcknowledgement confirms regional receipt custody only.
// It neither grants execution nor attests source physical cleanup.
type MigrationAdoptionAcknowledgement struct {
	RequestDigest string `json:"request_digest"`
}

func (a MigrationAdoptionAcknowledgement) ValidateFor(receipt MigrationAdoptionReceipt) error {
	if err := receipt.Validate(); err != nil {
		return err
	}
	if a.RequestDigest != receipt.Proof.RequestDigest {
		return fmt.Errorf("migration adoption acknowledgement changed receipt")
	}
	return nil
}

func MigrationAdoptionReceiptPath(slot string) string {
	return SlotPath(slot) + "/migration-adoption-receipt"
}

// MigrationAdoptionCommandResponse contains only a previously committed
// regional command. A nil command is pending, never implicit authorization.
type MigrationAdoptionCommandResponse struct {
	SlotID  string                    `json:"slot_id"`
	Command *MigrationAdoptionRequest `json:"command"`
}

func (r MigrationAdoptionCommandResponse) ValidateFor(slot string) error {
	if ValidateSlotID(slot) != nil || r.SlotID != slot {
		return fmt.Errorf("migration adoption command changed slot")
	}
	if r.Command != nil && (r.Command.Validate() != nil || r.Command.Target.SlotID != slot) {
		return fmt.Errorf("invalid migration adoption command")
	}
	return nil
}

func MigrationAdoptionCommandPath(slot string) string {
	return SlotPath(slot) + "/migration-adoption-command"
}

func (r NodeControlResponse) ValidateCommandReadyResult(request CommandReadyControlRequest) error {
	if err := r.Validate(); err != nil {
		return err
	}
	if r.MigrationRestore != nil {
		return fmt.Errorf("command readiness cannot return a restore receipt")
	}
	if a := r.MigrationAdoption; a != nil {
		digest, err := request.Proof.Digest()
		if err != nil {
			return err
		}
		if a.Request.CommandReadyDigest != digest || a.Request.Target.SlotID != request.Proof.SlotID ||
			a.Request.OperationID != request.Proof.OperationID || a.Request.ClaimID != request.Proof.ClaimID || a.Request.ProcdInstanceID != request.Proof.ProcdInstanceID {
			return fmt.Errorf("adoption receipt changed command readiness")
		}
	}
	return nil
}
