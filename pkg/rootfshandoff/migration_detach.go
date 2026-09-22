package rootfshandoff

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"
)

// MigrationRootFSDetachRequest is an explicit custody handoff, not an ordinary
// release. AuthorizationDigest names the regionally committed handoff command;
// its authority must be verified by ctld before entering the session manager.
type MigrationRootFSDetachRequest struct {
	OperationID         string `json:"operation_id"`
	CutDigest           string `json:"cut_digest"`
	AuthorizationDigest string `json:"authorization_digest"`
}

func (r MigrationRootFSDetachRequest) Validate() error {
	return (MigrationRootFSCutRequest{OperationID: r.OperationID, GenerationID: r.OperationID,
		CaptureRequestDigest: r.AuthorizationDigest, SourceBindingDigest: r.CutDigest}).Validate()
}

// MigrationRootFSDetachProof retains the already-published cut. Physical
// detachment must never replace it with a checkpoint made during unmount.
// Session uses the common physical absence observation, without the crash
// abandon proof's instruction to retain the old regional head.
type MigrationRootFSDetachProof struct {
	Request MigrationRootFSDetachRequest `json:"request"`
	Session CrashFenceSessionObservation `json:"session"`
	Digest  string                       `json:"digest"`
}

func (p MigrationRootFSDetachProof) digest() (string, error) {
	p.Digest = ""
	payload, err := json.Marshal(p)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:]), nil
}

func NewMigrationRootFSDetachProof(request MigrationRootFSDetachRequest, session CrashFenceSessionObservation) (MigrationRootFSDetachProof, error) {
	proof := MigrationRootFSDetachProof{Request: request, Session: session}
	var err error
	proof.Digest, err = proof.digest()
	if err == nil {
		err = proof.Validate()
	}
	return proof, err
}

func (p MigrationRootFSDetachProof) Validate() error {
	if err := p.Request.Validate(); err != nil {
		return err
	}
	if err := p.Session.Validate(); err != nil {
		return err
	}
	if p.Session.OperationID != p.Request.OperationID {
		return fmt.Errorf("migration detach operation changed")
	}
	if _, err := time.Parse(time.RFC3339Nano, p.Session.ObservedAt); err != nil {
		return err
	}
	want, err := p.digest()
	if err != nil || want != p.Digest {
		return fmt.Errorf("migration detach proof digest changed")
	}
	return nil
}

func (p MigrationRootFSDetachProof) ValidateFor(stage StageRequest, cut MigrationRootFSCut, request MigrationRootFSDetachRequest) error {
	if err := p.Validate(); err != nil {
		return err
	}
	if err := cut.ValidateFor(stage, cut.Request); err != nil {
		return err
	}
	if p.Request != request || request.CutDigest != cut.Digest || request.OperationID != cut.Request.OperationID ||
		p.Session.Parent != stage.Parent || p.Session.RootFSID != stage.Identity.RootFSID ||
		p.Session.WriterEpoch != stage.Identity.WriterEpoch || p.Session.BindingDigest != cut.Request.SourceBindingDigest {
		return fmt.Errorf("migration detach proof changed its retained source cut")
	}
	return nil
}
