package procdapi

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/google/uuid"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
)

const (
	RuntimeMigrationPath = "/internal/v1/runtime/migration"
	MigrationPrepare     = "prepare"
	MigrationRestore     = "restore"
	MigrationCancel      = "cancel"
)

// RuntimeMigrationRequest is private manager-to-procd control, never a public
// sandbox action. Its signed permission binds the exact process, assignment,
// lifecycle epoch and action so a source prepare token cannot authorize restore.
type RuntimeMigrationRequest struct {
	Action         string                             `json:"action"`
	InstanceID     string                             `json:"instance_id"`
	LifecycleEpoch int64                              `json:"lifecycle_epoch"`
	Assignment     runtimecontrol.MigrationAssignment `json:"assignment"`
}

func (r RuntimeMigrationRequest) Digest() (string, error) {
	if r.Action != MigrationPrepare && r.Action != MigrationRestore && r.Action != MigrationCancel {
		return "", errors.New("invalid runtime migration action")
	}
	id, err := uuid.Parse(r.InstanceID)
	if err != nil || id == uuid.Nil || id.String() != r.InstanceID || r.LifecycleEpoch <= 0 || r.Assignment.Target.TeamID == "" {
		return "", errors.New("invalid runtime migration identity")
	}
	if err := r.Assignment.Validate(); err != nil {
		return "", err
	}
	payload, err := json.Marshal(r)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:]), nil
}

func (r RuntimeMigrationRequest) Permission() (string, error) {
	digest, err := r.Digest()
	if err != nil {
		return "", err
	}
	return "runtime:migration:" + digest, nil
}

type RuntimeMigrationResponse struct {
	InstanceID        string `json:"instance_id"`
	RequestDigest     string `json:"request_digest"`
	RuntimeGeneration int64  `json:"runtime_generation"`
	State             string `json:"state"`
}

func (r RuntimeMigrationResponse) ValidateFor(request RuntimeMigrationRequest) error {
	digest, err := request.Digest()
	if err != nil {
		return err
	}
	state, generation := "ready", request.Assignment.SourceGeneration
	if request.Action == MigrationPrepare {
		state = "prepared"
	}
	if request.Action == MigrationRestore {
		generation = request.Assignment.Target.RuntimeGeneration
	}
	if r.InstanceID != request.InstanceID || r.RequestDigest != digest || r.State != state || r.RuntimeGeneration != generation {
		return errors.New("runtime migration response does not match the exact request")
	}
	return nil
}

// MigrateRuntime sends one exact internal handover command. It does not retry
// or fall back to a fresh runtime; the durable regional transaction owns retry.
func (c *ProcdClient) MigrateRuntime(ctx context.Context, address string, request RuntimeMigrationRequest, token string) (*RuntimeMigrationResponse, error) {
	if _, err := request.Digest(); err != nil {
		return nil, err
	}
	response, err := doBoundedProcdRequest[RuntimeMigrationResponse](ctx, c.httpClient, http.MethodPut,
		address+RuntimeMigrationPath, token, "migrate runtime", request, 1<<20)
	if err != nil {
		return nil, err
	}
	if err := response.ValidateFor(request); err != nil {
		return nil, err
	}
	return response, nil
}
