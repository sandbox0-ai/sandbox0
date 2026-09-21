package nomadmigration

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"

	"github.com/sandbox0-ai/sandbox0/pkg/procdapi"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
)

// PreparationCancellation is a durable command for the original source procd.
// It grants neither checkpoint nor execution, and cannot cancel captured work.
type PreparationCancellation struct {
	Address string                           `json:"address"`
	Request procdapi.RuntimeMigrationRequest `json:"request"`
}

func (c PreparationCancellation) Digest() (string, error) {
	if c.Request.Action != procdapi.MigrationCancel {
		return "", errors.New("invalid preparation cancellation action")
	}
	if _, err := c.Request.Digest(); err != nil {
		return "", err
	}
	if protocol.ValidateNomadProcdAddress(c.Address) != nil {
		return "", errors.New("noncanonical source procd address")
	}
	payload, err := json.Marshal(c)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(payload)
	return hex.EncodeToString(digest[:]), nil
}

type PreparationCancellationStore interface {
	ListNomadMigrationPreparationCancellations(context.Context, string, int) ([]string, error)
	AuthorizeNomadSandboxMigrationPreparationCancellation(context.Context, string) (*PreparationCancellation, error)
	CommitNomadSandboxMigrationPreparationCancellation(context.Context, PreparationCancellation, procdapi.RuntimeMigrationResponse) error
}

func NewPreparationCancellation(store PreparationCancellationStore, procd Procd, tokens HandoverTokens) (*Coordinator, error) {
	if store == nil || procd == nil || tokens == nil {
		return nil, errors.New("migration cancellation authorities are required")
	}
	return &Coordinator{list: store.ListNomadMigrationPreparationCancellations, step: func(ctx context.Context, id string) (bool, error) {
		c, err := store.AuthorizeNomadSandboxMigrationPreparationCancellation(ctx, id)
		if err != nil || c == nil {
			return false, err
		}
		if _, err := c.Digest(); err != nil || c.Request.Assignment.OperationID != id {
			return false, errors.New("cancellation changed operation authority")
		}
		token, err := tokens.GenerateMigrationToken(c.Request)
		if err != nil {
			return false, err
		}
		if token == "" {
			return false, errors.New("empty migration cancellation token")
		}
		receipt, err := procd.MigrateRuntime(ctx, c.Address, c.Request, token)
		if err != nil {
			return false, err
		}
		if receipt == nil || receipt.ValidateFor(c.Request) != nil {
			return false, errors.New("invalid source cancellation acknowledgement")
		}
		err = store.CommitNomadSandboxMigrationPreparationCancellation(ctx, *c, *receipt)
		return err == nil, err
	}}, nil
}
