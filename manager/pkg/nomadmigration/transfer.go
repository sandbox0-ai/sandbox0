// Package nomadmigration advances system-owned migration transactions using
// PostgreSQL commands and authenticated node channels. It has no public API.
package nomadmigration

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
)

const migrationBatchSize = 8

// Transfer is a read-only projection of one committed migration transaction.
// Absence of a receipt is pending work, never permission to skip a physical step.
type Transfer struct {
	Publication protocol.MigrationPublicationRequest
	Published   *protocol.MigrationPublication
	Image       *protocol.MigrationImagePrepareRequest
	Prepared    *protocol.MigrationImagePrepared
	Fence       *protocol.MigrationSourceFenceRequest
}

func (t Transfer) Validate() error {
	publicationDigest, err := t.Publication.Digest()
	if err != nil {
		return err
	}
	if t.Published != nil && t.Published.ValidateFor(t.Publication) != nil {
		return errors.New("invalid stored migration publication")
	}
	if t.Image != nil {
		digest, err := t.Image.Publication.Digest()
		if err != nil || digest != publicationDigest || t.Published == nil || t.Image.Receipt != *t.Published || t.Image.Validate() != nil {
			return errors.New("migration image changed publication")
		}
	}
	if t.Prepared != nil && (t.Image == nil || t.Prepared.ValidateFor(*t.Image) != nil) {
		return errors.New("invalid stored migration image receipt")
	}
	if t.Fence != nil {
		digest, err := t.Fence.PublicationRequest.Digest()
		if err != nil || digest != publicationDigest || t.Published == nil || t.Prepared == nil || t.Fence.Publication != *t.Published {
			return errors.New("migration fence lacks exact durable destination image")
		}
		if _, err := t.Fence.Digest(); err != nil {
			return err
		}
	}
	return nil
}

// Store reuses existing immutable commands and receipt commits. Reads never
// renew leases or create authority. Every new command remains a database CAS.
type Store interface {
	ListNomadMigrationTransfers(context.Context, string, int) ([]string, error)
	GetNomadMigrationTransfer(context.Context, string) (*Transfer, error)
	CommitNomadSandboxMigrationPublication(context.Context, protocol.MigrationPublicationRequest, protocol.MigrationPublication) error
	AuthorizeNomadSandboxMigrationImagePreparation(context.Context, runtimecontrol.MigrationAssignment) (*protocol.MigrationImagePrepareRequest, error)
	CommitNomadSandboxMigrationImagePreparation(context.Context, protocol.MigrationImagePrepareRequest, protocol.MigrationImagePrepared) error
	AuthorizeNomadSandboxMigrationSourceFence(context.Context, protocol.MigrationSourceFenceRequest) (*protocol.MigrationSourceFenceRequest, error)
	CommitNomadSandboxMigrationSourceFence(context.Context, protocol.MigrationSourceFenceRequest, protocol.MigrationSourceFenceProof) error
}

type Node interface {
	PublishMigration(context.Context, protocol.MigrationPublicationRequest) (*protocol.MigrationPublication, error)
	PrepareMigrationImage(context.Context, protocol.MigrationImagePrepareRequest) (*protocol.MigrationImagePrepared, error)
	FenceMigrationSource(context.Context, protocol.MigrationSourceFenceRequest) (*protocol.MigrationSourceFenceProof, error)
}

// Result counts transitions, not completed migrations or resource releases.
type Result struct{ Candidates, Advanced, Skipped, Failed int }
type Report struct {
	Result Result
	Error  error
}

// Coordinator runs one bounded step per candidate per pass. Constructors bind
// evacuation and execution phases to existing PostgreSQL authority. Each store
// transaction owns its admission and lifecycle decisions; the loop only schedules.
type Coordinator struct {
	mu    sync.Mutex
	store Store
	node  Node
	after string
	list  func(context.Context, string, int) ([]string, error)
	step  func(context.Context, string) (bool, error)
}

func New(store Store, node Node) (*Coordinator, error) {
	if store == nil || node == nil {
		return nil, errors.New("migration transfer authorities are required")
	}
	c := &Coordinator{store: store, node: node}
	c.list = store.ListNomadMigrationTransfers
	c.step = c.advance
	return c, nil
}

// RunOnce retains an exclusive cursor after a failed attempt but never skips
// an unvisited candidate when its pass is canceled. The cursor is scheduling
// state only; every physical action starts with fresh PostgreSQL evidence.
func (c *Coordinator) RunOnce(ctx context.Context) (Result, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var result Result
	ids, err := c.list(ctx, c.after, migrationBatchSize)
	if err != nil {
		return result, err
	}
	if len(ids) > migrationBatchSize {
		return result, errors.New("migration work scan exceeded limit")
	}
	result.Candidates = len(ids)
	var failures []error
	for _, id := range ids {
		if err := ctx.Err(); err != nil {
			return result, errors.Join(append(failures, err)...)
		}
		if id == "" || id <= c.after {
			return result, errors.Join(append(failures, errors.New("migration work cursor did not advance"))...)
		}
		step, cancel := context.WithTimeout(ctx, 2*time.Minute)
		advanced, err := c.step(step, id)
		cancel()
		c.after = id
		if err != nil {
			result.Failed++
			failures = append(failures, fmt.Errorf("advance migration %s: %w", id, err))
		} else if advanced {
			result.Advanced++
		} else {
			result.Skipped++
		}
	}
	if len(ids) < migrationBatchSize {
		c.after = ""
	}
	return result, errors.Join(failures...)
}

func (c *Coordinator) advance(ctx context.Context, id string) (bool, error) {
	t, err := c.store.GetNomadMigrationTransfer(ctx, id)
	if err != nil || t == nil {
		return false, err
	}
	if err := t.Validate(); err != nil {
		return false, err
	}
	if t.Publication.Assignment.OperationID != id {
		return false, errors.New("migration transfer changed operation")
	}
	if t.Published == nil {
		receipt, err := c.node.PublishMigration(ctx, t.Publication)
		if err != nil {
			return false, err
		}
		if receipt == nil || receipt.ValidateFor(t.Publication) != nil {
			return false, errors.New("source returned invalid publication")
		}
		err = c.store.CommitNomadSandboxMigrationPublication(ctx, t.Publication, *receipt)
		return err == nil, err
	}
	if t.Prepared == nil {
		command := t.Image
		if command == nil {
			command, err = c.store.AuthorizeNomadSandboxMigrationImagePreparation(ctx, t.Publication.Assignment)
			if err != nil {
				return false, err
			}
		}
		t.Image = command
		if command == nil || t.Validate() != nil {
			return false, errors.New("destination image command changed authority")
		}
		receipt, err := c.node.PrepareMigrationImage(ctx, *command)
		if err != nil {
			return false, err
		}
		if receipt == nil || receipt.ValidateFor(*command) != nil {
			return false, errors.New("destination returned invalid image receipt")
		}
		err = c.store.CommitNomadSandboxMigrationImagePreparation(ctx, *command, *receipt)
		return err == nil, err
	}
	command := t.Fence
	if command == nil {
		command, err = c.store.AuthorizeNomadSandboxMigrationSourceFence(ctx, protocol.MigrationSourceFenceRequest{PublicationRequest: t.Publication, Publication: *t.Published})
		if err != nil {
			return false, err
		}
	}
	t.Fence = command
	if command == nil || t.Validate() != nil {
		return false, errors.New("source fence command changed authority")
	}
	proof, err := c.node.FenceMigrationSource(ctx, *command)
	if err != nil {
		return false, err
	}
	if proof == nil || proof.ValidateFor(*command) != nil {
		return false, errors.New("source returned invalid physical fence")
	}
	err = c.store.CommitNomadSandboxMigrationSourceFence(ctx, *command, *proof)
	return err == nil, err
}

// Run is independent of terminal cleanup so a slow image transfer cannot hold
// the cleanup loop. Every manager replica can retry the same database commands.
func (c *Coordinator) Run(ctx context.Context, report func(Report)) error {
	for ctx.Err() == nil {
		pass, cancel := context.WithTimeout(ctx, 5*time.Minute)
		result, err := c.RunOnce(pass)
		cancel()
		if ctx.Err() != nil {
			break
		}
		if report != nil {
			report(Report{Result: result, Error: err})
		}
		timer := time.NewTimer(time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
		case <-timer.C:
		}
	}
	return ctx.Err()
}
