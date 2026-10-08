package sandboxstore

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/credentialbinding"
)

type claimReservationBatches struct {
	mu    sync.Mutex
	teams map[string][]*claimReservationCall
}

type claimReservationCall struct {
	ctx     context.Context
	request *ReserveSandboxClaimRequest
	done    chan claimReservationResult
}

type claimReservationResult struct {
	record *SandboxRecord
	err    error
}

// ReserveSandboxClaim groups a burst's fresh, credential-free reservations
// under one durable team-quota transaction. Retries and credential bindings use
// the existing individual path. No caller receives a record before commit.
func (s *PGSandboxStore) ReserveSandboxClaim(ctx context.Context, request *ReserveSandboxClaimRequest) (*SandboxRecord, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("sandbox store is not configured")
	}
	if request == nil {
		return nil, fmt.Errorf("sandbox claim reservation request is required")
	}
	_, operation, ttl, err := normalizeSandboxClaimInput(request.Record, request.OperationID, request.LeaseTTL)
	if err != nil {
		return nil, err
	}
	if request.ActiveSandboxLimit != nil && *request.ActiveSandboxLimit < 0 {
		return nil, fmt.Errorf("active sandbox limit must be non-negative")
	}
	// The request can outlive a canceled HTTP waiter. Own all mutable input
	// rather than reading caller-owned maps/pointers after returning.
	copyRequest, record := *request, *request.Record
	copyRequest.OperationID, copyRequest.LeaseTTL = operation, ttl
	config, err := json.Marshal(record.Config)
	if err != nil {
		return nil, err
	}
	var ownedConfig SandboxConfig
	if err := json.Unmarshal(config, &ownedConfig); err != nil {
		return nil, err
	}
	record.Config = ownedConfig
	record.TemplateSpec = *record.TemplateSpec.DeepCopy()
	copyRequest.Record = &record
	copyRequest.CredentialBindings = credentialbinding.CloneStore(request.CredentialBindings)
	if request.ActiveSandboxLimit != nil {
		limit := *request.ActiveSandboxLimit
		copyRequest.ActiveSandboxLimit = &limit
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	call := &claimReservationCall{ctx: ctx, request: &copyRequest, done: make(chan claimReservationResult, 1)}
	queue := &s.claimReservationBatches
	queue.mu.Lock()
	if queue.teams == nil {
		queue.teams = make(map[string][]*claimReservationCall)
	}
	team := record.TeamID
	_, running := queue.teams[team]
	queue.teams[team] = append(queue.teams[team], call)
	queue.mu.Unlock()
	if !running {
		go s.runClaimReservationBatches(team)
	}
	select {
	case result := <-call.done:
		return result.record, result.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (s *PGSandboxStore) runClaimReservationBatches(team string) {
	queue := &s.claimReservationBatches
	for {
		// A bounded collection window lets simultaneous callers share WAL
		// durability without retaining an idle worker or a database connection.
		time.Sleep(2 * time.Millisecond)
		queue.mu.Lock()
		pending := queue.teams[team]
		if len(pending) == 0 {
			delete(queue.teams, team)
			queue.mu.Unlock()
			return
		}
		count := min(16, len(pending))
		batch := append([]*claimReservationCall(nil), pending[:count]...)
		for i := range count {
			pending[i] = nil
		}
		queue.teams[team] = pending[count:]
		queue.mu.Unlock()
		active := batch[:0]
		for _, call := range batch {
			if err := call.ctx.Err(); err != nil {
				call.done <- claimReservationResult{err: err}
			} else {
				active = append(active, call)
			}
		}
		if len(active) == 0 {
			continue
		}
		results, err := s.reserveFreshSandboxClaimBatch(active)
		if err != nil {
			// A duplicate operation, retry, binding or canceled transaction must
			// not poison unrelated callers. Rollback precedes individual retries;
			// the durable retry path also covers an ambiguous commit response.
			for _, call := range active {
				record, err := s.reserveSandboxClaim(call.ctx, call.request)
				call.done <- claimReservationResult{record: record, err: err}
			}
			continue
		}
		for i, call := range active {
			call.done <- results[i]
		}
	}
}

func (s *PGSandboxStore) reserveFreshSandboxClaimBatch(calls []*claimReservationCall) ([]claimReservationResult, error) {
	if len(calls) < 2 {
		return nil, fmt.Errorf("individual reservation required")
	}
	ids, operations := make(map[string]bool), make(map[string]bool)
	sandboxIDs := make([]string, 0, len(calls))
	args := make([][]any, len(calls))
	limited := false
	deadline := 15 * time.Second
	team := calls[0].request.Record.TeamID
	for i, call := range calls {
		r := call.request
		if len(r.CredentialBindings) != 0 || ids[r.Record.ID] || operations[r.OperationID] ||
			r.Record.TeamID != team || len(team) > 512 || len(r.Record.ID) > 512 {
			return nil, fmt.Errorf("individual reservation required")
		}
		ids[r.Record.ID], operations[r.OperationID] = true, true
		sandboxIDs = append(sandboxIDs, r.Record.ID)
		var err error
		args[i], err = sandboxRecordInsertArgs(r.Record)
		if err != nil {
			return nil, err
		}
		_, operation, ttl, err := normalizeSandboxClaimInput(r.Record, r.OperationID, r.LeaseTTL)
		if err != nil {
			return nil, err
		}
		args[i] = append(args[i], operation, SandboxRuntimeClaimPhaseClaiming, ttl.Milliseconds(), credentialbinding.DigestStore(nil))
		deadline = min(deadline, ttl)
		limited = limited || r.ActiveSandboxLimit != nil
	}
	// One canceled waiter cannot abort the other callers' transaction. The
	// bounded transaction owns its inputs; canceled committed reservations are
	// still governed by the existing abandoned-claim lease and cleanup fence.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(calls[0].ctx), deadline)
	defer cancel()
	// Keep a live peer independent of canceled waiters, but release the lock
	// and connection promptly when the whole group has been abandoned.
	var cancellationMu sync.Mutex
	remaining := len(calls)
	for _, call := range calls {
		stop := context.AfterFunc(call.ctx, func() {
			cancellationMu.Lock()
			defer cancellationMu.Unlock()
			remaining--
			if remaining == 0 {
				cancel()
			}
		})
		defer stop()
	}
	release, err := s.claimAdmissionTurns.acquire(ctx, team)
	if err != nil {
		return nil, err
	}
	defer release()
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := lockActiveSandboxQuotaTeam(ctx, tx, team); err != nil {
		return nil, err
	}
	var existing bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM manager.sandboxes WHERE sandbox_id=ANY($1::text[]))`, sandboxIDs).Scan(&existing); err != nil {
		return nil, err
	}
	if existing {
		return nil, fmt.Errorf("individual retry required")
	}
	var current int64
	if limited {
		current, err = countActiveSandboxQuotaReservations(ctx, tx, team)
		if err != nil {
			return nil, err
		}
	}
	results := make([]claimReservationResult, len(calls))
	admitted := make([]int, 0, len(calls))
	statements := &pgx.Batch{}
	for i, call := range calls {
		r := call.request
		if err := call.ctx.Err(); err != nil {
			results[i].err = err
			continue
		}
		if r.ActiveSandboxLimit != nil && current >= *r.ActiveSandboxLimit {
			results[i].err = &ActiveSandboxQuotaExceededError{TeamID: team, Current: current, Limit: *r.ActiveSandboxLimit}
			continue
		}
		current++
		admitted = append(admitted, i)
		statements.Queue(sandboxClaimReservationInsertSQL(), args[i]...)
		// Preserve empty-binding replacement even if an orphan projection from
		// a previous authority once used this otherwise absent sandbox ID.
		statements.Queue(`DELETE FROM sandbox_egress_credential_bindings WHERE team_id=$1 AND sandbox_id=$2`, team, r.Record.ID)
	}
	if len(admitted) == 0 {
		return results, nil
	}
	responses := tx.SendBatch(ctx, statements)
	for _, i := range admitted {
		results[i].record, err = scanSandboxRecord(responses.QueryRow())
		if err == nil && results[i].record == nil {
			err = ErrSandboxClaimReservationConflict
		}
		if err == nil {
			_, err = responses.Exec()
		}
		if err != nil {
			_ = responses.Close()
			return nil, err
		}
	}
	if err := responses.Close(); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return results, nil
}
