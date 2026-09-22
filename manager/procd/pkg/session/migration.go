package session

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
)

type generationHandover struct {
	operationID string
	sandboxID   string
	from, to    int64
}

// RebindRuntimeGeneration updates the restored supervisor in place. It never
// reloads session files, applies runtime-recovery policy, restarts attempts or
// discards input receipts. The controller must keep APIs gated until it returns
// success. A partial filesystem failure stays retryable under the same exact
// operation, with the target generation retained in memory.
func (s *Supervisor) RebindRuntimeGeneration(operationID, sandboxID string, from, to int64) error {
	if operationID == "" || len(operationID) > 256 || strings.TrimSpace(operationID) != operationID ||
		strings.ContainsAny(operationID, "\x00\r\n") || sandboxID == "" ||
		from <= 0 || from == math.MaxInt64 || to != from+1 {
		return errors.New("invalid session generation handover")
	}
	handover := generationHandover{operationID: operationID, sandboxID: sandboxID, from: from, to: to}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.active || s.sandboxID != sandboxID {
		return errors.New("session handover does not match the active sandbox")
	}
	if s.runtimeGeneration != from && (s.runtimeGeneration != to || s.generationHandover != handover) {
		return errors.New("session handover does not match the current generation")
	}
	if s.generationHandoverPending && s.generationHandover != handover {
		return errors.New("another session generation handover is incomplete")
	}
	if s.runtimeGeneration == to && !s.generationHandoverPending {
		return nil
	}
	// Lock the entire set in stable order. Creation and attempt startup use
	// supervisor -> session order too, so none can install a source-generation
	// attempt after this handover's in-memory commit.
	ids := make([]string, 0, len(s.sessions))
	for id := range s.sessions {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		s.sessions[id].mu.Lock()
	}
	defer func() {
		for i := len(ids) - 1; i >= 0; i-- {
			s.sessions[ids[i]].mu.Unlock()
		}
	}()
	s.runtimeGeneration = to
	s.generationHandover = handover
	s.generationHandoverPending = true
	for _, id := range ids {
		managed := s.sessions[id]
		managed.record.RuntimeGeneration = to
		// Attempt.RuntimeGeneration remains the generation where the same
		// attempt started; migration does not manufacture another attempt.
	}
	var failures []error
	for _, id := range ids {
		if err := s.saveLocked(s.sessions[id]); err != nil {
			failures = append(failures, fmt.Errorf("persist migrated session %s: %w", id, err))
		}
	}
	if err := errors.Join(failures...); err != nil {
		return err
	}
	s.generationHandoverPending = false
	return nil
}
