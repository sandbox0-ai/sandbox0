package session

import (
	"errors"
	"fmt"
	"maps"
	"math"
	"sort"
	"strings"

	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
)

type generationHandover struct {
	operationID string
	sandboxID   string
	sourceID    string
	from, to    int64
}

// RebindRuntimeGeneration updates the restored supervisor in place. It never
// reloads session files, applies runtime-recovery policy, restarts attempts or
// discards input receipts. The controller must keep APIs gated until it returns
// success. A partial filesystem failure stays retryable under the same exact
// operation, with the target generation retained in memory.
func (s *Supervisor) RebindRuntimeGeneration(operationID, sandboxID string, from, to int64) error {
	if from == math.MaxInt64 || to != from+1 {
		return errors.New("invalid session generation handover")
	}
	return s.RebindRuntimeIdentity(operationID, sandboxID, sandboxID, from, to)
}

// RebindRuntimeIdentity preserves restored process handles and journals while
// binding them to a resumed sandbox or an explicitly authorized fork. The caller
// must keep API admission closed and own regional restore authority.
func (s *Supervisor) RebindRuntimeIdentity(operationID, sourceID, sandboxID string, from, to int64) error {
	if (sourceID == sandboxID && (from == math.MaxInt64 || to != from+1)) ||
		(sourceID != sandboxID && to != 1) {
		return errors.New("invalid session generation handover")
	}
	return s.rebindRuntimeIdentity(operationID, sourceID, sandboxID, from, to)
}

// RebindCheckpoint preserves the captured session process even when failed
// destination attempts consumed intermediate regional generations. The exact
// restore assignment binds that predecessor; migration keeps its stricter rule.
func (s *Supervisor) RebindCheckpoint(request runtimecontrol.CheckpointRestoreAssignment) error {
	if err := request.Validate(); err != nil {
		return err
	}
	return s.rebindRuntimeIdentity(request.OperationID, request.Capture.SandboxID, request.Target.SandboxID,
		request.Capture.RuntimeGeneration, request.Target.RuntimeGeneration)
}

func (s *Supervisor) rebindRuntimeIdentity(operationID, sourceID, sandboxID string, from, to int64) error {
	if operationID == "" || len(operationID) > 256 || strings.TrimSpace(operationID) != operationID ||
		strings.ContainsAny(operationID, "\x00\r\n") || !validHandoverSandboxID(sourceID) ||
		!validHandoverSandboxID(sandboxID) || from <= 0 || to <= 0 {
		return errors.New("invalid session generation handover")
	}
	handover := generationHandover{operationID: operationID, sourceID: sourceID, sandboxID: sandboxID, from: from, to: to}
	s.mu.Lock()
	defer s.mu.Unlock()
	retrying := s.generationHandover == handover && s.sandboxID == sandboxID && s.runtimeGeneration == to
	if !s.active || (!retrying && (s.sandboxID != sourceID || s.runtimeGeneration != from)) {
		return errors.New("session handover does not match the active sandbox")
	}
	if s.generationHandoverPending && s.generationHandover != handover {
		return errors.New("another session generation handover is incomplete")
	}
	if retrying && !s.generationHandoverPending {
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
	s.sandboxID = sandboxID
	if sourceID != sandboxID {
		s.sandboxEnv = maps.Clone(s.sandboxEnv)
		if _, ok := s.sandboxEnv[runtimecontrol.EnvSandboxID]; ok {
			s.sandboxEnv[runtimecontrol.EnvSandboxID] = sandboxID
		}
	}
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
	if sourceID != sandboxID {
		if err := s.store.rebindCheckpointOwner(sourceID, sandboxID); err != nil {
			return err
		}
	}
	s.generationHandoverPending = false
	return nil
}

func validHandoverSandboxID(value string) bool {
	return value != "" && len(value) <= 256 && strings.TrimSpace(value) == value &&
		!strings.ContainsAny(value, "\x00\r\n")
}
