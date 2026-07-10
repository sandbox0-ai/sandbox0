package session

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/sandbox0-ai/sandbox0/manager/procd/pkg/process"
	cmdproc "github.com/sandbox0-ai/sandbox0/manager/procd/pkg/process/cmd"
	"go.uber.org/zap"
)

const maxInputReceipts = 1024

type attemptRuntime struct {
	attemptID       string
	proc            process.Process
	exitEvents      chan process.ExitEvent
	done            chan struct{}
	doneOnce        sync.Once
	ready           bool
	outputTail      []byte
	exitReason      string
	suppressRestart bool
}

type managedSession struct {
	mu            sync.Mutex
	inputMu       sync.Mutex
	record        Session
	journal       *Journal
	runtime       *attemptRuntime
	restartTimes  []time.Time
	restartCancel context.CancelFunc
	deleting      bool
}

// Supervisor owns process attempts, session state, and event journals.
type Supervisor struct {
	mu                sync.RWMutex
	store             *FileStore
	sessions          map[string]*managedSession
	creationKeys      map[string]string
	transients        map[string]process.Process
	sandboxID         string
	sandboxEnv        map[string]string
	runtimeGeneration int64
	initialized       bool
	ctx               context.Context
	cancel            context.CancelFunc
	logger            *zap.Logger
}

func NewSupervisor(store *FileStore, logger *zap.Logger) (*Supervisor, error) {
	if store == nil {
		return nil, errors.New("session store is required")
	}
	if logger == nil {
		logger = zap.NewNop()
	}
	ctx, cancel := context.WithCancel(context.Background())
	s := &Supervisor{
		store:        store,
		sessions:     map[string]*managedSession{},
		creationKeys: map[string]string{},
		transients:   map[string]process.Process{},
		sandboxEnv:   map[string]string{},
		ctx:          ctx,
		cancel:       cancel,
		logger:       logger,
	}
	go s.cleanupLoop()
	return s, nil
}

func (s *Supervisor) loadPersistedSessionsLocked() error {
	if len(s.sessions) > 0 {
		return nil
	}
	stored, err := s.store.Load()
	if err != nil {
		return err
	}
	loadedSessions := make(map[string]*managedSession, len(stored))
	loadedKeys := make(map[string]string)
	closeLoaded := func() {
		for _, managed := range loadedSessions {
			_ = managed.journal.Close()
		}
	}
	for _, record := range stored {
		if record.CreationKey != "" {
			if existingID, exists := loadedKeys[record.CreationKey]; exists {
				closeLoaded()
				return fmt.Errorf("sessions %s and %s share creation key %q", existingID, record.ID, record.CreationKey)
			}
			loadedKeys[record.CreationKey] = record.ID
		}
		path, err := s.store.JournalPath(record.ID)
		if err != nil {
			closeLoaded()
			return err
		}
		journal, err := OpenJournal(path, record.Spec.EventRetention, record.Cursor.Latest)
		if err != nil {
			closeLoaded()
			return fmt.Errorf("open journal for %s: %w", record.ID, err)
		}
		record.Cursor = journal.Cursor()
		loadedSessions[record.ID] = &managedSession{record: record, journal: journal}
	}
	s.sessions = loadedSessions
	s.creationKeys = loadedKeys
	return nil
}

// StartTransient registers and starts a legacy process through the shared supervisor.
// Transient processes are not persisted or exposed as execution sessions.
func (s *Supervisor) StartTransient(id string, proc process.Process) error {
	if strings.TrimSpace(id) == "" || proc == nil {
		return errors.New("transient id and process are required")
	}
	s.mu.Lock()
	if _, exists := s.transients[id]; exists {
		s.mu.Unlock()
		return ErrSessionExists
	}
	s.transients[id] = proc
	s.mu.Unlock()
	if err := proc.Start(); err != nil {
		s.mu.Lock()
		delete(s.transients, id)
		s.mu.Unlock()
		return err
	}
	return nil
}

// DeleteTransient stops and removes a transient process.
func (s *Supervisor) DeleteTransient(id string) error {
	s.mu.Lock()
	proc, exists := s.transients[id]
	if exists {
		delete(s.transients, id)
	}
	s.mu.Unlock()
	if !exists {
		return ErrSessionNotFound
	}
	return proc.Stop()
}

// RestartTransient restarts a transient process without changing its identity.
func (s *Supervisor) RestartTransient(id string) error {
	s.mu.RLock()
	proc, exists := s.transients[id]
	s.mu.RUnlock()
	if !exists {
		return ErrSessionNotFound
	}
	return proc.Restart()
}

// Initialize binds persisted state to the sandbox, provides sandbox-scoped
// defaults, and reconciles sessions after a runtime replacement.
func (s *Supervisor) Initialize(sandboxID string, runtimeGeneration int64, sandboxEnv map[string]string) error {
	sandboxID = strings.TrimSpace(sandboxID)
	s.mu.Lock()
	if s.initialized {
		if s.sandboxID != sandboxID {
			s.mu.Unlock()
			return fmt.Errorf("session supervisor is already bound to sandbox %q", s.sandboxID)
		}
		s.runtimeGeneration = runtimeGeneration
		s.sandboxEnv = process.CloneEnvVars(sandboxEnv)
		s.mu.Unlock()
		return nil
	}
	bound, err := s.store.BindSandbox(sandboxID)
	if err != nil {
		s.mu.Unlock()
		return err
	}
	if !bound {
		if err := s.store.ResetForSandbox(sandboxID); err != nil {
			s.mu.Unlock()
			return err
		}
		s.sessions = map[string]*managedSession{}
		s.creationKeys = map[string]string{}
	}
	// Rootfs restore happens after the new procd process starts. Load only now,
	// after manager has applied the checkpoint into the portal backing store.
	if err := s.loadPersistedSessionsLocked(); err != nil {
		s.mu.Unlock()
		return err
	}
	s.initialized = true
	s.sandboxID = sandboxID
	s.runtimeGeneration = runtimeGeneration
	s.sandboxEnv = process.CloneEnvVars(sandboxEnv)
	sessions := make([]*managedSession, 0, len(s.sessions))
	for _, managed := range s.sessions {
		sessions = append(sessions, managed)
	}
	s.mu.Unlock()

	for _, managed := range sessions {
		shouldStart, err := s.recoverPersistedSession(managed, runtimeGeneration)
		if err != nil {
			return err
		}
		if shouldStart {
			if err := s.startAttempt(managed, "runtime_recovered"); err != nil {
				s.logger.Warn("Failed to recover session attempt",
					zap.String("session_id", managed.record.ID),
					zap.Error(err),
				)
			}
		}
	}
	return nil
}

// SetSandboxEnvVars replaces sandbox-scoped defaults used by future attempts.
func (s *Supervisor) SetSandboxEnvVars(sandboxEnv map[string]string) {
	s.mu.Lock()
	s.sandboxEnv = process.CloneEnvVars(sandboxEnv)
	s.mu.Unlock()
}

func (s *Supervisor) Create(spec SessionSpec, creationKey string) (*Session, bool, error) {
	spec = normalizeSpec(spec)
	if err := validateSpec(spec); err != nil {
		return nil, false, err
	}
	creationKey = strings.TrimSpace(creationKey)

	// Serialize creation so the idempotency key is a true uniqueness boundary,
	// including when identical retries arrive concurrently.
	s.mu.Lock()
	if existingID, exists := s.creationKeys[creationKey]; creationKey != "" && exists {
		existing := s.sessions[existingID]
		if existing == nil {
			delete(s.creationKeys, creationKey)
		} else {
			existing.mu.Lock()
			matchesSpec := reflect.DeepEqual(existing.record.Spec, spec)
			deleting := existing.deleting
			snapshot := cloneSession(existing.record)
			existing.mu.Unlock()
			s.mu.Unlock()
			if deleting || !matchesSpec {
				return nil, false, ErrSessionExists
			}
			return &snapshot, true, nil
		}
	}

	now := time.Now().UTC()
	id := ""
	for id == "" {
		candidate := "ses-" + uuid.NewString()[:12]
		if _, exists := s.sessions[candidate]; !exists {
			id = candidate
		}
	}
	record := Session{
		ID:                id,
		Spec:              spec,
		SpecVersion:       1,
		Phase:             PhasePending,
		RuntimeGeneration: s.runtimeGeneration,
		CreatedAt:         now,
		UpdatedAt:         now,
		LastActivityAt:    now,
		CreationKey:       creationKey,
	}
	path, err := s.store.JournalPath(record.ID)
	if err != nil {
		s.mu.Unlock()
		return nil, false, err
	}
	journal, err := OpenJournal(path, spec.EventRetention, 0)
	if err != nil {
		s.mu.Unlock()
		return nil, false, err
	}
	managed := &managedSession{record: record, journal: journal}
	managed.mu.Lock()
	if _, err := s.appendEventLocked(managed, Event{Type: "session.created"}); err != nil {
		managed.mu.Unlock()
		_ = journal.Close()
		_ = s.store.Delete(record.ID)
		s.mu.Unlock()
		return nil, false, err
	}
	if err := s.saveLocked(managed); err != nil {
		managed.mu.Unlock()
		_ = journal.Close()
		_ = s.store.Delete(record.ID)
		s.mu.Unlock()
		return nil, false, err
	}
	managed.mu.Unlock()

	s.sessions[record.ID] = managed
	if creationKey != "" {
		s.creationKeys[creationKey] = record.ID
	}
	initialized := s.initialized
	s.mu.Unlock()

	if initialized && spec.Lifecycle.DesiredState == DesiredStateRunning {
		if err := s.startAttempt(managed, "created"); err != nil {
			s.logger.Warn("Failed to start created session",
				zap.String("session_id", record.ID),
				zap.Error(err),
			)
		}
	}
	snapshot := s.snapshot(managed)
	return &snapshot, false, nil
}

func (s *Supervisor) List() []Session {
	s.mu.RLock()
	managed := make([]*managedSession, 0, len(s.sessions))
	for _, item := range s.sessions {
		managed = append(managed, item)
	}
	s.mu.RUnlock()
	result := make([]Session, 0, len(managed))
	for _, item := range managed {
		result = append(result, s.snapshot(item))
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].CreatedAt.Equal(result[j].CreatedAt) {
			return result[i].ID < result[j].ID
		}
		return result[i].CreatedAt.Before(result[j].CreatedAt)
	})
	return result
}

func (s *Supervisor) Get(id string) (*Session, error) {
	managed, err := s.getManaged(id)
	if err != nil {
		return nil, err
	}
	snapshot := s.snapshot(managed)
	return &snapshot, nil
}

func (s *Supervisor) Update(id string, spec SessionSpec) (*Session, error) {
	spec = normalizeSpec(spec)
	if err := validateSpec(spec); err != nil {
		return nil, err
	}
	managed, err := s.getManaged(id)
	if err != nil {
		return nil, err
	}
	managed.mu.Lock()
	oldSpec := managed.record.Spec
	hadRuntime := managed.runtime != nil
	managed.record.Spec = spec
	managed.record.SpecVersion++
	managed.record.UpdatedAt = time.Now().UTC()
	if _, err := s.appendEventLocked(managed, Event{Type: "session.updated"}); err != nil {
		managed.mu.Unlock()
		return nil, err
	}
	if err := managed.journal.SetRetention(spec.EventRetention); err != nil {
		managed.mu.Unlock()
		return nil, err
	}
	if err := s.saveLocked(managed); err != nil {
		managed.mu.Unlock()
		return nil, err
	}
	managed.mu.Unlock()

	executionChanged := !reflect.DeepEqual(executionSpec(oldSpec), executionSpec(spec))
	if spec.Lifecycle.DesiredState == DesiredStateStopped {
		if err := s.stopAttempt(managed, "desired_state_stopped"); err != nil {
			return nil, err
		}
	} else if executionChanged && hadRuntime {
		if err := s.stopAttempt(managed, "spec_replaced"); err != nil {
			return nil, err
		}
		if err := s.startAttempt(managed, "spec_updated"); err != nil {
			return nil, err
		}
	} else if !hadRuntime {
		if err := s.startAttempt(managed, "desired_state_running"); err != nil {
			return nil, err
		}
	}
	snapshot := s.snapshot(managed)
	return &snapshot, nil
}

func (s *Supervisor) SetDesiredState(id string, state DesiredState) (*Session, error) {
	if state != DesiredStateRunning && state != DesiredStateStopped {
		return nil, errors.New("invalid desired state")
	}
	managed, err := s.getManaged(id)
	if err != nil {
		return nil, err
	}
	managed.mu.Lock()
	managed.record.Spec.Lifecycle.DesiredState = state
	managed.record.SpecVersion++
	managed.record.UpdatedAt = time.Now().UTC()
	if _, err := s.appendEventLocked(managed, Event{Type: "session.desired_state", Reason: string(state)}); err != nil {
		managed.mu.Unlock()
		return nil, err
	}
	if err := s.saveLocked(managed); err != nil {
		managed.mu.Unlock()
		return nil, err
	}
	hasRuntime := managed.runtime != nil
	managed.mu.Unlock()
	if state == DesiredStateStopped {
		if err := s.stopAttempt(managed, "desired_state_stopped"); err != nil {
			return nil, err
		}
	} else if !hasRuntime {
		if err := s.startAttempt(managed, "desired_state_running"); err != nil {
			return nil, err
		}
	}
	snapshot := s.snapshot(managed)
	return &snapshot, nil
}

func (s *Supervisor) CreateAttempt(id string, replaceCurrent bool) (*Session, error) {
	managed, err := s.getManaged(id)
	if err != nil {
		return nil, err
	}
	managed.mu.Lock()
	hasRuntime := managed.runtime != nil
	managed.mu.Unlock()
	if hasRuntime && !replaceCurrent {
		return nil, errors.New("session already has a running attempt")
	}
	if hasRuntime {
		if err := s.stopAttempt(managed, "attempt_replaced"); err != nil {
			return nil, err
		}
	}
	managed.mu.Lock()
	managed.record.Spec.Lifecycle.DesiredState = DesiredStateRunning
	managed.record.SpecVersion++
	if err := s.saveLocked(managed); err != nil {
		managed.mu.Unlock()
		return nil, err
	}
	managed.mu.Unlock()
	if err := s.startAttempt(managed, "attempt_created"); err != nil {
		return nil, err
	}
	snapshot := s.snapshot(managed)
	return &snapshot, nil
}

func (s *Supervisor) Delete(id string) error {
	managed, err := s.getManaged(id)
	if err != nil {
		return err
	}
	managed.mu.Lock()
	managed.deleting = true
	managed.record.Spec.Lifecycle.DesiredState = DesiredStateStopped
	managed.mu.Unlock()
	if err := s.stopAttempt(managed, "session_deleted"); err != nil {
		return err
	}
	s.mu.Lock()
	delete(s.sessions, id)
	if managed.record.CreationKey != "" {
		delete(s.creationKeys, managed.record.CreationKey)
	}
	s.mu.Unlock()
	managed.mu.Lock()
	if managed.restartCancel != nil {
		managed.restartCancel()
		managed.restartCancel = nil
	}
	managed.mu.Unlock()
	if err := managed.journal.Close(); err != nil {
		return err
	}
	return s.store.Delete(id)
}

func (s *Supervisor) WriteInput(id string, request InputRequest) (*InputResponse, error) {
	managed, err := s.getManaged(id)
	if err != nil {
		return nil, err
	}
	request.InputID = strings.TrimSpace(request.InputID)
	if request.InputID == "" {
		return nil, errors.New("input_id is required")
	}
	data, err := base64.StdEncoding.DecodeString(request.DataBase64)
	if err != nil {
		return nil, errors.New("data_base64 is invalid")
	}
	if len(data) == 0 && !request.EOF {
		return nil, errors.New("data_base64 or eof is required")
	}
	managed.inputMu.Lock()
	defer managed.inputMu.Unlock()

	managed.mu.Lock()
	runtime := managed.runtime
	attemptID := ""
	if managed.record.Attempt != nil {
		attemptID = managed.record.Attempt.ID
	}
	if request.ExpectedAttemptID != "" && request.ExpectedAttemptID != attemptID {
		managed.mu.Unlock()
		return nil, ErrAttemptMismatch
	}
	digest := inputDigest(attemptID, data, request.EOF)
	for _, receipt := range managed.record.InputReceipts {
		if receipt.InputID != request.InputID {
			continue
		}
		if receipt.Digest != digest || receipt.AttemptID != attemptID {
			managed.mu.Unlock()
			return nil, ErrInputAlreadyExists
		}
		response := &InputResponse{InputID: request.InputID, AttemptID: attemptID, Accepted: true, Duplicate: true}
		managed.mu.Unlock()
		return response, nil
	}
	if runtime == nil {
		managed.mu.Unlock()
		return nil, ErrSessionNotRunning
	}
	proc := runtime.proc
	attemptID = runtime.attemptID
	managed.mu.Unlock()

	if len(data) > 0 {
		if err := proc.WriteInput(data); err != nil {
			return nil, err
		}
	}
	if request.EOF {
		closer, ok := proc.(interface{ CloseInput() error })
		if !ok {
			return nil, errors.New("process input cannot be closed")
		}
		if err := closer.CloseInput(); err != nil {
			return nil, err
		}
	}

	managed.mu.Lock()
	if managed.runtime == nil || managed.runtime.attemptID != attemptID {
		managed.mu.Unlock()
		return nil, ErrAttemptMismatch
	}
	now := time.Now().UTC()
	managed.record.InputReceipts = append(managed.record.InputReceipts, InputReceipt{
		InputID: request.InputID, AttemptID: attemptID, Digest: digest, AcceptedAt: now,
	})
	if len(managed.record.InputReceipts) > maxInputReceipts {
		managed.record.InputReceipts = append([]InputReceipt(nil), managed.record.InputReceipts[len(managed.record.InputReceipts)-maxInputReceipts:]...)
	}
	managed.record.LastActivityAt = now
	managed.record.UpdatedAt = now
	_, _ = s.appendEventLocked(managed, Event{Type: "input.accepted", AttemptID: attemptID})
	if err := s.saveLocked(managed); err != nil {
		managed.mu.Unlock()
		return nil, err
	}
	managed.mu.Unlock()
	return &InputResponse{InputID: request.InputID, AttemptID: attemptID, Accepted: true}, nil
}

func (s *Supervisor) SendSignal(id, expectedAttemptID string, signal syscall.Signal) error {
	managed, err := s.getManaged(id)
	if err != nil {
		return err
	}
	managed.mu.Lock()
	runtime := managed.runtime
	if runtime == nil {
		managed.mu.Unlock()
		return ErrSessionNotRunning
	}
	if expectedAttemptID != "" && expectedAttemptID != runtime.attemptID {
		managed.mu.Unlock()
		return ErrAttemptMismatch
	}
	proc := runtime.proc
	attemptID := runtime.attemptID
	managed.mu.Unlock()
	if err := proc.SendSignal(signal); err != nil {
		return err
	}
	managed.mu.Lock()
	_, _ = s.appendEventLocked(managed, Event{Type: "signal.sent", AttemptID: attemptID, Reason: signal.String()})
	managed.record.LastActivityAt = time.Now().UTC()
	err = s.saveLocked(managed)
	managed.mu.Unlock()
	return err
}

func (s *Supervisor) ResizeTerminal(id, expectedAttemptID string, rows, cols uint16) error {
	if rows == 0 || cols == 0 {
		return errors.New("rows and cols must be greater than zero")
	}
	managed, err := s.getManaged(id)
	if err != nil {
		return err
	}
	managed.mu.Lock()
	runtime := managed.runtime
	if runtime == nil {
		managed.mu.Unlock()
		return ErrSessionNotRunning
	}
	if expectedAttemptID != "" && expectedAttemptID != runtime.attemptID {
		managed.mu.Unlock()
		return ErrAttemptMismatch
	}
	proc := runtime.proc
	attemptID := runtime.attemptID
	managed.mu.Unlock()
	if err := proc.ResizePTY(process.PTYSize{Rows: rows, Cols: cols}); err != nil {
		return err
	}
	managed.mu.Lock()
	_, _ = s.appendEventLocked(managed, Event{Type: "terminal.resized", AttemptID: attemptID})
	managed.record.LastActivityAt = time.Now().UTC()
	err = s.saveLocked(managed)
	managed.mu.Unlock()
	return err
}

func (s *Supervisor) Events(id string, after int64, limit int) (EventPage, error) {
	managed, err := s.getManaged(id)
	if err != nil {
		return EventPage{}, err
	}
	return managed.journal.Read(after, limit)
}

func (s *Supervisor) Subscribe(id string, after int64) ([]Event, <-chan Event, func(), EventCursor, error) {
	managed, err := s.getManaged(id)
	if err != nil {
		return nil, nil, nil, EventCursor{}, err
	}
	return managed.journal.Subscribe(after)
}

func (s *Supervisor) PauseAll() error {
	s.mu.RLock()
	sessions := make([]*managedSession, 0, len(s.sessions))
	for _, managed := range s.sessions {
		sessions = append(sessions, managed)
	}
	transients := make([]process.Process, 0, len(s.transients))
	for _, proc := range s.transients {
		transients = append(transients, proc)
	}
	s.mu.RUnlock()
	var errs []error
	for _, proc := range transients {
		if proc.IsRunning() {
			errs = append(errs, proc.Pause())
		}
	}
	for _, managed := range sessions {
		managed.mu.Lock()
		runtime := managed.runtime
		if runtime == nil || !runtime.proc.IsRunning() {
			managed.mu.Unlock()
			continue
		}
		proc := runtime.proc
		attemptID := runtime.attemptID
		managed.mu.Unlock()
		if err := proc.Pause(); err != nil {
			errs = append(errs, err)
			continue
		}
		managed.mu.Lock()
		managed.record.Phase = PhasePaused
		managed.record.UpdatedAt = time.Now().UTC()
		_, _ = s.appendEventLocked(managed, Event{Type: "session.paused", AttemptID: attemptID})
		if err := s.saveLocked(managed); err != nil {
			errs = append(errs, err)
		}
		if err := managed.journal.Flush(); err != nil {
			errs = append(errs, err)
		}
		managed.mu.Unlock()
	}
	return errors.Join(errs...)
}

func (s *Supervisor) ResumeAll() error {
	s.mu.RLock()
	sessions := make([]*managedSession, 0, len(s.sessions))
	for _, managed := range s.sessions {
		sessions = append(sessions, managed)
	}
	transients := make([]process.Process, 0, len(s.transients))
	for _, proc := range s.transients {
		transients = append(transients, proc)
	}
	s.mu.RUnlock()
	var errs []error
	for _, proc := range transients {
		if proc.IsPaused() {
			errs = append(errs, proc.Resume())
		}
	}
	for _, managed := range sessions {
		managed.mu.Lock()
		runtime := managed.runtime
		if runtime == nil || !runtime.proc.IsPaused() {
			managed.mu.Unlock()
			continue
		}
		proc := runtime.proc
		attemptID := runtime.attemptID
		managed.mu.Unlock()
		if err := proc.Resume(); err != nil {
			errs = append(errs, err)
			continue
		}
		managed.mu.Lock()
		managed.record.Phase = PhaseRunning
		managed.record.UpdatedAt = time.Now().UTC()
		_, _ = s.appendEventLocked(managed, Event{Type: "session.resumed", AttemptID: attemptID})
		if err := s.saveLocked(managed); err != nil {
			errs = append(errs, err)
		}
		managed.mu.Unlock()
	}
	return errors.Join(errs...)
}

func (s *Supervisor) Flush() error {
	s.mu.RLock()
	sessions := make([]*managedSession, 0, len(s.sessions))
	for _, managed := range s.sessions {
		sessions = append(sessions, managed)
	}
	s.mu.RUnlock()
	var errs []error
	for _, managed := range sessions {
		managed.mu.Lock()
		managed.record.Cursor = managed.journal.Cursor()
		if err := s.saveLocked(managed); err != nil {
			errs = append(errs, err)
		}
		if err := managed.journal.Flush(); err != nil {
			errs = append(errs, err)
		}
		managed.mu.Unlock()
	}
	return errors.Join(errs...)
}

func (s *Supervisor) Close() error {
	s.cancel()
	s.mu.RLock()
	sessions := make([]*managedSession, 0, len(s.sessions))
	for _, managed := range s.sessions {
		sessions = append(sessions, managed)
	}
	s.mu.RUnlock()
	var errs []error
	for _, managed := range sessions {
		managed.mu.Lock()
		if managed.restartCancel != nil {
			managed.restartCancel()
		}
		managed.record.Cursor = managed.journal.Cursor()
		if err := s.saveLocked(managed); err != nil {
			errs = append(errs, err)
		}
		managed.mu.Unlock()
		if err := managed.journal.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (s *Supervisor) startAttempt(managed *managedSession, reason string) error {
	s.mu.RLock()
	initialized := s.initialized
	runtimeGeneration := s.runtimeGeneration
	sandboxEnv := process.CloneEnvVars(s.sandboxEnv)
	s.mu.RUnlock()
	if !initialized {
		return nil
	}

	managed.mu.Lock()
	if managed.deleting || managed.record.Spec.Lifecycle.DesiredState != DesiredStateRunning {
		managed.mu.Unlock()
		return nil
	}
	if managed.runtime != nil {
		managed.mu.Unlock()
		return nil
	}
	if managed.restartCancel != nil {
		managed.restartCancel()
		managed.restartCancel = nil
	}
	number := int64(1)
	if managed.record.Attempt != nil {
		number = managed.record.Attempt.Number + 1
	}
	attemptID := "att-" + uuid.NewString()[:12]
	attempt := &Attempt{ID: attemptID, Number: number, RuntimeGeneration: runtimeGeneration}
	managed.record.Attempt = attempt
	managed.record.RuntimeGeneration = runtimeGeneration
	managed.record.Phase = PhaseStarting
	managed.record.UpdatedAt = time.Now().UTC()
	config := process.ProcessConfig{
		Type:       process.ProcessTypeCMD,
		Command:    append([]string(nil), managed.record.Spec.Command...),
		CWD:        managed.record.Spec.CWD,
		EnvVars:    process.MergeEnvVars(sandboxEnv, managed.record.Spec.Env),
		BufferSize: 256,
		PipeStdin:  managed.record.Spec.IO.Mode == IOModePipes,
	}
	if managed.record.Spec.IO.Mode == IOModePTY {
		terminal := managed.record.Spec.IO.Terminal
		config.PTYSize = &process.PTYSize{Rows: terminal.Rows, Cols: terminal.Cols}
		config.Term = terminal.Term
	}
	proc, err := cmdproc.NewCMD(attemptID, config, config.Command)
	if err != nil {
		managed.record.Phase = PhaseFailed
		managed.mu.Unlock()
		return err
	}
	output := make(chan process.ProcessOutput, 256)
	proc.SetReliableOutput(output)
	runtime := &attemptRuntime{
		attemptID:  attemptID,
		proc:       proc,
		exitEvents: make(chan process.ExitEvent, 1),
		done:       make(chan struct{}),
	}
	proc.AddExitHandler(func(event process.ExitEvent) {
		select {
		case runtime.exitEvents <- event:
		default:
		}
	})
	managed.runtime = runtime
	if _, err := s.appendEventLocked(managed, Event{Type: "attempt.starting", AttemptID: attemptID, Reason: reason}); err != nil {
		managed.runtime = nil
		managed.mu.Unlock()
		return err
	}
	if err := s.saveLocked(managed); err != nil {
		managed.runtime = nil
		managed.mu.Unlock()
		return err
	}
	managed.mu.Unlock()

	if err := proc.Start(); err != nil {
		s.handleStartFailure(managed, runtime, err)
		return err
	}
	go s.pumpOutput(managed, runtime, output)

	managed.mu.Lock()
	if managed.runtime != runtime {
		managed.mu.Unlock()
		return nil
	}
	now := time.Now().UTC()
	managed.record.Attempt.PID = proc.PID()
	managed.record.Attempt.StartedAt = &now
	managed.record.UpdatedAt = now
	_, _ = s.appendEventLocked(managed, Event{Type: "attempt.started", AttemptID: attemptID})
	readiness := managed.record.Spec.Readiness
	_ = s.saveLocked(managed)
	managed.mu.Unlock()

	s.startReadiness(managed, runtime, readiness)
	return nil
}

func (s *Supervisor) startReadiness(managed *managedSession, runtime *attemptRuntime, readiness ReadinessSpec) {
	switch readiness.Type {
	case ReadinessProcess:
		s.markReady(managed, runtime)
	case ReadinessDelay:
		go func() {
			timer := time.NewTimer(time.Duration(readiness.DelayMS) * time.Millisecond)
			defer timer.Stop()
			select {
			case <-s.ctx.Done():
			case <-runtime.done:
			case <-timer.C:
				s.markReady(managed, runtime)
			}
		}()
	case ReadinessOutput:
	}
	if readiness.Type != ReadinessProcess && readiness.TimeoutMS > 0 {
		go func() {
			timer := time.NewTimer(time.Duration(readiness.TimeoutMS) * time.Millisecond)
			defer timer.Stop()
			select {
			case <-s.ctx.Done():
			case <-runtime.done:
			case <-timer.C:
				s.failReadiness(managed, runtime)
			}
		}()
	}
}

func (s *Supervisor) pumpOutput(managed *managedSession, runtime *attemptRuntime, output <-chan process.ProcessOutput) {
	for item := range output {
		managed.mu.Lock()
		if managed.runtime != runtime {
			managed.mu.Unlock()
			return
		}
		now := time.Now().UTC()
		managed.record.LastActivityAt = now
		managed.record.UpdatedAt = now
		_, err := s.appendEventLocked(managed, Event{
			Type:       "output",
			AttemptID:  runtime.attemptID,
			Stream:     string(item.Source),
			DataBase64: base64.StdEncoding.EncodeToString(item.Data),
		})
		ready := s.observeReadinessLocked(managed, runtime, item.Data)
		managed.mu.Unlock()
		if err != nil {
			s.logger.Warn("Failed to append session output", zap.String("session_id", managed.record.ID), zap.Error(err))
		}
		if ready {
			s.markReady(managed, runtime)
		}
	}
	var event process.ExitEvent
	select {
	case event = <-runtime.exitEvents:
	case <-s.ctx.Done():
		return
	}
	s.handleExit(managed, runtime, event)
}

func (s *Supervisor) observeReadinessLocked(managed *managedSession, runtime *attemptRuntime, data []byte) bool {
	if runtime.ready || managed.record.Spec.Readiness.Type != ReadinessOutput {
		return false
	}
	token := []byte(managed.record.Spec.Readiness.Output)
	combined := make([]byte, 0, len(runtime.outputTail)+len(data))
	combined = append(combined, runtime.outputTail...)
	combined = append(combined, data...)
	if bytes.Contains(combined, token) {
		return true
	}
	keep := len(token) - 1
	if keep < 0 {
		keep = 0
	}
	if len(combined) > keep {
		combined = combined[len(combined)-keep:]
	}
	runtime.outputTail = append(runtime.outputTail[:0], combined...)
	return false
}

func (s *Supervisor) markReady(managed *managedSession, runtime *attemptRuntime) {
	managed.mu.Lock()
	defer managed.mu.Unlock()
	if managed.runtime != runtime || runtime.ready {
		return
	}
	runtime.ready = true
	managed.record.Phase = PhaseRunning
	managed.record.UpdatedAt = time.Now().UTC()
	_, _ = s.appendEventLocked(managed, Event{Type: "session.ready", AttemptID: runtime.attemptID})
	if err := s.saveLocked(managed); err != nil {
		s.logger.Warn("Failed to persist ready session", zap.String("session_id", managed.record.ID), zap.Error(err))
	}
}

func (s *Supervisor) failReadiness(managed *managedSession, runtime *attemptRuntime) {
	managed.mu.Lock()
	if managed.runtime != runtime || runtime.ready {
		managed.mu.Unlock()
		return
	}
	runtime.exitReason = "readiness_timeout"
	proc := runtime.proc
	grace := time.Duration(managed.record.Spec.Lifecycle.StopGracePeriodSeconds) * time.Second
	managed.mu.Unlock()
	_ = proc.SendSignal(syscall.SIGTERM)
	go s.killAfterGrace(runtime, proc, grace)
}

func (s *Supervisor) handleStartFailure(managed *managedSession, runtime *attemptRuntime, startErr error) {
	managed.mu.Lock()
	if managed.runtime != runtime {
		managed.mu.Unlock()
		return
	}
	now := time.Now().UTC()
	managed.record.Attempt.FinishedAt = &now
	managed.record.Attempt.Reason = "start_failed"
	managed.record.Phase = PhaseFailed
	managed.record.UpdatedAt = now
	_, _ = s.appendEventLocked(managed, Event{Type: "attempt.exited", AttemptID: runtime.attemptID, Reason: "start_failed"})
	managed.runtime = nil
	runtime.doneOnce.Do(func() { close(runtime.done) })
	shouldRestart, delay := s.restartDecisionLocked(managed, 1)
	_ = s.saveLocked(managed)
	managed.mu.Unlock()
	s.logger.Warn("Session attempt failed to start", zap.String("session_id", managed.record.ID), zap.Error(startErr))
	if shouldRestart {
		s.scheduleRestart(managed, delay)
	}
}

func (s *Supervisor) handleExit(managed *managedSession, runtime *attemptRuntime, event process.ExitEvent) {
	managed.mu.Lock()
	if managed.runtime != runtime {
		managed.mu.Unlock()
		return
	}
	now := time.Now().UTC()
	exitCode := event.ExitCode
	managed.record.Attempt.FinishedAt = &now
	managed.record.Attempt.ExitCode = &exitCode
	managed.record.Attempt.PID = 0
	reason := runtime.exitReason
	if reason == "" {
		reason = string(event.State)
	}
	managed.record.Attempt.Reason = reason
	managed.record.UpdatedAt = now
	managed.record.LastActivityAt = now
	_, _ = s.appendEventLocked(managed, Event{
		Type: "attempt.exited", AttemptID: runtime.attemptID, ExitCode: &exitCode, Reason: reason,
	})
	managed.runtime = nil
	runtime.doneOnce.Do(func() { close(runtime.done) })
	shouldRestart := false
	delay := time.Duration(0)
	if managed.deleting || runtime.suppressRestart || managed.record.Spec.Lifecycle.DesiredState == DesiredStateStopped {
		managed.record.Phase = PhaseStopped
	} else {
		shouldRestart, delay = s.restartDecisionLocked(managed, exitCode)
		if !shouldRestart && managed.record.Phase != PhaseFailed {
			managed.record.Phase = PhaseExited
		}
	}
	if err := s.saveLocked(managed); err != nil {
		s.logger.Warn("Failed to persist exited session", zap.String("session_id", managed.record.ID), zap.Error(err))
	}
	managed.mu.Unlock()
	if shouldRestart {
		s.scheduleRestart(managed, delay)
	}
}

func (s *Supervisor) restartDecisionLocked(managed *managedSession, exitCode int) (bool, time.Duration) {
	restart := managed.record.Spec.Lifecycle.Restart
	shouldRestart := restart.Policy == RestartPolicyAlways || (restart.Policy == RestartPolicyOnFailure && exitCode != 0)
	if !shouldRestart {
		return false, 0
	}
	now := time.Now()
	window := time.Duration(restart.WindowSeconds) * time.Second
	kept := managed.restartTimes[:0]
	for _, attempt := range managed.restartTimes {
		if window == 0 || now.Sub(attempt) <= window {
			kept = append(kept, attempt)
		}
	}
	managed.restartTimes = kept
	if restart.MaxRestarts > 0 && len(managed.restartTimes) >= int(restart.MaxRestarts) {
		managed.record.Phase = PhaseFailed
		_, _ = s.appendEventLocked(managed, Event{Type: "session.failed", Reason: "restart_limit_exceeded"})
		return false, 0
	}
	managed.restartTimes = append(managed.restartTimes, now)
	managed.record.RestartCount++
	managed.record.Phase = PhaseBackoff
	backoff := time.Duration(restart.InitialBackoffMS) * time.Millisecond
	for i := 1; i < len(managed.restartTimes); i++ {
		backoff *= 2
		maximum := time.Duration(restart.MaxBackoffMS) * time.Millisecond
		if maximum > 0 && backoff >= maximum {
			backoff = maximum
			break
		}
	}
	_, _ = s.appendEventLocked(managed, Event{Type: "session.backoff", Reason: backoff.String()})
	return true, backoff
}

func (s *Supervisor) scheduleRestart(managed *managedSession, delay time.Duration) {
	ctx, cancel := context.WithCancel(s.ctx)
	managed.mu.Lock()
	if managed.deleting || managed.record.Spec.Lifecycle.DesiredState != DesiredStateRunning {
		managed.mu.Unlock()
		cancel()
		return
	}
	if managed.restartCancel != nil {
		managed.restartCancel()
	}
	managed.restartCancel = cancel
	managed.mu.Unlock()
	go func() {
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
		if err := s.startAttempt(managed, "restart_policy"); err != nil {
			s.logger.Warn("Failed to restart session", zap.String("session_id", managed.record.ID), zap.Error(err))
		}
	}()
}

func (s *Supervisor) stopAttempt(managed *managedSession, reason string) error {
	managed.mu.Lock()
	runtime := managed.runtime
	if runtime == nil {
		managed.record.Phase = PhaseStopped
		managed.record.UpdatedAt = time.Now().UTC()
		err := s.saveLocked(managed)
		managed.mu.Unlock()
		return err
	}
	runtime.exitReason = reason
	runtime.suppressRestart = true
	managed.record.Phase = PhaseStopping
	managed.record.UpdatedAt = time.Now().UTC()
	_, _ = s.appendEventLocked(managed, Event{Type: "attempt.stopping", AttemptID: runtime.attemptID, Reason: reason})
	if err := s.saveLocked(managed); err != nil {
		managed.mu.Unlock()
		return err
	}
	proc := runtime.proc
	done := runtime.done
	grace := time.Duration(managed.record.Spec.Lifecycle.StopGracePeriodSeconds) * time.Second
	managed.mu.Unlock()

	if err := proc.SendSignal(syscall.SIGTERM); err != nil && !errors.Is(err, process.ErrProcessNotRunning) {
		return err
	}
	if grace <= 0 {
		grace = time.Duration(defaultStopGraceSeconds) * time.Second
	}
	timer := time.NewTimer(grace)
	defer timer.Stop()
	select {
	case <-done:
		return nil
	case <-timer.C:
		_ = proc.SendSignal(syscall.SIGKILL)
	}
	select {
	case <-done:
		return nil
	case <-time.After(5 * time.Second):
		return errors.New("timed out waiting for session process to stop")
	}
}

func (s *Supervisor) killAfterGrace(runtime *attemptRuntime, proc process.Process, grace time.Duration) {
	if grace <= 0 {
		grace = time.Duration(defaultStopGraceSeconds) * time.Second
	}
	timer := time.NewTimer(grace)
	defer timer.Stop()
	select {
	case <-runtime.done:
	case <-s.ctx.Done():
	case <-timer.C:
		_ = proc.SendSignal(syscall.SIGKILL)
	}
}

func (s *Supervisor) recoverPersistedSession(managed *managedSession, runtimeGeneration int64) (bool, error) {
	managed.mu.Lock()
	defer managed.mu.Unlock()
	now := time.Now().UTC()
	wasActive := managed.record.Phase == PhasePending || managed.record.Phase == PhaseStarting || managed.record.Phase == PhaseRunning || managed.record.Phase == PhasePaused || managed.record.Phase == PhaseBackoff || managed.record.Phase == PhaseSuspended
	if managed.record.Attempt != nil && managed.record.Attempt.FinishedAt == nil {
		wasActive = true
		managed.record.Attempt.FinishedAt = &now
		managed.record.Attempt.PID = 0
		managed.record.Attempt.Reason = "runtime_replaced"
		managed.record.Phase = PhaseSuspended
		_, _ = s.appendEventLocked(managed, Event{
			Type: "attempt.exited", AttemptID: managed.record.Attempt.ID, Reason: "runtime_replaced",
		})
	}
	managed.record.RuntimeGeneration = runtimeGeneration
	managed.record.UpdatedAt = now
	if managed.record.Spec.Lifecycle.DesiredState != DesiredStateRunning {
		managed.record.Phase = PhaseStopped
		return false, s.saveLocked(managed)
	}
	if !wasActive {
		return false, s.saveLocked(managed)
	}
	if managed.record.Spec.Lifecycle.RuntimeRecovery == RuntimeRecoveryStop {
		managed.record.Spec.Lifecycle.DesiredState = DesiredStateStopped
		managed.record.Phase = PhaseStopped
		_, _ = s.appendEventLocked(managed, Event{Type: "session.stopped", Reason: "runtime_recovery_policy"})
		return false, s.saveLocked(managed)
	}
	if err := s.saveLocked(managed); err != nil {
		return false, err
	}
	return true, nil
}

func (s *Supervisor) appendEventLocked(managed *managedSession, event Event) (Event, error) {
	event.SessionID = managed.record.ID
	event.RuntimeGeneration = managed.record.RuntimeGeneration
	if event.AttemptID == "" && managed.record.Attempt != nil {
		event.AttemptID = managed.record.Attempt.ID
	}
	appended, err := managed.journal.Append(event)
	if err != nil {
		return Event{}, err
	}
	managed.record.Cursor = managed.journal.Cursor()
	return appended, nil
}

func (s *Supervisor) saveLocked(managed *managedSession) error {
	managed.record.Cursor = managed.journal.Cursor()
	return s.store.Save(managed.record)
}

func (s *Supervisor) getManaged(id string) (*managedSession, error) {
	s.mu.RLock()
	managed, ok := s.sessions[id]
	s.mu.RUnlock()
	if !ok {
		return nil, ErrSessionNotFound
	}
	return managed, nil
}

func (s *Supervisor) snapshot(managed *managedSession) Session {
	managed.mu.Lock()
	defer managed.mu.Unlock()
	managed.record.Cursor = managed.journal.Cursor()
	value := cloneSession(managed.record)
	value.InputReceipts = nil
	value.CreationKey = ""
	return value
}

func (s *Supervisor) cleanupLoop() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case now := <-ticker.C:
			s.pruneJournals(now)
			s.expireSessions(now)
		}
	}
}

func (s *Supervisor) pruneJournals(now time.Time) {
	s.mu.RLock()
	sessions := make([]*managedSession, 0, len(s.sessions))
	for _, managed := range s.sessions {
		sessions = append(sessions, managed)
	}
	s.mu.RUnlock()
	for _, managed := range sessions {
		if err := managed.journal.Prune(now); err != nil {
			s.logger.Warn("Failed to prune session journal", zap.String("session_id", managed.record.ID), zap.Error(err))
		}
	}
}

func (s *Supervisor) expireSessions(now time.Time) {
	s.mu.RLock()
	sessions := make([]*managedSession, 0, len(s.sessions))
	for _, managed := range s.sessions {
		sessions = append(sessions, managed)
	}
	s.mu.RUnlock()
	for _, managed := range sessions {
		managed.mu.Lock()
		lifecycle := managed.record.Spec.Lifecycle
		if lifecycle.DesiredState == DesiredStateStopped {
			managed.mu.Unlock()
			continue
		}
		expired := lifecycle.MaxLifetimeSeconds > 0 && now.Sub(managed.record.CreatedAt) >= time.Duration(lifecycle.MaxLifetimeSeconds)*time.Second
		if !expired && lifecycle.IdleTimeoutSeconds > 0 {
			expired = now.Sub(managed.record.LastActivityAt) >= time.Duration(lifecycle.IdleTimeoutSeconds)*time.Second
		}
		if expired {
			managed.record.Spec.Lifecycle.DesiredState = DesiredStateStopped
			_, _ = s.appendEventLocked(managed, Event{Type: "session.expired"})
			_ = s.saveLocked(managed)
		}
		managed.mu.Unlock()
		if expired {
			if err := s.stopAttempt(managed, "session_expired"); err != nil {
				s.logger.Warn("Failed to stop expired session", zap.String("session_id", managed.record.ID), zap.Error(err))
			}
		}
	}
}

func executionSpec(spec SessionSpec) any {
	return struct {
		Command   []string
		CWD       string
		Env       map[string]string
		IO        IOSpec
		Readiness ReadinessSpec
	}{spec.Command, spec.CWD, spec.Env, spec.IO, spec.Readiness}
}

func inputDigest(attemptID string, data []byte, eof bool) string {
	hash := sha256.New()
	hash.Write([]byte(attemptID))
	hash.Write([]byte{0})
	hash.Write(data)
	if eof {
		hash.Write([]byte{1})
	} else {
		hash.Write([]byte{0})
	}
	return hex.EncodeToString(hash.Sum(nil))
}
