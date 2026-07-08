package process

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/creack/pty"
	"github.com/gorilla/websocket"
)

const (
	defaultProcessInputBuffer = 128
	defaultHTTPTimeout        = 30 * time.Second
	stopGracePeriod           = 5 * time.Second
)

// Session owns a running process, brokered channels, and replayable events.
type Session struct {
	id        string
	spec      ProcessSpec
	state     ProcessSessionState
	pid       int
	createdAt time.Time
	startedAt *time.Time
	exitedAt  *time.Time
	exitCode  *int

	ctx    context.Context
	cancel context.CancelFunc
	cmd    *exec.Cmd

	stdio *stdioSessionChannel
	pty   *ptySessionChannel
	http  map[string]*httpSessionChannel
	ws    map[string]*websocketSessionChannel

	eventLog *EventLog

	inputMu           sync.Mutex
	inputFingerprints map[string]string
	inputResults      map[string]ProcessEvent

	mu            sync.RWMutex
	waitDone      chan struct{}
	stopRequested bool
	lastActive    time.Time
}

// NewSession validates and creates a process session.
func NewSession(id string, spec ProcessSpec) (*Session, error) {
	if strings.TrimSpace(id) == "" {
		return nil, fmt.Errorf("%w: id is required", ErrInvalidProcessSpec)
	}
	normalized, err := normalizeProcessSpec(spec)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	now := time.Now().UTC()
	return &Session{
		id:                id,
		spec:              normalized,
		state:             ProcessSessionStateCreated,
		createdAt:         now,
		ctx:               ctx,
		cancel:            cancel,
		http:              make(map[string]*httpSessionChannel),
		ws:                make(map[string]*websocketSessionChannel),
		eventLog:          NewEventLog(normalized.EventBufferSize),
		inputFingerprints: make(map[string]string),
		inputResults:      make(map[string]ProcessEvent),
		waitDone:          make(chan struct{}),
		lastActive:        now,
	}, nil
}

func normalizeProcessSpec(spec ProcessSpec) (ProcessSpec, error) {
	if len(spec.Command) == 0 || strings.TrimSpace(spec.Command[0]) == "" {
		return spec, fmt.Errorf("%w: command is required", ErrInvalidProcessSpec)
	}
	if len(spec.Channels) == 0 {
		return spec, fmt.Errorf("%w: at least one channel is required", ErrInvalidProcessSpec)
	}
	if spec.Restart.Policy == "" {
		spec.Restart.Policy = "never"
	}
	if spec.Restart.Policy != "never" {
		return spec, fmt.Errorf("%w: restart policy %q is not supported", ErrInvalidProcessSpec, spec.Restart.Policy)
	}
	if spec.InputBufferSize <= 0 {
		spec.InputBufferSize = defaultProcessInputBuffer
	}

	seen := map[string]struct{}{}
	stdioCount := 0
	ptyCount := 0
	for i := range spec.Channels {
		ch := &spec.Channels[i]
		ch.Name = strings.TrimSpace(ch.Name)
		if ch.Name == "" {
			return spec, fmt.Errorf("%w: channel name is required", ErrInvalidProcessSpec)
		}
		if _, ok := seen[ch.Name]; ok {
			return spec, fmt.Errorf("%w: duplicate channel %q", ErrInvalidProcessSpec, ch.Name)
		}
		seen[ch.Name] = struct{}{}
		if ch.Framing == "" {
			ch.Framing = ChannelFramingRaw
		}
		switch ch.Kind {
		case ChannelKindStdio:
			stdioCount++
			if !ch.Stdin && !ch.Stdout && !ch.Stderr {
				ch.Stdin = true
				ch.Stdout = true
				ch.Stderr = true
			}
		case ChannelKindPTY:
			ptyCount++
		case ChannelKindHTTP:
			if ch.HTTP == nil || strings.TrimSpace(ch.HTTP.BaseURL) == "" {
				return spec, fmt.Errorf("%w: http channel %q requires base_url", ErrInvalidProcessSpec, ch.Name)
			}
			if _, err := url.Parse(ch.HTTP.BaseURL); err != nil {
				return spec, fmt.Errorf("%w: invalid http base_url for channel %q", ErrInvalidProcessSpec, ch.Name)
			}
		case ChannelKindWebSocket:
			if ch.WebSocket == nil || strings.TrimSpace(ch.WebSocket.URL) == "" {
				return spec, fmt.Errorf("%w: websocket channel %q requires url", ErrInvalidProcessSpec, ch.Name)
			}
			if _, err := url.Parse(ch.WebSocket.URL); err != nil {
				return spec, fmt.Errorf("%w: invalid websocket url for channel %q", ErrInvalidProcessSpec, ch.Name)
			}
		default:
			return spec, fmt.Errorf("%w: %s", ErrUnsupportedChannelKind, ch.Kind)
		}
	}
	if stdioCount > 1 {
		return spec, fmt.Errorf("%w: only one stdio channel is supported per process", ErrInvalidProcessSpec)
	}
	if ptyCount > 1 {
		return spec, fmt.Errorf("%w: only one pty channel is supported per process", ErrInvalidProcessSpec)
	}
	if stdioCount > 0 && ptyCount > 0 {
		return spec, fmt.Errorf("%w: stdio and pty channels are mutually exclusive", ErrInvalidProcessSpec)
	}
	return spec, nil
}

// ID returns the session ID.
func (s *Session) ID() string {
	return s.id
}

// Start launches the process and starts configured channels.
func (s *Session) Start() error {
	s.mu.Lock()
	if s.state != ProcessSessionStateCreated {
		s.mu.Unlock()
		return ErrProcessAlreadyRunning
	}
	s.state = ProcessSessionStateStarting
	s.mu.Unlock()

	cmd := s.prepareCommand()
	var err error
	if ptySpec, ok := s.findChannel(ChannelKindPTY); ok {
		err = s.startPTY(cmd, ptySpec)
	} else {
		err = s.startPiped(cmd)
	}
	if err != nil {
		s.setTerminalState(ProcessSessionStateCrashed, 1)
		return err
	}
	s.cmd = cmd
	now := time.Now().UTC()
	s.mu.Lock()
	s.pid = cmd.Process.Pid
	s.startedAt = &now
	s.state = ProcessSessionStateRunning
	s.lastActive = now
	s.mu.Unlock()

	for _, ch := range s.spec.Channels {
		switch ch.Kind {
		case ChannelKindHTTP:
			s.http[ch.Name] = newHTTPSessionChannel(ch, s.eventLog, s.id)
		case ChannelKindWebSocket:
			s.ws[ch.Name] = newWebSocketSessionChannel(ch, s.eventLog, s.id)
		}
	}

	s.eventLog.Publish(ProcessEvent{
		ProcessID: s.id,
		Type:      EventTypeProcessStarted,
		Payload: map[string]any{
			"pid":     s.pid,
			"command": append([]string(nil), s.spec.Command...),
			"cwd":     s.spec.CWD,
			"alias":   s.spec.Alias,
		},
	})

	go s.monitorProcess()
	return nil
}

func (s *Session) prepareCommand() *exec.Cmd {
	cmdPath := s.spec.Command[0]
	args := []string{}
	if len(s.spec.Command) > 1 {
		args = s.spec.Command[1:]
	}
	cmd := exec.CommandContext(s.ctx, cmdPath, args...)
	if s.spec.CWD != "" {
		cmd.Dir = s.spec.CWD
	}
	cmd.Env = MergeEnvironment(os.Environ(), s.spec.EnvVars)
	return cmd
}

func (s *Session) startPiped(cmd *exec.Cmd) error {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	stdioSpec, hasStdio := s.findChannel(ChannelKindStdio)
	if !hasStdio {
		cmd.Stdout = io.Discard
		cmd.Stderr = io.Discard
		if err := cmd.Start(); err != nil {
			return fmt.Errorf("%w: %v", ErrProcessStartFailed, err)
		}
		return nil
	}

	stdio := newStdioSessionChannel(stdioSpec, s.spec.InputBufferSize, s.eventLog, s.id)
	if stdioSpec.Stdin {
		stdin, err := cmd.StdinPipe()
		if err != nil {
			return err
		}
		stdio.stdin = stdin
	}
	var stdout io.ReadCloser
	if stdioSpec.Stdout {
		var err error
		stdout, err = cmd.StdoutPipe()
		if err != nil {
			return err
		}
	} else {
		cmd.Stdout = io.Discard
	}
	var stderr io.ReadCloser
	if stdioSpec.Stderr {
		var err error
		stderr, err = cmd.StderrPipe()
		if err != nil {
			return err
		}
	} else {
		cmd.Stderr = io.Discard
	}

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("%w: %v", ErrProcessStartFailed, err)
	}
	s.stdio = stdio
	stdio.start()
	if stdout != nil {
		stdio.wg.Add(1)
		go stdio.readStream(stdout, OutputSourceStdout)
	}
	if stderr != nil {
		stdio.wg.Add(1)
		go stdio.readStream(stderr, OutputSourceStderr)
	}
	return nil
}

func (s *Session) startPTY(cmd *exec.Cmd, spec ChannelSpec) error {
	size := spec.PTYSize
	if size == nil {
		size = &PTYSize{Rows: 100, Cols: 500}
	}
	ptmx, err := pty.StartWithSize(cmd, &pty.Winsize{Rows: size.Rows, Cols: size.Cols})
	if err != nil {
		return fmt.Errorf("%w: %v", ErrProcessStartFailed, err)
	}
	ptyCh := newPTYSessionChannel(spec, s.spec.InputBufferSize, ptmx, s.eventLog, s.id)
	s.pty = ptyCh
	ptyCh.start()
	return nil
}

func (s *Session) monitorProcess() {
	err := s.cmd.Wait()
	exitCode := 0
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			exitCode = exitErr.ExitCode()
		} else if s.ctx.Err() == context.Canceled {
			exitCode = 137
		} else {
			exitCode = 1
		}
	}

	s.mu.RLock()
	stopRequested := s.stopRequested
	s.mu.RUnlock()

	state := ProcessSessionStateStopped
	eventType := EventTypeProcessExited
	if exitCode != 0 {
		if stopRequested || s.ctx.Err() == context.Canceled {
			state = ProcessSessionStateKilled
			eventType = EventTypeProcessStopped
		} else {
			state = ProcessSessionStateCrashed
			eventType = EventTypeProcessCrashed
		}
	}
	s.setTerminalState(state, exitCode)
	if s.stdio != nil {
		s.stdio.wait()
	}
	s.stopChannels()
	s.eventLog.Publish(ProcessEvent{
		ProcessID: s.id,
		Type:      eventType,
		Payload: map[string]any{
			"exit_code": exitCode,
			"state":     string(state),
		},
	})
	s.eventLog.Close()
	close(s.waitDone)
}

func (s *Session) stopChannels() {
	if s.stdio != nil {
		s.stdio.stop()
	}
	if s.pty != nil {
		s.pty.stop()
	}
	for _, ch := range s.ws {
		ch.stop()
	}
}

func (s *Session) findChannel(kind ChannelKind) (ChannelSpec, bool) {
	for _, ch := range s.spec.Channels {
		if ch.Kind == kind {
			return ch, true
		}
	}
	return ChannelSpec{}, false
}

func (s *Session) setTerminalState(state ProcessSessionState, exitCode int) {
	now := time.Now().UTC()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state = state
	s.exitedAt = &now
	s.exitCode = &exitCode
	s.lastActive = now
}

// Stop terminates the process session.
func (s *Session) Stop() error {
	s.mu.Lock()
	if s.state == ProcessSessionStateStopped || s.state == ProcessSessionStateKilled || s.state == ProcessSessionStateCrashed {
		s.mu.Unlock()
		return nil
	}
	s.state = ProcessSessionStateStopping
	s.stopRequested = true
	pid := s.pid
	s.mu.Unlock()

	if pid > 0 {
		if err := syscall.Kill(-pid, syscall.SIGTERM); err != nil {
			_ = syscall.Kill(pid, syscall.SIGTERM)
		}
	}

	select {
	case <-s.waitDone:
	case <-time.After(stopGracePeriod):
		if pid > 0 {
			if err := syscall.Kill(-pid, syscall.SIGKILL); err != nil {
				_ = syscall.Kill(pid, syscall.SIGKILL)
			}
		}
		s.cancel()
		<-s.waitDone
	}
	return nil
}

// Snapshot returns the current process-session state.
func (s *Session) Snapshot() ProcessSessionSnapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return ProcessSessionSnapshot{
		ID:        s.id,
		Alias:     s.spec.Alias,
		Command:   append([]string(nil), s.spec.Command...),
		CWD:       s.spec.CWD,
		EnvVars:   CloneEnvVars(s.spec.EnvVars),
		State:     s.state,
		PID:       s.pid,
		CreatedAt: s.createdAt,
		StartedAt: cloneTimePtr(s.startedAt),
		ExitedAt:  cloneTimePtr(s.exitedAt),
		ExitCode:  cloneIntPtr(s.exitCode),
		Channels:  append([]ChannelSpec(nil), s.spec.Channels...),
		EventLog:  s.eventLog.Snapshot(),
		Cleanup:   s.spec.Cleanup,
		Restart:   s.spec.Restart,
	}
}

func cloneTimePtr(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	copied := *value
	return &copied
}

func cloneIntPtr(value *int) *int {
	if value == nil {
		return nil
	}
	copied := *value
	return &copied
}

// HandleInput accepts a client input event and routes it to a channel.
func (s *Session) HandleInput(event ProcessInputEvent) (ProcessEvent, error) {
	if strings.TrimSpace(event.EventID) == "" {
		return ProcessEvent{}, fmt.Errorf("%w: event_id is required", ErrInvalidProcessEvent)
	}
	fingerprint := fingerprintInputEvent(event)
	s.inputMu.Lock()
	if existingFingerprint, ok := s.inputFingerprints[event.EventID]; ok {
		result, hasResult := s.inputResults[event.EventID]
		s.inputMu.Unlock()
		if existingFingerprint != fingerprint {
			return ProcessEvent{}, ErrDuplicateEventID
		}
		if hasResult {
			return result, nil
		}
		return ProcessEvent{}, ErrDuplicateEventID
	}
	s.inputFingerprints[event.EventID] = fingerprint
	s.inputMu.Unlock()

	result, err := s.routeInput(event)
	if err != nil {
		s.inputMu.Lock()
		delete(s.inputFingerprints, event.EventID)
		s.inputMu.Unlock()
		return ProcessEvent{}, err
	}

	s.inputMu.Lock()
	s.inputResults[event.EventID] = result
	s.inputMu.Unlock()
	s.Touch()
	return result, nil
}

func fingerprintInputEvent(event ProcessInputEvent) string {
	body, _ := json.Marshal(event)
	return string(body)
}

func (s *Session) routeInput(event ProcessInputEvent) (ProcessEvent, error) {
	switch event.Type {
	case EventTypeStdinWrite:
		if s.stdio == nil {
			return ProcessEvent{}, ErrUnsupportedChannelEvent
		}
		if event.Channel != s.stdio.name {
			return ProcessEvent{}, ErrUnsupportedChannelEvent
		}
		data := stringPayload(event.Payload, "data")
		if err := s.stdio.write([]byte(data)); err != nil {
			return ProcessEvent{}, err
		}
		return s.eventLog.Publish(ProcessEvent{
			EventID:   event.EventID,
			ProcessID: s.id,
			Channel:   s.stdio.name,
			Type:      EventTypeStdinWrite,
			Payload:   map[string]any{"data": data},
		}), nil
	case EventTypePTYInput:
		if s.pty == nil {
			return ProcessEvent{}, ErrUnsupportedChannelEvent
		}
		if event.Channel != s.pty.name {
			return ProcessEvent{}, ErrUnsupportedChannelEvent
		}
		data := stringPayload(event.Payload, "data")
		if err := s.pty.write([]byte(data)); err != nil {
			return ProcessEvent{}, err
		}
		return s.eventLog.Publish(ProcessEvent{
			EventID:   event.EventID,
			ProcessID: s.id,
			Channel:   s.pty.name,
			Type:      EventTypePTYInput,
			Payload:   map[string]any{"data": data},
		}), nil
	case EventTypeHTTPRequest:
		ch, ok := s.http[event.Channel]
		if !ok {
			return ProcessEvent{}, ErrUnsupportedChannelEvent
		}
		accepted := s.eventLog.Publish(ProcessEvent{
			EventID:   event.EventID,
			ProcessID: s.id,
			Channel:   ch.name,
			Type:      EventTypeHTTPRequest,
			Payload:   clonePayload(event.Payload),
		})
		go ch.doRequest(event)
		return accepted, nil
	case EventTypeWebSocketMsg:
		ch, ok := s.ws[event.Channel]
		if !ok {
			return ProcessEvent{}, ErrUnsupportedChannelEvent
		}
		if err := ch.writeMessage(event); err != nil {
			return ProcessEvent{}, err
		}
		return s.eventLog.Publish(ProcessEvent{
			EventID:   event.EventID,
			ProcessID: s.id,
			Channel:   ch.name,
			Type:      EventTypeWebSocketMsg,
			Payload:   clonePayload(event.Payload),
		}), nil
	default:
		return ProcessEvent{}, ErrUnsupportedChannelEvent
	}
}

// Subscribe subscribes to replay and live process events.
func (s *Session) Subscribe(cursor int64) (<-chan ProcessEvent, func()) {
	s.Touch()
	return s.eventLog.Subscribe(cursor)
}

// SendSignal sends a signal to the process group, falling back to the PID.
func (s *Session) SendSignal(sig syscall.Signal) error {
	s.mu.RLock()
	pid := s.pid
	state := s.state
	s.mu.RUnlock()
	if pid <= 0 || (state != ProcessSessionStateRunning && state != ProcessSessionStatePaused) {
		return ErrProcessNotRunning
	}
	if err := syscall.Kill(-pid, sig); err != nil {
		if err := syscall.Kill(pid, sig); err != nil {
			return fmt.Errorf("%w: %v", ErrSignalFailed, err)
		}
	}
	s.Touch()
	return nil
}

// ResizePTY resizes a PTY channel.
func (s *Session) ResizePTY(channel string, size PTYSize) error {
	if s.pty == nil || s.pty.name != channel {
		return ErrPTYNotAvailable
	}
	return s.pty.resize(size)
}

// Touch records session activity.
func (s *Session) Touch() {
	s.mu.Lock()
	s.lastActive = time.Now().UTC()
	s.mu.Unlock()
}

func (s *Session) shouldCleanup(now time.Time) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.spec.Cleanup.TTLSec > 0 && now.Sub(s.createdAt) >= time.Duration(s.spec.Cleanup.TTLSec)*time.Second {
		return true
	}
	if s.spec.Cleanup.IdleTimeoutSec > 0 && now.Sub(s.lastActive) >= time.Duration(s.spec.Cleanup.IdleTimeoutSec)*time.Second {
		return true
	}
	return false
}

type stdioSessionChannel struct {
	name      string
	framing   ChannelFraming
	stdin     io.WriteCloser
	inputCh   chan []byte
	stopCh    chan struct{}
	stopOnce  sync.Once
	wg        sync.WaitGroup
	eventLog  *EventLog
	processID string
}

func newStdioSessionChannel(spec ChannelSpec, inputBuffer int, log *EventLog, processID string) *stdioSessionChannel {
	return &stdioSessionChannel{
		name:      spec.Name,
		framing:   spec.Framing,
		inputCh:   make(chan []byte, inputBuffer),
		stopCh:    make(chan struct{}),
		eventLog:  log,
		processID: processID,
	}
}

func (c *stdioSessionChannel) start() {
	if c.stdin != nil {
		go c.writeLoop()
	}
}

func (c *stdioSessionChannel) write(data []byte) error {
	if c.stdin == nil {
		return ErrUnsupportedChannelEvent
	}
	payload := append([]byte(nil), data...)
	select {
	case c.inputCh <- payload:
		return nil
	default:
		return ErrInputBufferFull
	}
}

func (c *stdioSessionChannel) writeLoop() {
	for {
		select {
		case <-c.stopCh:
			return
		case data := <-c.inputCh:
			if len(data) == 0 {
				continue
			}
			if _, err := c.stdin.Write(data); err != nil {
				c.eventLog.Publish(ProcessEvent{
					ProcessID: c.processID,
					Channel:   c.name,
					Type:      EventTypeError,
					Payload:   map[string]any{"message": err.Error(), "source": "stdin"},
				})
			}
		}
	}
}

func (c *stdioSessionChannel) readStream(reader io.ReadCloser, source OutputSource) {
	defer c.wg.Done()
	defer reader.Close()
	if c.framing == ChannelFramingRaw {
		c.readRaw(reader, source)
		return
	}
	c.readLines(reader, source)
}

func (c *stdioSessionChannel) readRaw(reader io.Reader, source OutputSource) {
	buf := make([]byte, 4096)
	for {
		n, err := reader.Read(buf)
		if n > 0 {
			eventType := EventTypeStdoutChunk
			if source == OutputSourceStderr {
				eventType = EventTypeStderrChunk
			}
			c.eventLog.Publish(ProcessEvent{
				ProcessID: c.processID,
				Channel:   c.name,
				Type:      eventType,
				Payload: map[string]any{
					"data": string(buf[:n]),
				},
			})
		}
		if err != nil {
			return
		}
	}
}

func (c *stdioSessionChannel) readLines(reader io.Reader, source OutputSource) {
	buf := bufio.NewReader(reader)
	for {
		line, err := buf.ReadBytes('\n')
		if len(line) > 0 {
			line = bytes.TrimSuffix(line, []byte("\n"))
			line = bytes.TrimSuffix(line, []byte("\r"))
			eventType := EventTypeStdoutLine
			if source == OutputSourceStderr {
				eventType = EventTypeStderrLine
			}
			if (c.framing == ChannelFramingJSONL || c.framing == ChannelFramingJSONRPC) && len(bytes.TrimSpace(line)) > 0 && !json.Valid(line) {
				c.eventLog.Publish(ProcessEvent{
					ProcessID: c.processID,
					Channel:   c.name,
					Type:      EventTypeError,
					Payload: map[string]any{
						"message": "invalid json line",
						"source":  string(source),
						"data":    string(line),
					},
				})
			}
			c.eventLog.Publish(ProcessEvent{
				ProcessID: c.processID,
				Channel:   c.name,
				Type:      eventType,
				Payload: map[string]any{
					"data": string(line),
				},
			})
		}
		if err != nil {
			return
		}
	}
}

func (c *stdioSessionChannel) stop() {
	c.stopOnce.Do(func() {
		close(c.stopCh)
		if c.stdin != nil {
			_ = c.stdin.Close()
		}
	})
}

func (c *stdioSessionChannel) wait() {
	c.wg.Wait()
}

type ptySessionChannel struct {
	name      string
	ptmx      *os.File
	inputCh   chan []byte
	stopCh    chan struct{}
	stopOnce  sync.Once
	eventLog  *EventLog
	processID string
}

func newPTYSessionChannel(spec ChannelSpec, inputBuffer int, ptmx *os.File, log *EventLog, processID string) *ptySessionChannel {
	return &ptySessionChannel{
		name:      spec.Name,
		ptmx:      ptmx,
		inputCh:   make(chan []byte, inputBuffer),
		stopCh:    make(chan struct{}),
		eventLog:  log,
		processID: processID,
	}
}

func (c *ptySessionChannel) start() {
	go c.writeLoop()
	go c.readLoop()
}

func (c *ptySessionChannel) write(data []byte) error {
	payload := append([]byte(nil), data...)
	select {
	case c.inputCh <- payload:
		return nil
	default:
		return ErrInputBufferFull
	}
}

func (c *ptySessionChannel) writeLoop() {
	for {
		select {
		case <-c.stopCh:
			return
		case data := <-c.inputCh:
			if len(data) == 0 {
				continue
			}
			_, _ = c.ptmx.Write(data)
		}
	}
}

func (c *ptySessionChannel) readLoop() {
	buf := make([]byte, 4096)
	for {
		n, err := c.ptmx.Read(buf)
		if n > 0 {
			c.eventLog.Publish(ProcessEvent{
				ProcessID: c.processID,
				Channel:   c.name,
				Type:      EventTypePTYOutput,
				Payload: map[string]any{
					"data": string(buf[:n]),
				},
			})
		}
		if err != nil {
			return
		}
	}
}

func (c *ptySessionChannel) resize(size PTYSize) error {
	if size.Rows == 0 || size.Cols == 0 {
		return fmt.Errorf("%w: rows and cols must be > 0", ErrInvalidPTYSize)
	}
	return pty.Setsize(c.ptmx, &pty.Winsize{Rows: size.Rows, Cols: size.Cols})
}

func (c *ptySessionChannel) stop() {
	c.stopOnce.Do(func() {
		close(c.stopCh)
		_ = c.ptmx.Close()
	})
}

type httpSessionChannel struct {
	name      string
	spec      HTTPChannelSpec
	client    *http.Client
	eventLog  *EventLog
	processID string
}

func newHTTPSessionChannel(spec ChannelSpec, log *EventLog, processID string) *httpSessionChannel {
	timeout := defaultHTTPTimeout
	if spec.HTTP.Timeout > 0 {
		timeout = time.Duration(spec.HTTP.Timeout) * time.Second
	}
	return &httpSessionChannel{
		name:      spec.Name,
		spec:      *spec.HTTP,
		client:    &http.Client{Timeout: timeout},
		eventLog:  log,
		processID: processID,
	}
}

func (c *httpSessionChannel) doRequest(event ProcessInputEvent) {
	method := strings.ToUpper(stringPayload(event.Payload, "method"))
	if method == "" {
		method = http.MethodGet
	}
	target := joinURL(c.spec.BaseURL, stringPayload(event.Payload, "path"))
	body := strings.NewReader(stringPayload(event.Payload, "body"))
	req, err := http.NewRequest(method, target, body)
	if err != nil {
		c.publishError(err)
		return
	}
	for k, v := range c.spec.Headers {
		req.Header.Set(k, v)
	}
	for k, v := range stringMapPayload(event.Payload, "headers") {
		req.Header.Set(k, v)
	}
	resp, err := c.client.Do(req)
	if err != nil {
		c.publishError(err)
		return
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	c.eventLog.Publish(ProcessEvent{
		EventID:   event.EventID,
		ProcessID: c.processID,
		Channel:   c.name,
		Type:      EventTypeHTTPResponse,
		Payload: map[string]any{
			"status_code": resp.StatusCode,
			"body":        string(respBody),
		},
	})
}

func (c *httpSessionChannel) publishError(err error) {
	c.eventLog.Publish(ProcessEvent{
		ProcessID: c.processID,
		Channel:   c.name,
		Type:      EventTypeError,
		Payload:   map[string]any{"message": err.Error()},
	})
}

type websocketSessionChannel struct {
	name      string
	spec      WebSocketChannelSpec
	eventLog  *EventLog
	processID string

	mu     sync.Mutex
	conn   *websocket.Conn
	closed bool
}

func newWebSocketSessionChannel(spec ChannelSpec, log *EventLog, processID string) *websocketSessionChannel {
	return &websocketSessionChannel{
		name:      spec.Name,
		spec:      *spec.WebSocket,
		eventLog:  log,
		processID: processID,
	}
}

func (c *websocketSessionChannel) writeMessage(event ProcessInputEvent) error {
	conn, err := c.ensureConnected()
	if err != nil {
		return err
	}
	return conn.WriteMessage(websocket.TextMessage, []byte(stringPayload(event.Payload, "data")))
}

func (c *websocketSessionChannel) ensureConnected() (*websocket.Conn, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, ErrProcessFinished
	}
	if c.conn != nil {
		return c.conn, nil
	}
	header := http.Header{}
	for k, v := range c.spec.Headers {
		header.Set(k, v)
	}
	conn, _, err := websocket.DefaultDialer.Dial(c.spec.URL, header)
	if err != nil {
		return nil, err
	}
	c.conn = conn
	c.eventLog.Publish(ProcessEvent{
		ProcessID: c.processID,
		Channel:   c.name,
		Type:      EventTypeWebSocketOpen,
	})
	go c.readLoop(conn)
	return conn, nil
}

func (c *websocketSessionChannel) readLoop(conn *websocket.Conn) {
	for {
		_, data, err := conn.ReadMessage()
		if err != nil {
			c.eventLog.Publish(ProcessEvent{
				ProcessID: c.processID,
				Channel:   c.name,
				Type:      EventTypeWebSocketClose,
				Payload:   map[string]any{"message": err.Error()},
			})
			return
		}
		c.eventLog.Publish(ProcessEvent{
			ProcessID: c.processID,
			Channel:   c.name,
			Type:      EventTypeWebSocketMsg,
			Payload:   map[string]any{"data": string(data)},
		})
	}
}

func (c *websocketSessionChannel) stop() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	if c.conn != nil {
		_ = c.conn.Close()
		c.conn = nil
	}
}

func stringPayload(payload map[string]any, key string) string {
	if payload == nil {
		return ""
	}
	value, ok := payload[key]
	if !ok || value == nil {
		return ""
	}
	switch typed := value.(type) {
	case string:
		return typed
	default:
		return fmt.Sprint(typed)
	}
}

func stringMapPayload(payload map[string]any, key string) map[string]string {
	result := map[string]string{}
	if payload == nil {
		return result
	}
	value, ok := payload[key]
	if !ok || value == nil {
		return result
	}
	body, err := json.Marshal(value)
	if err != nil {
		return result
	}
	_ = json.Unmarshal(body, &result)
	return result
}

func clonePayload(payload map[string]any) map[string]any {
	if payload == nil {
		return nil
	}
	cloned := make(map[string]any, len(payload))
	for k, v := range payload {
		cloned[k] = v
	}
	return cloned
}

func joinURL(base, path string) string {
	if path == "" {
		return base
	}
	base = strings.TrimRight(base, "/")
	path = "/" + strings.TrimLeft(path, "/")
	return base + path
}
