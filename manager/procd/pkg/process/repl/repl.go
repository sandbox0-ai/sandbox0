package repl

import (
	"bytes"
	"fmt"
	"sync"
	"time"

	"github.com/sandbox0-ai/infra/manager/procd/pkg/process"
)

// REPL implements a configurable REPL process.
type REPL struct {
	*process.BaseProcess
	runner *process.PTYRunner
	config *REPLConfig

	// Ready detection state
	mu          sync.Mutex
	readyOnce   sync.Once
	readyToken  []byte
	readyBuffer []byte
}

var _ process.Process = (*REPL)(nil)

// New creates a new REPL process with the given configuration.
func New(id string, replConfig *REPLConfig, processConfig process.ProcessConfig) (*REPL, error) {
	if replConfig == nil {
		return nil, fmt.Errorf("REPL config is required")
	}
	config := replConfig.Clone()
	config.applyDefaults()
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid REPL config: %w", err)
	}

	bp := process.NewBaseProcess(id, process.ProcessTypeREPL, processConfig)

	repl := &REPL{
		BaseProcess: bp,
		config:      config,
	}

	if config.Ready.Mode == ReadyModePromptToken && config.Ready.Token != "" {
		repl.readyToken = []byte(config.Ready.Token)
		repl.readyBuffer = make([]byte, 0, len(repl.readyToken))
	}

	// Set up output filter if needed for readiness detection
	var outputFilter func([]byte) ([]byte, bool)
	if config.Ready.Mode == ReadyModePromptToken && len(repl.readyToken) > 0 {
		outputFilter = repl.filterOutput
	}

	repl.runner = process.NewPTYRunner(bp, outputFilter, nil)
	return repl, nil
}

// NewFromRegistry creates a REPL using a config from the registry.
func NewFromRegistry(id string, language string, processConfig process.ProcessConfig) (*REPL, error) {
	replConfig, ok := DefaultRegistry.Get(language)
	if !ok {
		return nil, fmt.Errorf("unknown REPL language: %s", language)
	}
	return New(id, replConfig, processConfig)
}

// Start starts the REPL process.
func (r *REPL) Start() error {
	if r.IsRunning() {
		return process.ErrProcessAlreadyRunning
	}

	processConfig := r.GetConfig()

	// Build exec candidates from REPL config
	candidates := make([]execCandidate, len(r.config.Candidates))
	for i, c := range r.config.Candidates {
		candidates[i] = execCandidate{
			name: c.Name,
			args: c.Args,
		}
	}

	// Build environment variables
	extraEnv := r.buildEnvVars(processConfig)
	if err := startWithCandidates(r.BaseProcess, r.runner, processConfig, candidates, extraEnv); err != nil {
		return err
	}
	r.scheduleStartupReady()
	return nil
}

// buildEnvVars builds the environment variables for the REPL.
func (r *REPL) buildEnvVars(processConfig process.ProcessConfig) []string {
	var envVars []string

	// Get TERM value
	term := processConfig.Term
	if term == "" && r.config.DefaultTerm != "" {
		term = r.config.DefaultTerm
	}

	// Get prompt value
	prompt := r.config.Prompt.CustomPrompt

	for _, env := range r.config.Env {
		var value string
		switch env.ValueFrom {
		case "term":
			value = term
		case "prompt":
			value = prompt
		default:
			value = env.Value
		}
		if value != "" {
			envVars = append(envVars, fmt.Sprintf("%s=%s", env.Name, value))
		}
	}

	return envVars
}

// Stop stops the REPL process.
func (r *REPL) Stop() error {
	return r.runner.Stop()
}
func (r *REPL) WriteInput(data []byte) error {
	return r.BaseProcess.WriteInput(data)
}

// Restart restarts the process.
func (r *REPL) Restart() error {
	if err := r.Stop(); err != nil {
		return err
	}
	time.Sleep(100 * time.Millisecond)
	return r.Start()
}

// ResizeTerminal resizes the PTY.
func (r *REPL) ResizeTerminal(size process.PTYSize) error {
	if !r.IsRunning() {
		return process.ErrProcessNotRunning
	}
	return r.BaseProcess.ResizePTY(size)
}

// Config returns the REPL configuration.
func (r *REPL) Config() *REPLConfig {
	return r.config.Clone()
}

// filterOutput detects readiness without altering output.
func (r *REPL) filterOutput(data []byte) ([]byte, bool) {
	if len(data) == 0 {
		return data, false
	}
	if r.detectReadyToken(data) && r.markReady() {
		return data, true
	}
	return data, false
}

// Language returns the REPL language/name.
func (r *REPL) Language() string {
	return r.config.Name
}

// ExitCommand returns the command to gracefully exit the REPL.
func (r *REPL) ExitCommand() string {
	return r.config.ExitCommand
}

func (r *REPL) detectReadyToken(data []byte) bool {
	if len(r.readyToken) == 0 {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	if len(r.readyBuffer) == 0 {
		if bytes.Contains(data, r.readyToken) {
			return true
		}
		r.readyBuffer = append(r.readyBuffer[:0], tailForToken(data, len(r.readyToken))...)
		return false
	}

	combined := make([]byte, 0, len(r.readyBuffer)+len(data))
	combined = append(combined, r.readyBuffer...)
	combined = append(combined, data...)
	if bytes.Contains(combined, r.readyToken) {
		r.readyBuffer = r.readyBuffer[:0]
		return true
	}
	r.readyBuffer = append(r.readyBuffer[:0], tailForToken(combined, len(r.readyToken))...)
	return false
}

func tailForToken(data []byte, tokenLen int) []byte {
	if tokenLen <= 1 || len(data) < tokenLen-1 {
		return data
	}
	return data[len(data)-(tokenLen-1):]
}

func (r *REPL) markReady() bool {
	fired := false
	r.readyOnce.Do(func() {
		fired = true
	})
	return fired
}

func (r *REPL) scheduleStartupReady() {
	if r.config.Ready.Mode != ReadyModeStartupDelay {
		return
	}
	delay := time.Duration(r.config.Ready.StartupDelayMs) * time.Millisecond
	if delay <= 0 {
		delay = time.Duration(DefaultReadyStartupDelayMs) * time.Millisecond
	}
	go func() {
		time.Sleep(delay)
		if !r.IsRunning() {
			return
		}
		if r.markReady() {
			r.BaseProcess.SignalInputReady()
			r.BaseProcess.PublishOutput(process.ProcessOutput{
				Source: process.OutputSourcePrompt,
			})
		}
	}()
}
