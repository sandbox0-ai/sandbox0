package repl

import (
	"bytes"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/sandbox0-ai/infra/manager/procd/pkg/process"
)

// REPL implements a configurable REPL process.
type REPL struct {
	*process.BaseProcess
	runner *process.PTYRunner
	config *REPLConfig

	// Output processing state
	mu            sync.Mutex
	lastInput     string
	promptRegexps []*regexp.Regexp
}

// New creates a new REPL process with the given configuration.
func New(id string, replConfig *REPLConfig, processConfig process.ProcessConfig) (*REPL, error) {
	if replConfig == nil {
		return nil, fmt.Errorf("REPL config is required")
	}
	if err := replConfig.Validate(); err != nil {
		return nil, fmt.Errorf("invalid REPL config: %w", err)
	}

	bp := process.NewBaseProcess(id, process.ProcessTypeREPL, processConfig)

	repl := &REPL{
		BaseProcess: bp,
		config:      replConfig.Clone(),
	}

	// Compile prompt patterns
	repl.promptRegexps = make([]*regexp.Regexp, 0, len(replConfig.Prompt.Patterns))
	for _, pattern := range replConfig.Prompt.Patterns {
		if re, err := regexp.Compile(pattern); err == nil {
			repl.promptRegexps = append(repl.promptRegexps, re)
		}
	}

	// Set up output filter if needed
	var outputFilter func([]byte) []byte
	if replConfig.Output.FilterEcho || replConfig.Output.TrimPrompt || replConfig.Output.StripANSI {
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

	return startWithCandidates(r.BaseProcess, r.runner, processConfig, candidates, extraEnv)
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

// WriteInput writes data to the REPL and tracks it for echo filtering.
func (r *REPL) WriteInput(data []byte) error {
	// Track input for echo filtering
	if r.config.Output.FilterEcho {
		input := string(data)
		// Remove trailing newline for matching
		input = strings.TrimSuffix(input, "\n")
		input = strings.TrimSuffix(input, "\r")
		r.SetLastInput(input)
	}
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

// SetLastInput sets the last input for echo filtering.
func (r *REPL) SetLastInput(input string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.lastInput = input
}

// filterOutput processes output according to config.
func (r *REPL) filterOutput(data []byte) []byte {
	if len(data) == 0 {
		return data
	}

	result := data

	// Strip ANSI escape sequences
	if r.config.Output.StripANSI {
		result = stripANSI(result)
	}

	// Normalize line endings: convert \r\n to \n, and standalone \r to \n
	result = bytes.ReplaceAll(result, []byte("\r\n"), []byte("\n"))
	result = bytes.ReplaceAll(result, []byte("\r"), []byte("\n"))

	// Filter echoed input
	if r.config.Output.FilterEcho {
		r.mu.Lock()
		lastInput := r.lastInput
		r.mu.Unlock()

		if lastInput != "" && bytes.Contains(result, []byte(lastInput)) {
			// Remove the echoed command line (command + newline)
			result = bytes.Replace(result, []byte(lastInput+"\n"), []byte{}, 1)
		}
	}

	// Trim prompt patterns
	if r.config.Output.TrimPrompt {
		for _, re := range r.promptRegexps {
			result = re.ReplaceAll(result, []byte{})
		}
	}

	// Clean up multiple consecutive newlines
	for bytes.Contains(result, []byte("\n\n\n")) {
		result = bytes.ReplaceAll(result, []byte("\n\n\n"), []byte("\n\n"))
	}

	// Trim leading/trailing whitespace
	result = bytes.TrimSpace(result)

	return result
}

// DetectPrompt checks if the output contains a prompt.
func (r *REPL) DetectPrompt(data []byte) bool {
	str := string(data)
	for _, re := range r.promptRegexps {
		if re.MatchString(str) {
			return true
		}
	}
	return false
}

// ansiRegex matches ANSI escape sequences.
var ansiRegex = regexp.MustCompile(`\x1b\[[0-9;]*[a-zA-Z]`)

// stripANSI removes ANSI escape sequences from data.
func stripANSI(data []byte) []byte {
	return ansiRegex.ReplaceAll(data, []byte{})
}

// Language returns the REPL language/name.
func (r *REPL) Language() string {
	return r.config.Name
}

// ExitCommand returns the command to gracefully exit the REPL.
func (r *REPL) ExitCommand() string {
	return r.config.ExitCommand
}
