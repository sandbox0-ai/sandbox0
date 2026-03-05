package repl

import (
	"fmt"

	"github.com/sandbox0-ai/sandbox0/manager/procd/pkg/process"
)

// NewREPL creates a REPL process based on the alias specified in config.
func NewREPL(id string, config process.ProcessConfig) (*REPL, error) {
	alias := config.Alias
	if alias == "" {
		return nil, fmt.Errorf("alias is required in process config")
	}

	replConfig, ok := DefaultRegistry.Get(alias)
	if !ok {
		return nil, fmt.Errorf("unknown REPL alias: %s (available: %v)", alias, DefaultRegistry.List())
	}

	return New(id, replConfig, config)
}

// NewCustomREPL creates a REPL with a custom configuration.
// Use this for user-defined CLI tools or non-standard REPLs.
func NewCustomREPL(id string, replConfig *REPLConfig, processConfig process.ProcessConfig) (*REPL, error) {
	return New(id, replConfig, processConfig)
}

// CreateREPLConfig creates a minimal REPL config for a custom CLI.
//
// Example:
//
//	config := CreateREPLConfig("myrepl", []ExecCandidate{
//	    {Name: "myrepl", Args: []string{"--interactive"}},
//	})
func CreateREPLConfig(name string, candidates []ExecCandidate) *REPLConfig {
	return &REPLConfig{
		Name:        name,
		DisplayName: name,
		Candidates:  candidates,
		Ready: ReadyConfig{
			Mode: ReadyModeStartupDelay,
		},
	}
}
