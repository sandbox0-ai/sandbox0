package repl

import (
	"fmt"

	"github.com/sandbox0-ai/infra/manager/procd/pkg/process"
)

// NewREPL creates a REPL process based on the language specified in config.
func NewREPL(id string, config process.ProcessConfig) (*REPL, error) {
	language := config.Language
	if language == "" {
		return nil, fmt.Errorf("language is required in process config")
	}

	replConfig, ok := DefaultRegistry.Get(language)
	if !ok {
		return nil, fmt.Errorf("unknown REPL language: %s (available: %v)", language, DefaultRegistry.List())
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
		Prompt: PromptConfig{
			Patterns: []string{`> `, `>>> `},
		},
	}
}
