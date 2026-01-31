// Package repl provides configurable REPL process implementations.
package repl

import (
	"fmt"
)

// ExecCandidate represents a candidate executable with its arguments.
type ExecCandidate struct {
	// Name is the executable name (searched in PATH).
	Name string `json:"name" yaml:"name"`
	// Args are the command-line arguments.
	Args []string `json:"args" yaml:"args"`
}

// EnvVar represents an environment variable configuration.
type EnvVar struct {
	// Name is the environment variable name.
	Name string `json:"name" yaml:"name"`
	// Value is the literal value (mutually exclusive with ValueFrom).
	Value string `json:"value,omitempty" yaml:"value,omitempty"`
	// ValueFrom references a config field like "prompt" or "term".
	ValueFrom string `json:"value_from,omitempty" yaml:"value_from,omitempty"`
}

// PromptConfig defines how to customize prompts.
type PromptConfig struct {
	// CustomPrompt is the custom prompt to set (if supported by the REPL).
	CustomPrompt string `json:"custom_prompt,omitempty" yaml:"custom_prompt,omitempty"`
}

// ReadyMode defines how initial input readiness is detected.
type ReadyMode string

const (
	ReadyModePromptToken  ReadyMode = "prompt_token"
	ReadyModeStartupDelay ReadyMode = "startup_delay"
)

// ReadyConfig defines how the REPL becomes input-ready.
type ReadyConfig struct {
	// Mode controls the readiness detection strategy.
	Mode ReadyMode `json:"mode,omitempty" yaml:"mode,omitempty"`
	// Token is the prompt token used for ReadyModePromptToken.
	Token string `json:"token,omitempty" yaml:"token,omitempty"`
	// StartupDelayMs is the delay before ready in startup_delay mode.
	StartupDelayMs int `json:"startup_delay_ms,omitempty" yaml:"startup_delay_ms,omitempty"`
}

// REPLConfig defines the configuration for a REPL type.
type REPLConfig struct {
	// Name is the REPL identifier (e.g., "python", "node", "bash").
	Name string `json:"name" yaml:"name"`
	// DisplayName is a human-readable name.
	DisplayName string `json:"display_name,omitempty" yaml:"display_name,omitempty"`
	// Description describes the REPL.
	Description string `json:"description,omitempty" yaml:"description,omitempty"`

	// Candidates are the executable candidates in order of preference.
	Candidates []ExecCandidate `json:"candidates" yaml:"candidates"`

	// Environment variables to set.
	Env []EnvVar `json:"env,omitempty" yaml:"env,omitempty"`

	// DefaultTerm is the default TERM value if not specified.
	DefaultTerm string `json:"default_term,omitempty" yaml:"default_term,omitempty"`

	// Prompt configuration.
	Prompt PromptConfig `json:"prompt,omitempty" yaml:"prompt,omitempty"`

	// Ready configuration.
	Ready ReadyConfig `json:"ready,omitempty" yaml:"ready,omitempty"`
}

// Validate validates the REPL configuration.
func (c *REPLConfig) Validate() error {
	if c.Name == "" {
		return fmt.Errorf("REPL config name is required")
	}
	if len(c.Candidates) == 0 {
		return fmt.Errorf("REPL config must have at least one candidate")
	}
	for i, candidate := range c.Candidates {
		if candidate.Name == "" {
			return fmt.Errorf("candidate %d: name is required", i)
		}
	}
	switch c.Ready.Mode {
	case "", ReadyModePromptToken, ReadyModeStartupDelay:
		// ok
	default:
		return fmt.Errorf("ready mode %q is invalid", c.Ready.Mode)
	}
	if c.Ready.Mode == ReadyModePromptToken && c.Ready.Token == "" {
		return fmt.Errorf("ready token is required for prompt_token mode")
	}
	if c.Ready.Mode == ReadyModeStartupDelay && c.Ready.StartupDelayMs < 0 {
		return fmt.Errorf("startup_delay_ms must be >= 0")
	}
	return nil
}

func (c *REPLConfig) applyDefaults() {
	if c.Ready.Mode == "" {
		if c.Ready.Token != "" {
			c.Ready.Mode = ReadyModePromptToken
		} else {
			c.Ready.Mode = ReadyModeStartupDelay
		}
	}
	if c.Ready.Mode == ReadyModeStartupDelay && c.Ready.StartupDelayMs == 0 {
		c.Ready.StartupDelayMs = DefaultReadyStartupDelayMs
	}
}

// Clone returns a deep copy of the config.
func (c *REPLConfig) Clone() *REPLConfig {
	clone := *c
	clone.Candidates = make([]ExecCandidate, len(c.Candidates))
	for i, cand := range c.Candidates {
		clone.Candidates[i] = ExecCandidate{
			Name: cand.Name,
			Args: append([]string(nil), cand.Args...),
		}
	}
	clone.Env = make([]EnvVar, len(c.Env))
	copy(clone.Env, c.Env)
	return &clone
}

// DefaultReadyToken is the default prompt token for REPLs.
const DefaultReadyToken = "_S0_> "

// DefaultContinuationToken is the default continuation prompt for REPLs.
const DefaultContinuationToken = "__S0_CONT__ "

// DefaultReadyStartupDelayMs is the default startup delay for readiness.
const DefaultReadyStartupDelayMs = 200

// BuiltinConfigs contains built-in REPL configurations.
var BuiltinConfigs = map[string]*REPLConfig{
	"python": {
		Name:        "python",
		DisplayName: "Python",
		Description: "Python interactive interpreter",
		Candidates: []ExecCandidate{
			{Name: "python3", Args: []string{"-q", "-i", "-u", "-c", "import sys; sys.ps1='" + DefaultReadyToken + "'; sys.ps2='" + DefaultContinuationToken + "'"}},
			{Name: "python", Args: []string{"-q", "-i", "-u", "-c", "import sys; sys.ps1='" + DefaultReadyToken + "'; sys.ps2='" + DefaultContinuationToken + "'"}},
		},
		Env: []EnvVar{
			{Name: "PYTHONUNBUFFERED", Value: "1"},
			{Name: "PYTHONDONTWRITEBYTECODE", Value: "1"},
		},
		Ready: ReadyConfig{
			Mode:  ReadyModePromptToken,
			Token: DefaultReadyToken,
		},
	},

	"node": {
		Name:        "node",
		DisplayName: "Node.js",
		Description: "Node.js JavaScript runtime REPL",
		Candidates: []ExecCandidate{
			{Name: "node", Args: []string{"-e", "require('repl').start({prompt: '" + DefaultReadyToken + "'})"}},
			{Name: "nodejs", Args: []string{"-e", "require('repl').start({prompt: '" + DefaultReadyToken + "'})"}},
		},
		Ready: ReadyConfig{
			Mode:  ReadyModePromptToken,
			Token: DefaultReadyToken,
		},
	},

	"bash": {
		Name:        "bash",
		DisplayName: "Bash",
		Description: "Bash shell",
		Candidates: []ExecCandidate{
			{Name: "bash", Args: []string{"--norc", "--noprofile", "-i"}},
			{Name: "sh", Args: []string{"-i"}},
		},
		DefaultTerm: "xterm-256color",
		Env: []EnvVar{
			{Name: "TERM", ValueFrom: "term"},
			{Name: "PS1", ValueFrom: "prompt"},
		},
		Prompt: PromptConfig{
			CustomPrompt: DefaultReadyToken,
		},
		Ready: ReadyConfig{
			Mode:  ReadyModePromptToken,
			Token: DefaultReadyToken,
		},
	},

	"zsh": {
		Name:        "zsh",
		DisplayName: "Zsh",
		Description: "Z shell",
		Candidates: []ExecCandidate{
			{Name: "zsh", Args: []string{"--no-rcs", "-i"}},
			{Name: "bash", Args: []string{"--norc", "--noprofile", "-i"}}, // Fallback
			{Name: "sh", Args: []string{"-i"}},
		},
		DefaultTerm: "xterm-256color",
		Env: []EnvVar{
			{Name: "TERM", ValueFrom: "term"},
			{Name: "PS1", ValueFrom: "prompt"},
		},
		Prompt: PromptConfig{
			CustomPrompt: DefaultReadyToken,
		},
		Ready: ReadyConfig{
			Mode:  ReadyModePromptToken,
			Token: DefaultReadyToken,
		},
	},

	"ruby": {
		Name:        "ruby",
		DisplayName: "Ruby",
		Description: "Ruby IRB (Interactive Ruby)",
		Candidates: []ExecCandidate{
			{Name: "ruby", Args: []string{"-e", "require 'irb'; IRB.conf[:PROMPT][:S0]={PROMPT_I:'" + DefaultReadyToken + "', PROMPT_S:'" + DefaultContinuationToken + "', PROMPT_C:'" + DefaultContinuationToken + "', RETURN:\"%s\\n\"}; IRB.conf[:PROMPT_MODE]=:S0; IRB.start"}},
		},
		Ready: ReadyConfig{
			Mode:  ReadyModePromptToken,
			Token: DefaultReadyToken,
		},
	},

	"lua": {
		Name:        "lua",
		DisplayName: "Lua",
		Description: "Lua interpreter",
		Candidates: []ExecCandidate{
			{Name: "lua", Args: []string{"-i", "-e", "_PROMPT='" + DefaultReadyToken + "'; _PROMPT2='" + DefaultContinuationToken + "'"}},
			{Name: "lua5.4", Args: []string{"-i", "-e", "_PROMPT='" + DefaultReadyToken + "'; _PROMPT2='" + DefaultContinuationToken + "'"}},
			{Name: "lua5.3", Args: []string{"-i", "-e", "_PROMPT='" + DefaultReadyToken + "'; _PROMPT2='" + DefaultContinuationToken + "'"}},
			{Name: "lua5.2", Args: []string{"-i", "-e", "_PROMPT='" + DefaultReadyToken + "'; _PROMPT2='" + DefaultContinuationToken + "'"}},
			{Name: "lua5.1", Args: []string{"-i", "-e", "_PROMPT='" + DefaultReadyToken + "'; _PROMPT2='" + DefaultContinuationToken + "'"}},
			{Name: "luajit", Args: []string{"-i", "-e", "_PROMPT='" + DefaultReadyToken + "'; _PROMPT2='" + DefaultContinuationToken + "'"}},
		},
		Ready: ReadyConfig{
			Mode:  ReadyModePromptToken,
			Token: DefaultReadyToken,
		},
	},

	"php": {
		Name:        "php",
		DisplayName: "PHP",
		Description: "PHP interactive mode",
		Candidates: []ExecCandidate{
			{Name: "psysh", Args: []string{}}, // PsySH is better for REPL
			{Name: "php", Args: []string{"-a"}},
		},
		Ready: ReadyConfig{
			Mode: ReadyModeStartupDelay,
		},
	},

	"r": {
		Name:        "r",
		DisplayName: "R",
		Description: "R statistical computing",
		Candidates: []ExecCandidate{
			{Name: "R", Args: []string{"--no-save", "--no-restore", "--interactive", "-e", "options(prompt='" + DefaultReadyToken + "', continue='" + DefaultContinuationToken + "')"}},
			{Name: "Rscript", Args: []string{"-e", "options(prompt='" + DefaultReadyToken + "', continue='" + DefaultContinuationToken + "'); while(TRUE) { cat(getOption('prompt')); eval(parse(text=readline())) }"}},
		},
		Ready: ReadyConfig{
			Mode:  ReadyModePromptToken,
			Token: DefaultReadyToken,
		},
	},

	"perl": {
		Name:        "perl",
		DisplayName: "Perl",
		Description: "Perl debugger as REPL",
		Candidates: []ExecCandidate{
			{Name: "reply", Args: []string{}}, // Reply is a better Perl REPL
			{Name: "perl", Args: []string{"-de0"}},
		},
		Ready: ReadyConfig{
			Mode: ReadyModeStartupDelay,
		},
	},

	// Database REPLs
	"redis-cli": {
		Name:        "redis-cli",
		DisplayName: "Redis CLI",
		Description: "Redis command-line interface",
		Candidates: []ExecCandidate{
			{Name: "redis-cli", Args: []string{}},
		},
		Ready: ReadyConfig{
			Mode: ReadyModeStartupDelay,
		},
	},

	"sqlite": {
		Name:        "sqlite",
		DisplayName: "SQLite",
		Description: "SQLite command-line shell",
		Candidates: []ExecCandidate{
			{Name: "sqlite3", Args: []string{"-interactive", "-cmd", ".prompt '" + DefaultReadyToken + "' '" + DefaultContinuationToken + "'"}},
			{Name: "sqlite", Args: []string{"-interactive", "-cmd", ".prompt '" + DefaultReadyToken + "' '" + DefaultContinuationToken + "'"}},
		},
		Ready: ReadyConfig{
			Mode:  ReadyModePromptToken,
			Token: DefaultReadyToken,
		},
	},

	"swift": {
		Name:        "swift",
		DisplayName: "Swift",
		Description: "Swift REPL",
		Candidates: []ExecCandidate{
			{Name: "swift", Args: []string{"-repl"}},
			{Name: "swift", Args: []string{}},
		},
		Ready: ReadyConfig{
			Mode: ReadyModeStartupDelay,
		},
	},

	"kotlin": {
		Name:        "kotlin",
		DisplayName: "Kotlin",
		Description: "Kotlin REPL",
		Candidates: []ExecCandidate{
			{Name: "kotlinc", Args: []string{}},
			{Name: "kotlin", Args: []string{}},
		},
		Ready: ReadyConfig{
			Mode: ReadyModeStartupDelay,
		},
	},
}

// GetBuiltinConfig returns a clone of a built-in config.
func GetBuiltinConfig(name string) (*REPLConfig, bool) {
	config, ok := BuiltinConfigs[name]
	if !ok {
		return nil, false
	}
	return config.Clone(), true
}

// ListBuiltinConfigs returns the names of all built-in configs.
func ListBuiltinConfigs() []string {
	names := make([]string, 0, len(BuiltinConfigs))
	for name := range BuiltinConfigs {
		names = append(names, name)
	}
	return names
}

// REPLRegistry holds custom REPL configurations.
type REPLRegistry struct {
	configs map[string]*REPLConfig
}

// NewREPLRegistry creates a new registry with built-in configs.
func NewREPLRegistry() *REPLRegistry {
	r := &REPLRegistry{
		configs: make(map[string]*REPLConfig),
	}
	// Copy built-in configs
	for name, config := range BuiltinConfigs {
		r.configs[name] = config.Clone()
	}
	return r
}

// Register adds or updates a REPL configuration.
func (r *REPLRegistry) Register(config *REPLConfig) error {
	if err := config.Validate(); err != nil {
		return err
	}
	r.configs[config.Name] = config.Clone()
	return nil
}

// Get returns a REPL configuration.
func (r *REPLRegistry) Get(name string) (*REPLConfig, bool) {
	config, ok := r.configs[name]
	if !ok {
		return nil, false
	}
	return config.Clone(), true
}

// List returns all registered config names.
func (r *REPLRegistry) List() []string {
	names := make([]string, 0, len(r.configs))
	for name := range r.configs {
		names = append(names, name)
	}
	return names
}

// DefaultRegistry is the global registry.
var DefaultRegistry = NewREPLRegistry()
