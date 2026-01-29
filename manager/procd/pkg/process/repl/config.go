// Package repl provides configurable REPL process implementations.
package repl

import (
	"fmt"
	"regexp"
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

// PromptConfig defines how to detect and customize prompts.
type PromptConfig struct {
	// Patterns are regex patterns to detect prompt.
	Patterns []string `json:"patterns,omitempty" yaml:"patterns,omitempty"`
	// CustomPrompt is the custom prompt to set (if supported by the REPL).
	CustomPrompt string `json:"custom_prompt,omitempty" yaml:"custom_prompt,omitempty"`
	// PromptEnvVar is the env var name to set prompt (e.g., "PS1" for shells).
	PromptEnvVar string `json:"prompt_env_var,omitempty" yaml:"prompt_env_var,omitempty"`
}

// OutputConfig defines output processing configuration.
type OutputConfig struct {
	// StripANSI removes ANSI escape sequences from output.
	StripANSI bool `json:"strip_ansi,omitempty" yaml:"strip_ansi,omitempty"`
	// FilterEcho removes echoed input from output.
	FilterEcho bool `json:"filter_echo,omitempty" yaml:"filter_echo,omitempty"`
	// TrimPrompt removes prompt patterns from output.
	TrimPrompt bool `json:"trim_prompt,omitempty" yaml:"trim_prompt,omitempty"`
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

	// Output processing configuration.
	Output OutputConfig `json:"output,omitempty" yaml:"output,omitempty"`

	// InitCommands are commands to run after REPL starts (e.g., disable history).
	InitCommands []string `json:"init_commands,omitempty" yaml:"init_commands,omitempty"`

	// ExitCommand is the command to gracefully exit the REPL.
	ExitCommand string `json:"exit_command,omitempty" yaml:"exit_command,omitempty"`
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
	// Validate prompt patterns are valid regex
	if len(c.Prompt.Patterns) == 0 {
		return fmt.Errorf("prompt patterns are required for REPL completion detection")
	}
	for i, pattern := range c.Prompt.Patterns {
		if _, err := regexp.Compile(pattern); err != nil {
			return fmt.Errorf("prompt pattern %d is invalid regex: %w", i, err)
		}
	}
	return nil
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
	clone.Prompt.Patterns = append([]string(nil), c.Prompt.Patterns...)
	clone.InitCommands = append([]string(nil), c.InitCommands...)
	return &clone
}

// DefaultPrompt is the default custom prompt for shell-like REPLs.
const DefaultPrompt = "SANDBOX0>>> "

// BuiltinConfigs contains built-in REPL configurations.
var BuiltinConfigs = map[string]*REPLConfig{
	"python": {
		Name:        "python",
		DisplayName: "Python",
		Description: "Python interactive interpreter",
		Candidates: []ExecCandidate{
			{Name: "ipython3", Args: []string{"--simple-prompt", "-i", "--no-banner", "--colors=NoColor"}},
			{Name: "ipython", Args: []string{"--simple-prompt", "-i", "--no-banner", "--colors=NoColor"}},
			{Name: "python3", Args: []string{"-i", "-u"}},
			{Name: "python", Args: []string{"-i", "-u"}},
			{Name: "python2", Args: []string{"-i", "-u"}},
		},
		Env: []EnvVar{
			{Name: "PYTHONUNBUFFERED", Value: "1"},
			{Name: "PYTHONDONTWRITEBYTECODE", Value: "1"},
		},
		Prompt: PromptConfig{
			Patterns: []string{
				`In \[\d+\]:`, // IPython input prompt
				`Out\[\d+\]:`, // IPython output prompt
				`\.{3}:`,      // IPython continuation
				`>>> `,        // Standard Python prompt
				`\.\.\. `,     // Standard continuation
			},
		},
		Output: OutputConfig{
			StripANSI:  true,
			FilterEcho: true,
			TrimPrompt: true,
		},
		ExitCommand: "exit()",
	},

	"node": {
		Name:        "node",
		DisplayName: "Node.js",
		Description: "Node.js JavaScript runtime REPL",
		Candidates: []ExecCandidate{
			{Name: "node", Args: []string{"--interactive"}},
			{Name: "nodejs", Args: []string{"--interactive"}},
		},
		Prompt: PromptConfig{
			Patterns: []string{
				`> `,      // Standard Node prompt
				`\.\.\. `, // Continuation
			},
		},
		Output: OutputConfig{
			StripANSI:  true,
			FilterEcho: true,
			TrimPrompt: true,
		},
		ExitCommand: ".exit",
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
			CustomPrompt: DefaultPrompt,
			PromptEnvVar: "PS1",
			Patterns: []string{
				regexp.QuoteMeta(DefaultPrompt),
			},
		},
		Output: OutputConfig{
			StripANSI:  true,
			FilterEcho: true,
			TrimPrompt: true,
		},
		ExitCommand: "exit",
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
			CustomPrompt: DefaultPrompt,
			PromptEnvVar: "PS1",
			Patterns: []string{
				regexp.QuoteMeta(DefaultPrompt),
			},
		},
		Output: OutputConfig{
			StripANSI:  true,
			FilterEcho: true,
			TrimPrompt: true,
		},
		ExitCommand: "exit",
	},

	"ruby": {
		Name:        "ruby",
		DisplayName: "Ruby",
		Description: "Ruby IRB (Interactive Ruby)",
		Candidates: []ExecCandidate{
			{Name: "irb", Args: []string{"--simple-prompt", "--noreadline"}},
			{Name: "ruby", Args: []string{"-e", "require 'irb'; IRB.start"}},
		},
		Prompt: PromptConfig{
			Patterns: []string{
				`>> `,     // IRB simple prompt
				`\?> `,    // IRB continuation
				`irb.*> `, // IRB standard prompt
			},
		},
		Output: OutputConfig{
			StripANSI:  true,
			FilterEcho: true,
			TrimPrompt: true,
		},
		ExitCommand: "exit",
	},

	"lua": {
		Name:        "lua",
		DisplayName: "Lua",
		Description: "Lua interpreter",
		Candidates: []ExecCandidate{
			{Name: "lua", Args: []string{"-i"}},
			{Name: "lua5.4", Args: []string{"-i"}},
			{Name: "lua5.3", Args: []string{"-i"}},
			{Name: "lua5.2", Args: []string{"-i"}},
			{Name: "lua5.1", Args: []string{"-i"}},
			{Name: "luajit", Args: []string{"-i"}},
		},
		Prompt: PromptConfig{
			Patterns: []string{
				`> `,  // Lua prompt
				`>> `, // Continuation
			},
		},
		Output: OutputConfig{
			StripANSI:  true,
			FilterEcho: true,
			TrimPrompt: true,
		},
		ExitCommand: "os.exit()",
	},

	"php": {
		Name:        "php",
		DisplayName: "PHP",
		Description: "PHP interactive mode",
		Candidates: []ExecCandidate{
			{Name: "psysh", Args: []string{}}, // PsySH is better for REPL
			{Name: "php", Args: []string{"-a"}},
		},
		Prompt: PromptConfig{
			Patterns: []string{
				`php > `,    // PHP prompt
				`>>> `,      // PsySH prompt
				`\.\.\. > `, // Continuation
			},
		},
		Output: OutputConfig{
			StripANSI:  true,
			FilterEcho: true,
			TrimPrompt: true,
		},
		ExitCommand: "exit",
	},

	"r": {
		Name:        "r",
		DisplayName: "R",
		Description: "R statistical computing",
		Candidates: []ExecCandidate{
			{Name: "R", Args: []string{"--no-save", "--no-restore", "--interactive"}},
			{Name: "Rscript", Args: []string{"-e", "options(prompt='> '); while(TRUE) { cat('> '); eval(parse(text=readline())) }"}},
		},
		Prompt: PromptConfig{
			Patterns: []string{
				`> `,  // R prompt
				`\+ `, // Continuation
			},
		},
		Output: OutputConfig{
			StripANSI:  true,
			FilterEcho: true,
			TrimPrompt: true,
		},
		ExitCommand: "q()",
	},

	"perl": {
		Name:        "perl",
		DisplayName: "Perl",
		Description: "Perl debugger as REPL",
		Candidates: []ExecCandidate{
			{Name: "reply", Args: []string{}}, // Reply is a better Perl REPL
			{Name: "perl", Args: []string{"-de0"}},
		},
		Prompt: PromptConfig{
			Patterns: []string{
				`DB<\d+> `, // Perl debugger prompt
				`\d+> `,    // Reply prompt
			},
		},
		Output: OutputConfig{
			StripANSI:  true,
			FilterEcho: true,
			TrimPrompt: true,
		},
		ExitCommand: "q",
	},

	// Database REPLs
	"redis-cli": {
		Name:        "redis-cli",
		DisplayName: "Redis CLI",
		Description: "Redis command-line interface",
		Candidates: []ExecCandidate{
			{Name: "redis-cli", Args: []string{}},
		},
		Prompt: PromptConfig{
			Patterns: []string{
				`\d+\.\d+\.\d+\.\d+:\d+> `, // Connected prompt (127.0.0.1:6379>)
				`redis.*> `,                // Named connection
				`> `,                       // Generic
			},
		},
		Output: OutputConfig{
			StripANSI:  true,
			FilterEcho: true,
			TrimPrompt: true,
		},
		ExitCommand: "QUIT",
	},

	"sqlite": {
		Name:        "sqlite",
		DisplayName: "SQLite",
		Description: "SQLite command-line shell",
		Candidates: []ExecCandidate{
			{Name: "sqlite3", Args: []string{"-interactive"}},
			{Name: "sqlite", Args: []string{"-interactive"}},
		},
		Prompt: PromptConfig{
			Patterns: []string{
				`sqlite> `,
				`   \.\.\.> `, // Continuation
			},
		},
		Output: OutputConfig{
			StripANSI:  true,
			FilterEcho: true,
			TrimPrompt: true,
		},
		ExitCommand: ".quit",
	},

	"mysql": {
		Name:        "mysql",
		DisplayName: "MySQL",
		Description: "MySQL command-line client",
		Candidates: []ExecCandidate{
			{Name: "mysql", Args: []string{}},
			{Name: "mariadb", Args: []string{}},
		},
		Prompt: PromptConfig{
			Patterns: []string{
				`mysql> `,
				`    -> `, // Continuation
				`MariaDB.*> `,
			},
		},
		Output: OutputConfig{
			StripANSI:  true,
			FilterEcho: true,
			TrimPrompt: true,
		},
		ExitCommand: "exit",
	},

	"psql": {
		Name:        "psql",
		DisplayName: "PostgreSQL",
		Description: "PostgreSQL interactive terminal",
		Candidates: []ExecCandidate{
			{Name: "psql", Args: []string{}},
		},
		Prompt: PromptConfig{
			Patterns: []string{
				`\w+=# `, // Superuser prompt
				`\w+=> `, // Normal user prompt
				`\w+-# `, // Continuation (superuser)
				`\w+-> `, // Continuation (normal)
			},
		},
		Output: OutputConfig{
			StripANSI:  true,
			FilterEcho: true,
			TrimPrompt: true,
		},
		ExitCommand: `\q`,
	},

	// Additional language REPLs
	"elixir": {
		Name:        "elixir",
		DisplayName: "Elixir",
		Description: "Elixir IEx shell",
		Candidates: []ExecCandidate{
			{Name: "iex", Args: []string{}},
		},
		Prompt: PromptConfig{
			Patterns: []string{
				`iex\(\d+\)> `,   // IEx prompt
				`\.{3}\(\d+\)> `, // Continuation
			},
		},
		Output: OutputConfig{
			StripANSI:  true,
			FilterEcho: true,
			TrimPrompt: true,
		},
		ExitCommand: "System.halt",
	},

	"erlang": {
		Name:        "erlang",
		DisplayName: "Erlang",
		Description: "Erlang shell",
		Candidates: []ExecCandidate{
			{Name: "erl", Args: []string{}},
		},
		Prompt: PromptConfig{
			Patterns: []string{
				`\d+> `, // Erlang prompt
			},
		},
		Output: OutputConfig{
			StripANSI:  true,
			FilterEcho: true,
			TrimPrompt: true,
		},
		ExitCommand: "q().",
	},

	"scala": {
		Name:        "scala",
		DisplayName: "Scala",
		Description: "Scala REPL",
		Candidates: []ExecCandidate{
			{Name: "scala", Args: []string{}},
			{Name: "amm", Args: []string{}}, // Ammonite REPL
		},
		Prompt: PromptConfig{
			Patterns: []string{
				`scala> `,
				`     \| `, // Continuation
				`@ `,       // Ammonite
			},
		},
		Output: OutputConfig{
			StripANSI:  true,
			FilterEcho: true,
			TrimPrompt: true,
		},
		ExitCommand: ":quit",
	},

	"clojure": {
		Name:        "clojure",
		DisplayName: "Clojure",
		Description: "Clojure REPL",
		Candidates: []ExecCandidate{
			{Name: "clj", Args: []string{}},
			{Name: "clojure", Args: []string{}},
			{Name: "lein", Args: []string{"repl"}},
		},
		Prompt: PromptConfig{
			Patterns: []string{
				`user=> `, // Default Clojure prompt
				`\w+=> `,  // Namespace prompt
				`#_=> `,   // Continuation
			},
		},
		Output: OutputConfig{
			StripANSI:  true,
			FilterEcho: true,
			TrimPrompt: true,
		},
		ExitCommand: "(System/exit 0)",
	},

	"haskell": {
		Name:        "haskell",
		DisplayName: "Haskell",
		Description: "GHCi - Glasgow Haskell Compiler interactive",
		Candidates: []ExecCandidate{
			{Name: "ghci", Args: []string{}},
		},
		Prompt: PromptConfig{
			Patterns: []string{
				`Prelude> `,
				`\*?\w+> `, // Module prompt
				`ghci> `,
			},
		},
		Output: OutputConfig{
			StripANSI:  true,
			FilterEcho: true,
			TrimPrompt: true,
		},
		ExitCommand: ":quit",
	},

	"ocaml": {
		Name:        "ocaml",
		DisplayName: "OCaml",
		Description: "OCaml toplevel",
		Candidates: []ExecCandidate{
			{Name: "utop", Args: []string{}}, // Better UX
			{Name: "ocaml", Args: []string{}},
		},
		Prompt: PromptConfig{
			Patterns: []string{
				`# `,            // Standard OCaml
				`utop\[\d+\]> `, // UTop
			},
		},
		Output: OutputConfig{
			StripANSI:  true,
			FilterEcho: true,
			TrimPrompt: true,
		},
		ExitCommand: "#quit;;",
	},

	"julia": {
		Name:        "julia",
		DisplayName: "Julia",
		Description: "Julia REPL",
		Candidates: []ExecCandidate{
			{Name: "julia", Args: []string{}},
		},
		Prompt: PromptConfig{
			Patterns: []string{
				`julia> `,
				`help\?> `, // Help mode
				`shell> `,  // Shell mode
				`pkg> `,    // Package mode
			},
		},
		Output: OutputConfig{
			StripANSI:  true,
			FilterEcho: true,
			TrimPrompt: true,
		},
		ExitCommand: "exit()",
	},

	"swift": {
		Name:        "swift",
		DisplayName: "Swift",
		Description: "Swift REPL",
		Candidates: []ExecCandidate{
			{Name: "swift", Args: []string{"-repl"}},
			{Name: "swift", Args: []string{}},
		},
		Prompt: PromptConfig{
			Patterns: []string{
				`\d+> `,  // Swift REPL prompt
				`\d+\. `, // Continuation
			},
		},
		Output: OutputConfig{
			StripANSI:  true,
			FilterEcho: true,
			TrimPrompt: true,
		},
		ExitCommand: ":quit",
	},

	"kotlin": {
		Name:        "kotlin",
		DisplayName: "Kotlin",
		Description: "Kotlin REPL",
		Candidates: []ExecCandidate{
			{Name: "kotlinc", Args: []string{}},
			{Name: "kotlin", Args: []string{}},
		},
		Prompt: PromptConfig{
			Patterns: []string{
				`>>> `,    // Kotlin prompt
				`\.\.\. `, // Continuation
			},
		},
		Output: OutputConfig{
			StripANSI:  true,
			FilterEcho: true,
			TrimPrompt: true,
		},
		ExitCommand: ":quit",
	},

	"groovy": {
		Name:        "groovy",
		DisplayName: "Groovy",
		Description: "Groovy shell",
		Candidates: []ExecCandidate{
			{Name: "groovysh", Args: []string{}},
		},
		Prompt: PromptConfig{
			Patterns: []string{
				`groovy:\d+> `,
			},
		},
		Output: OutputConfig{
			StripANSI:  true,
			FilterEcho: true,
			TrimPrompt: true,
		},
		ExitCommand: ":exit",
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
