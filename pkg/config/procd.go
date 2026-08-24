package config

import "github.com/sandbox0-ai/sandbox0/pkg/procdconfig"

const (
	DefaultWebhookOutboxDir = procdconfig.DefaultWebhookOutboxDir
	DefaultSessionStateDir  = procdconfig.DefaultSessionStateDir
)

// ProcdConfig exposes the shared procd runtime configuration inside manager
// configuration while keeping the procd binary independent from this package.
type ProcdConfig struct {
	procdconfig.Config `yaml:",inline" json:",inline"`
}
