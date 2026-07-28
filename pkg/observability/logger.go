package observability

import (
	coreobs "github.com/sandbox0-ai/sandbox0/pkg/observability/core"
	"go.uber.org/zap"
)

// LoggerConfig configures the shared zap logger used by sandbox0 services.
type LoggerConfig = coreobs.LoggerConfig

// MigrateLogger adapts zap logging to migration packages.
type MigrateLogger = coreobs.MigrateLogger

// NewLogger creates a production JSON zap logger with the shared sandbox0 log format.
func NewLogger(cfg LoggerConfig) (*zap.Logger, error) {
	return coreobs.NewLogger(cfg)
}

// NewMigrateLogger returns a logger compatible with migration libraries.
func NewMigrateLogger(logger *zap.Logger) MigrateLogger {
	return coreobs.NewMigrateLogger(logger)
}
