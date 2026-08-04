package observability

import (
	coreobs "github.com/sandbox0-ai/sandbox0/pkg/observability/core"
	"go.uber.org/zap"
)

var (
	ErrMissingServiceName = coreobs.ErrMissingServiceName
	ErrMissingLogger      = coreobs.ErrMissingLogger
)

// Config holds configuration for the observability provider.
type Config coreobs.Config

// TraceExporterConfig configures where traces are sent.
type TraceExporterConfig = coreobs.TraceExporterConfig

// ConfigFromEnv builds service observability config from standard OpenTelemetry
// environment variables while preserving the legacy sandbox0 exporter envs.
func ConfigFromEnv(serviceName string, logger *zap.Logger) Config {
	return Config(coreobs.ConfigFromEnv(serviceName, logger))
}
