package observability

import (
	coreobs "github.com/sandbox0-ai/sandbox0/pkg/observability/core"
	httpobs "github.com/sandbox0-ai/sandbox0/pkg/observability/http"
	k8sobs "github.com/sandbox0-ai/sandbox0/pkg/observability/k8s"
	pgxobs "github.com/sandbox0-ai/sandbox0/pkg/observability/pgx"
)

// Provider combines the observability core with the optional client adapters
// used by control-plane and data-plane services.
type Provider struct {
	*coreobs.Provider

	HTTP httpobs.Adapter
	K8s  k8sobs.Adapter
	Pgx  pgxobs.Adapter
}

// New creates a new observability provider.
func New(cfg Config) (*Provider, error) {
	coreProvider, err := coreobs.New(coreobs.Config(cfg))
	if err != nil {
		return nil, err
	}
	normalized := coreProvider.Config()
	disabled := normalized.DisableTracing && normalized.DisableMetrics && normalized.DisableLogging

	httpAdapter := httpobs.NewAdapter(httpobs.AdapterConfig{
		ServiceName:    normalized.ServiceName,
		Tracer:         coreProvider.Tracer(),
		Logger:         normalized.Logger,
		Registry:       normalized.MetricsRegistry,
		DisableMetrics: normalized.DisableMetrics,
		DisableLogging: normalized.DisableLogging,
		Disabled:       disabled,
	})
	k8sAdapter := k8sobs.NewAdapter(k8sobs.AdapterConfig{
		ServiceName:    normalized.ServiceName,
		Tracer:         coreProvider.Tracer(),
		Logger:         normalized.Logger,
		Registry:       normalized.MetricsRegistry,
		DisableMetrics: normalized.DisableMetrics,
		DisableLogging: normalized.DisableLogging,
		Disabled:       disabled,
	})
	pgxAdapter := pgxobs.NewAdapter(pgxobs.AdapterConfig{
		ServiceName:    normalized.ServiceName,
		Tracer:         coreProvider.Tracer(),
		Logger:         normalized.Logger,
		Registry:       normalized.MetricsRegistry,
		DisableMetrics: normalized.DisableMetrics,
		DisableLogging: normalized.DisableLogging,
		Disabled:       disabled,
	})

	return &Provider{
		Provider: coreProvider,
		HTTP:     httpAdapter,
		K8s:      k8sAdapter,
		Pgx:      pgxAdapter,
	}, nil
}
