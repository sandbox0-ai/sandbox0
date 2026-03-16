package proxy

import "fmt"

type adapterKey struct {
	Transport string
	Protocol  string
}

type adapterRegistry struct {
	adapters  map[adapterKey]proxyAdapter
	fallbacks map[string]proxyAdapter
}

func newAdapterRegistry(adapters []proxyAdapter, fallbacks []proxyAdapter) (*adapterRegistry, error) {
	registry := &adapterRegistry{
		adapters:  make(map[adapterKey]proxyAdapter, len(adapters)),
		fallbacks: make(map[string]proxyAdapter, len(fallbacks)),
	}
	for _, adapter := range adapters {
		if err := registry.register(adapter); err != nil {
			return nil, err
		}
	}
	for _, adapter := range fallbacks {
		if err := registry.registerFallback(adapter); err != nil {
			return nil, err
		}
	}
	return registry, nil
}

func (r *adapterRegistry) Resolve(decision trafficDecision) (proxyAdapter, error) {
	if r == nil {
		return nil, fmt.Errorf("adapter registry is nil")
	}
	var adapter proxyAdapter
	switch decision.Action {
	case decisionActionPassThrough:
		adapter = r.fallbacks[decision.Transport]
		if adapter == nil {
			return nil, fmt.Errorf("fallback adapter not found for transport %q", decision.Transport)
		}
	case decisionActionUseAdapter:
		adapter = r.adapters[adapterKey{Transport: decision.Transport, Protocol: decision.Protocol}]
		if adapter == nil {
			return nil, fmt.Errorf("adapter not found for transport %q protocol %q", decision.Transport, decision.Protocol)
		}
	default:
		return nil, nil
	}
	if err := validateAdapterCapability(decision, adapter); err != nil {
		return nil, err
	}
	return adapter, nil
}

func (r *adapterRegistry) register(adapter proxyAdapter) error {
	if adapter == nil {
		return fmt.Errorf("adapter is nil")
	}
	key := adapterKey{
		Transport: adapter.Transport(),
		Protocol:  adapter.Protocol(),
	}
	if key.Transport == "" || key.Protocol == "" {
		return fmt.Errorf("adapter %q must declare transport and protocol", adapter.Name())
	}
	if key.Protocol == "unknown" {
		return fmt.Errorf("adapter %q with protocol unknown must be registered as fallback", adapter.Name())
	}
	if existing := r.adapters[key]; existing != nil {
		return fmt.Errorf("adapter already registered for transport %q protocol %q", key.Transport, key.Protocol)
	}
	r.adapters[key] = adapter
	return nil
}

func (r *adapterRegistry) registerFallback(adapter proxyAdapter) error {
	if adapter == nil {
		return fmt.Errorf("fallback adapter is nil")
	}
	transport := adapter.Transport()
	if transport == "" {
		return fmt.Errorf("fallback adapter %q must declare transport", adapter.Name())
	}
	if adapter.Protocol() != "unknown" {
		return fmt.Errorf("fallback adapter %q must use protocol unknown", adapter.Name())
	}
	if adapter.Capability() != adapterCapabilityPassThrough {
		return fmt.Errorf("fallback adapter %q must use pass-through capability", adapter.Name())
	}
	if existing := r.fallbacks[transport]; existing != nil {
		return fmt.Errorf("fallback adapter already registered for transport %q", transport)
	}
	r.fallbacks[transport] = adapter
	return nil
}

func validateAdapterCapability(decision trafficDecision, adapter proxyAdapter) error {
	if adapter == nil {
		return fmt.Errorf("adapter is nil")
	}
	switch adapter.Capability() {
	case adapterCapabilityPassThrough, adapterCapabilityInspect, adapterCapabilityTerminate:
	default:
		return fmt.Errorf("adapter %q has unsupported capability %q", adapter.Name(), adapter.Capability())
	}
	if decision.Action == decisionActionPassThrough && adapter.Capability() != adapterCapabilityPassThrough {
		return fmt.Errorf("adapter %q must use pass-through capability for fallback decisions", adapter.Name())
	}
	return nil
}
