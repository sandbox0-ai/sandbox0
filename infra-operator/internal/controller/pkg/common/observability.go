package common

import (
	"net/url"
	"sort"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"

	infrav1alpha1 "github.com/sandbox0-ai/sandbox0/infra-operator/api/v1alpha1"
)

const (
	envOTELResourceAttributes        = "OTEL_RESOURCE_ATTRIBUTES"
	envOTELTracesExporter            = "OTEL_TRACES_EXPORTER"
	envOTELTracesSampler             = "OTEL_TRACES_SAMPLER"
	envOTELTracesSamplerArg          = "OTEL_TRACES_SAMPLER_ARG"
	envOTELExporterOTLPTraceEndpoint = "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"
	envOTELExporterOTLPTraceHeaders  = "OTEL_EXPORTER_OTLP_TRACES_HEADERS"
	envOTELExporterOTLPTraceInsecure = "OTEL_EXPORTER_OTLP_TRACES_INSECURE"
	envOTELExporterOTLPTraceTimeout  = "OTEL_EXPORTER_OTLP_TRACES_TIMEOUT"
)

// ObservabilityEnvConfig identifies the platform component represented by a pod.
type ObservabilityEnvConfig struct {
	ServiceName string
	RegionID    string
	ClusterID   string
}

// AppendObservabilityEnvVars appends standard OpenTelemetry env vars for a
// platform component without overriding env vars already supplied by callers.
func AppendObservabilityEnvVars(env []corev1.EnvVar, infra *infrav1alpha1.Sandbox0Infra, cfg ObservabilityEnvConfig) []corev1.EnvVar {
	env = appendFieldRefEnvVarIfMissing(env, "POD_NAME", "metadata.name")
	env = appendFieldRefEnvVarIfMissing(env, "POD_NAMESPACE", "metadata.namespace")
	env = appendFieldRefEnvVarIfMissing(env, "NODE_NAME", "spec.nodeName")

	attrs := observabilityResourceAttributes(infra, cfg)
	if len(attrs) > 0 {
		env = appendLiteralEnvVarIfMissing(env, envOTELResourceAttributes, encodeKeyValueList(attrs))
	}

	if infra == nil || infra.Spec.Observability == nil || infra.Spec.Observability.Traces == nil || !infra.Spec.Observability.Traces.Enabled {
		return env
	}
	traces := infra.Spec.Observability.Traces
	exporter := strings.TrimSpace(string(traces.Exporter))
	if exporter == "" {
		exporter = string(infrav1alpha1.ObservabilityTraceExporterOTLP)
	}
	env = appendLiteralEnvVarIfMissing(env, envOTELTracesExporter, exporter)
	if endpoint := strings.TrimSpace(traces.Endpoint); endpoint != "" {
		env = appendLiteralEnvVarIfMissing(env, envOTELExporterOTLPTraceEndpoint, endpoint)
	}
	if len(traces.Headers) > 0 {
		env = appendLiteralEnvVarIfMissing(env, envOTELExporterOTLPTraceHeaders, encodeHeaderList(traces.Headers))
	}
	if traces.Insecure != nil {
		env = appendLiteralEnvVarIfMissing(env, envOTELExporterOTLPTraceInsecure, strconv.FormatBool(*traces.Insecure))
	}
	if traces.Timeout.Duration > 0 {
		env = appendLiteralEnvVarIfMissing(env, envOTELExporterOTLPTraceTimeout, traces.Timeout.Duration.String())
	}
	if sampleRate := strings.TrimSpace(traces.SampleRate); sampleRate != "" {
		rate, err := strconv.ParseFloat(sampleRate, 64)
		if err == nil {
			env = appendTraceSamplingEnvVars(env, rate)
		}
	}
	return env
}

func observabilityResourceAttributes(infra *infrav1alpha1.Sandbox0Infra, cfg ObservabilityEnvConfig) map[string]string {
	attrs := map[string]string{}
	if infra != nil && infra.Spec.Observability != nil {
		for key, value := range infra.Spec.Observability.ResourceAttributes {
			key = strings.TrimSpace(key)
			if key != "" {
				attrs[key] = strings.TrimSpace(value)
			}
		}
	}
	if serviceName := strings.TrimSpace(cfg.ServiceName); serviceName != "" {
		attrs["service.name"] = serviceName
	}
	if regionID := strings.TrimSpace(cfg.RegionID); regionID != "" {
		attrs["sandbox0.region.id"] = regionID
	}
	if clusterID := strings.TrimSpace(cfg.ClusterID); clusterID != "" {
		attrs["sandbox0.cluster.id"] = clusterID
	}
	attrs["k8s.namespace.name"] = "$(POD_NAMESPACE)"
	attrs["k8s.pod.name"] = "$(POD_NAME)"
	attrs["k8s.node.name"] = "$(NODE_NAME)"
	return attrs
}

func appendTraceSamplingEnvVars(env []corev1.EnvVar, rate float64) []corev1.EnvVar {
	switch {
	case rate < 0 || rate > 1:
		return env
	case rate == 0:
		return appendLiteralEnvVarIfMissing(env, envOTELTracesSampler, "always_off")
	case rate == 1:
		return appendLiteralEnvVarIfMissing(env, envOTELTracesSampler, "always_on")
	default:
		env = appendLiteralEnvVarIfMissing(env, envOTELTracesSampler, "parentbased_traceidratio")
		return appendLiteralEnvVarIfMissing(env, envOTELTracesSamplerArg, strconv.FormatFloat(rate, 'f', -1, 64))
	}
}

func appendLiteralEnvVarIfMissing(env []corev1.EnvVar, name, value string) []corev1.EnvVar {
	if envVarExists(env, name) {
		return env
	}
	return append(env, corev1.EnvVar{Name: name, Value: value})
}

func appendFieldRefEnvVarIfMissing(env []corev1.EnvVar, name, fieldPath string) []corev1.EnvVar {
	if envVarExists(env, name) {
		return env
	}
	return append(env, corev1.EnvVar{
		Name: name,
		ValueFrom: &corev1.EnvVarSource{
			FieldRef: &corev1.ObjectFieldSelector{FieldPath: fieldPath},
		},
	})
}

func envVarExists(env []corev1.EnvVar, name string) bool {
	for _, item := range env {
		if item.Name == name {
			return true
		}
	}
	return false
}

func encodeKeyValueList(values map[string]string) string {
	keys := make([]string, 0, len(values))
	for key := range values {
		if strings.TrimSpace(key) != "" {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, key+"="+values[key])
	}
	return strings.Join(parts, ",")
}

func encodeHeaderList(values map[string]string) string {
	keys := make([]string, 0, len(values))
	for key := range values {
		if strings.TrimSpace(key) != "" {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, strings.TrimSpace(key)+"="+url.QueryEscape(strings.TrimSpace(values[key])))
	}
	return strings.Join(parts, ",")
}
