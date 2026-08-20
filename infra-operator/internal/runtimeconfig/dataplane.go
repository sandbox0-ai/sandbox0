package runtimeconfig

import (
	apiconfig "github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	infrav1alpha1 "github.com/sandbox0-ai/sandbox0/infra-operator/api/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/pkg/procdconfig"
)

func ToManager(spec *infrav1alpha1.ManagerConfig) *apiconfig.ManagerConfig {
	cfg := &apiconfig.ManagerConfig{}
	if spec == nil {
		return cfg
	}
	webhookOutboxDir := ""
	if spec.ProcdConfig.WebhookOutboxDir != nil {
		webhookOutboxDir = *spec.ProcdConfig.WebhookOutboxDir
	}

	cfg.HTTPPort = spec.HTTPPort
	cfg.KubeConfig = spec.KubeConfig
	cfg.K8sClientQPS = spec.K8sClientQPS
	cfg.K8sClientBurst = spec.K8sClientBurst
	cfg.LeaderElection = spec.LeaderElection
	cfg.ResyncPeriod = spec.ResyncPeriod
	cfg.DatabaseMaxConns = spec.DatabaseMaxConns
	cfg.DatabaseMinConns = spec.DatabaseMinConns
	cfg.CleanupInterval = spec.CleanupInterval
	cfg.AutoscalerSafeToEvictAnnotationKeys = cloneStrings(spec.AutoscalerSafeToEvictAnnotationKeys)
	cfg.PodTeardown = apiconfig.PodTeardownConfig{
		MaxConcurrentPerNode:         spec.PodTeardown.MaxConcurrentPerNode,
		MaxConcurrentPerDegradedNode: spec.PodTeardown.MaxConcurrentPerDegradedNode,
		MaxConcurrentReplacements:    spec.PodTeardown.MaxConcurrentReplacements,
	}
	cfg.LogLevel = spec.LogLevel
	cfg.MetricsPort = spec.MetricsPort
	cfg.WebhookPort = spec.WebhookPort
	cfg.WebhookCertPath = spec.WebhookCertPath
	cfg.WebhookKeyPath = spec.WebhookKeyPath
	cfg.DefaultSandboxTTL = spec.DefaultSandboxTTL
	cfg.SandboxRuntimeBackend = spec.SandboxRuntimeBackend
	cfg.TeamTemplateMemoryPerCPU = spec.TeamTemplateMemoryPerCPU
	cfg.SandboxMaxMemory = spec.SandboxMaxMemory
	cfg.SandboxRuntimeClassName = spec.SandboxRuntimeClassName
	cfg.ProcdBinImageRef = spec.ProcdBinImageRef
	cfg.DefaultTeamQuotas = cloneTeamQuotaLimitConfigs(spec.DefaultTeamQuotas)
	cfg.AllowColdStartWithoutReadyDataPlane = spec.AllowColdStartWithoutReadyDataPlane
	cfg.NetworkPolicyApplyTimeout = spec.NetworkPolicyApplyTimeout
	cfg.NetworkPolicyApplyPollInterval = spec.NetworkPolicyApplyPollInterval
	cfg.EgressAuthDefaultResolveTTL = spec.EgressAuthDefaultResolveTTL
	cfg.PauseMinMemoryRequest = spec.PauseMinMemoryRequest
	cfg.PauseMinMemoryLimit = spec.PauseMinMemoryLimit
	cfg.PauseMemoryBufferRatio = spec.PauseMemoryBufferRatio
	cfg.PauseMinCPU = spec.PauseMinCPU
	cfg.ProcdClientTimeout = spec.ProcdClientTimeout
	cfg.RuntimeReadyTimeout = spec.RuntimeReadyTimeout
	cfg.ShutdownTimeout = spec.ShutdownTimeout
	cfg.ProcdConfig = apiconfig.ProcdConfig{
		Config: procdconfig.Config{
			HTTPPort:               spec.ProcdConfig.HTTPPort,
			LogLevel:               spec.ProcdConfig.LogLevel,
			RootPath:               spec.ProcdConfig.RootPath,
			ContextCleanupInterval: procdconfig.Duration{Duration: spec.ProcdConfig.ContextCleanupInterval.Duration},
			ContextIdleTimeout:     procdconfig.Duration{Duration: spec.ProcdConfig.ContextIdleTimeout.Duration},
			ContextMaxLifetime:     procdconfig.Duration{Duration: spec.ProcdConfig.ContextMaxLifetime.Duration},
			ContextFinishedTTL:     procdconfig.Duration{Duration: spec.ProcdConfig.ContextFinishedTTL.Duration},
			WebhookQueueSize:       spec.ProcdConfig.WebhookQueueSize,
			WebhookRequestTimeout:  procdconfig.Duration{Duration: spec.ProcdConfig.WebhookRequestTimeout.Duration},
			WebhookMaxRetries:      spec.ProcdConfig.WebhookMaxRetries,
			WebhookBaseBackoff:     procdconfig.Duration{Duration: spec.ProcdConfig.WebhookBaseBackoff.Duration},
			WebhookOutboxDir:       webhookOutboxDir,
			SessionStateDir:        apiconfig.DefaultSessionStateDir,
		},
	}
	cfg.NodeAuthority = apiconfig.NodeAuthorityConfig{
		Enabled: spec.NodeAuthority.Enabled, ListenHost: spec.NodeAuthority.ListenHost,
		Port: spec.NodeAuthority.Port, TLSSecretName: spec.NodeAuthority.TLSSecretName,
		WriterLeaseTTL:          spec.NodeAuthority.WriterLeaseTTL,
		WriterRenewalGrace:      spec.NodeAuthority.WriterRenewalGrace,
		RuntimeSlotHeartbeatTTL: spec.NodeAuthority.RuntimeSlotHeartbeatTTL,
		Identities:              cloneNodeAuthorityIdentities(spec.NodeAuthority.Identities),
		Claim: apiconfig.RuntimeSlotClaimConfig{
			SecretName: spec.NodeAuthority.Claim.SecretName,
			ClaimTTL:   spec.NodeAuthority.Claim.ClaimTTL,
			SLO:        spec.NodeAuthority.Claim.SLO,
		},
		Terminal: apiconfig.RuntimeSlotTerminalConfig{
			Enabled:           spec.NodeAuthority.Terminal.Enabled,
			ControlSecretName: spec.NodeAuthority.Terminal.ControlSecretName,
			Interval:          spec.NodeAuthority.Terminal.Interval,
			PassTimeout:       spec.NodeAuthority.Terminal.PassTimeout,
			ScanLimit:         spec.NodeAuthority.Terminal.ScanLimit,
		},
	}
	cfg.Autoscaler = apiconfig.AutoscalerConfig{
		MinScaleInterval:        spec.Autoscaler.MinScaleInterval,
		ScaleUpFactor:           spec.Autoscaler.ScaleUpFactor,
		MaxScaleStep:            spec.Autoscaler.MaxScaleStep,
		MinIdleBuffer:           spec.Autoscaler.MinIdleBuffer,
		TargetIdleRatio:         spec.Autoscaler.TargetIdleRatio,
		NoTrafficScaleDownAfter: spec.Autoscaler.NoTrafficScaleDownAfter,
		ScaleDownPercent:        spec.Autoscaler.ScaleDownPercent,
	}
	return cfg
}

func cloneNodeAuthorityIdentities(in []infrav1alpha1.NodeAuthorityIdentityConfig) []apiconfig.NodeAuthorityIdentityConfig {
	if len(in) == 0 {
		return nil
	}
	out := make([]apiconfig.NodeAuthorityIdentityConfig, 0, len(in))
	for _, identity := range in {
		out = append(out, apiconfig.NodeAuthorityIdentityConfig{
			CommonName: identity.CommonName, ClusterID: identity.ClusterID,
			NodeID: identity.NodeID, NodeUID: identity.NodeUID, PodUID: identity.PodUID,
		})
	}
	return out
}

func cloneTeamQuotaLimitConfigs(in []infrav1alpha1.TeamQuotaLimitConfig) []apiconfig.TeamQuotaLimitConfig {
	if len(in) == 0 {
		return nil
	}
	out := make([]apiconfig.TeamQuotaLimitConfig, 0, len(in))
	for _, limit := range in {
		out = append(out, apiconfig.TeamQuotaLimitConfig{
			Dimension:  limit.Dimension,
			LimitValue: limit.LimitValue,
			IntervalMS: limit.IntervalMS,
			BurstValue: limit.BurstValue,
		})
	}
	return out
}

func ToNetworkRuntime(spec *infrav1alpha1.NetworkRuntimeConfig) *apiconfig.NetworkRuntimeConfig {
	cfg := &apiconfig.NetworkRuntimeConfig{}
	if spec == nil {
		return cfg
	}

	cfg.LogLevel = spec.LogLevel
	cfg.NodeName = spec.NodeName
	cfg.EgressAuthResolverURL = spec.EgressAuthResolverURL
	cfg.EgressAuthEnabled = spec.EgressAuthEnabled
	cfg.EgressAuthResolverTimeout = spec.EgressAuthResolverTimeout
	cfg.EgressAuthFailurePolicy = spec.EgressAuthFailurePolicy
	cfg.MITMLeafTTL = spec.MITMLeafTTL
	cfg.ResyncPeriod = spec.ResyncPeriod
	cfg.MetricsPort = spec.MetricsPort
	cfg.HealthPort = spec.HealthPort
	cfg.FailClosed = spec.FailClosed
	cfg.PreferNFT = cloneBoolPointer(spec.PreferNFT)
	cfg.BurstRatio = spec.BurstRatio
	cfg.ProxyListenAddr = spec.ProxyListenAddr
	cfg.ProxyHTTPPort = spec.ProxyHTTPPort
	cfg.ProxyHTTPSPort = spec.ProxyHTTPSPort
	cfg.ProxyHeaderLimit = spec.ProxyHeaderLimit
	cfg.ProxyUpstreamTimeout = spec.ProxyUpstreamTimeout
	cfg.EgressBandwidthBytesPerSecond = spec.EgressBandwidthBytesPerSecond
	cfg.IngressBandwidthBytesPerSecond = spec.IngressBandwidthBytesPerSecond
	cfg.BandwidthBurstBytes = spec.BandwidthBurstBytes
	cfg.TeamEgressBandwidthBytesPerSecond = spec.TeamEgressBandwidthBytesPerSecond
	cfg.TeamIngressBandwidthBytesPerSecond = spec.TeamIngressBandwidthBytesPerSecond
	cfg.TeamBandwidthBurstBytes = spec.TeamBandwidthBurstBytes
	cfg.DNSPort = spec.DNSPort
	cfg.PlatformAllowedCIDRs = cloneStrings(spec.PlatformAllowedCIDRs)
	cfg.PlatformDeniedCIDRs = cloneStrings(spec.PlatformDeniedCIDRs)
	cfg.PlatformAllowedDomains = cloneStrings(spec.PlatformAllowedDomains)
	cfg.PlatformDeniedDomains = cloneStrings(spec.PlatformDeniedDomains)
	cfg.UseEBPF = spec.UseEBPF
	cfg.BPFFSPath = spec.BPFFSPath
	cfg.BPFPinPath = spec.BPFPinPath
	cfg.UseEDT = spec.UseEDT
	cfg.EDTHorizon = spec.EDTHorizon
	cfg.VethPrefix = spec.VethPrefix
	cfg.MetricsReportInterval = spec.MetricsReportInterval
	cfg.MeteringReportInterval = spec.MeteringReportInterval
	cfg.AuditLogPath = spec.AuditLogPath
	cfg.AuditLogMaxBytes = spec.AuditLogMaxBytes
	cfg.AuditLogMaxBackups = spec.AuditLogMaxBackups
	cfg.ShutdownDelay = spec.ShutdownDelay
	return cfg
}

func cloneBoolPointer(src *bool) *bool {
	if src == nil {
		return nil
	}
	value := *src
	return &value
}
