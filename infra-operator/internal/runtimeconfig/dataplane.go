package runtimeconfig

import (
	apiconfig "github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	infrav1alpha1 "github.com/sandbox0-ai/sandbox0/infra-operator/api/v1alpha1"
)

func ToManager(spec *infrav1alpha1.ManagerConfig) *apiconfig.ManagerConfig {
	cfg := &apiconfig.ManagerConfig{}
	if spec == nil {
		return cfg
	}

	cfg.HTTPPort = spec.HTTPPort
	cfg.KubeConfig = spec.KubeConfig
	cfg.LeaderElection = spec.LeaderElection
	cfg.ResyncPeriod = spec.ResyncPeriod
	cfg.DatabaseMaxConns = spec.DatabaseMaxConns
	cfg.DatabaseMinConns = spec.DatabaseMinConns
	cfg.CleanupInterval = spec.CleanupInterval
	cfg.LogLevel = spec.LogLevel
	cfg.MetricsPort = spec.MetricsPort
	cfg.WebhookPort = spec.WebhookPort
	cfg.WebhookCertPath = spec.WebhookCertPath
	cfg.WebhookKeyPath = spec.WebhookKeyPath
	cfg.DefaultSandboxTTL = spec.DefaultSandboxTTL
	cfg.TeamTemplateMemoryPerCPU = spec.TeamTemplateMemoryPerCPU
	cfg.SandboxRuntimeClassName = spec.SandboxRuntimeClassName
	cfg.NetdPolicyApplyTimeout = spec.NetdPolicyApplyTimeout
	cfg.NetdPolicyApplyPollInterval = spec.NetdPolicyApplyPollInterval
	cfg.PauseMinMemoryRequest = spec.PauseMinMemoryRequest
	cfg.PauseMinMemoryLimit = spec.PauseMinMemoryLimit
	cfg.PauseMemoryBufferRatio = spec.PauseMemoryBufferRatio
	cfg.PauseMinCPU = spec.PauseMinCPU
	cfg.ProcdClientTimeout = spec.ProcdClientTimeout
	cfg.ProcdInitTimeout = spec.ProcdInitTimeout
	cfg.ShutdownTimeout = spec.ShutdownTimeout
	cfg.ProcdConfig = apiconfig.ProcdConfig{
		HTTPPort:               spec.ProcdConfig.HTTPPort,
		LogLevel:               spec.ProcdConfig.LogLevel,
		JuiceFSCacheSize:       spec.ProcdConfig.JuiceFSCacheSize,
		JuiceFSPrefetch:        spec.ProcdConfig.JuiceFSPrefetch,
		JuiceFSBufferSize:      spec.ProcdConfig.JuiceFSBufferSize,
		JuiceFSWriteback:       spec.ProcdConfig.JuiceFSWriteback,
		RootPath:               spec.ProcdConfig.RootPath,
		CacheMaxBytes:          spec.ProcdConfig.CacheMaxBytes,
		CacheTTL:               spec.ProcdConfig.CacheTTL,
		ContextCleanupInterval: spec.ProcdConfig.ContextCleanupInterval,
		ContextIdleTimeout:     spec.ProcdConfig.ContextIdleTimeout,
		ContextMaxLifetime:     spec.ProcdConfig.ContextMaxLifetime,
		ContextFinishedTTL:     spec.ProcdConfig.ContextFinishedTTL,
		WebhookQueueSize:       spec.ProcdConfig.WebhookQueueSize,
		WebhookRequestTimeout:  spec.ProcdConfig.WebhookRequestTimeout,
		WebhookMaxRetries:      spec.ProcdConfig.WebhookMaxRetries,
		WebhookBaseBackoff:     spec.ProcdConfig.WebhookBaseBackoff,
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

func ToStorageProxy(spec *infrav1alpha1.StorageProxyConfig) *apiconfig.StorageProxyConfig {
	cfg := &apiconfig.StorageProxyConfig{}
	if spec == nil {
		return cfg
	}

	cfg.GRPCAddr = spec.GRPCAddr
	cfg.GRPCPort = spec.GRPCPort
	cfg.HTTPAddr = spec.HTTPAddr
	cfg.HTTPPort = spec.HTTPPort
	cfg.DatabaseMaxConns = spec.DatabaseMaxConns
	cfg.DatabaseMinConns = spec.DatabaseMinConns
	cfg.DatabaseSchema = spec.DatabaseSchema
	cfg.JuiceFSName = spec.JuiceFSName
	cfg.JuiceFSBlockSize = spec.JuiceFSBlockSize
	cfg.JuiceFSCompression = spec.JuiceFSCompression
	cfg.JuiceFSTrashDays = spec.JuiceFSTrashDays
	cfg.JuiceFSMetaRetries = spec.JuiceFSMetaRetries
	cfg.JuiceFSMaxUpload = spec.JuiceFSMaxUpload
	cfg.JuiceFSEncryptionEnabled = spec.JuiceFSEncryptionEnabled
	cfg.JuiceFSEncryptionPassphrase = spec.JuiceFSEncryptionPassphrase
	cfg.JuiceFSEncryptionAlgo = spec.JuiceFSEncryptionAlgo
	cfg.JuiceFSAttrTimeout = spec.JuiceFSAttrTimeout
	cfg.JuiceFSEntryTimeout = spec.JuiceFSEntryTimeout
	cfg.JuiceFSDirEntryTimeout = spec.JuiceFSDirEntryTimeout
	cfg.HeartbeatInterval = spec.HeartbeatInterval
	cfg.HeartbeatTimeout = spec.HeartbeatTimeout
	cfg.FlushTimeout = spec.FlushTimeout
	cfg.CleanupInterval = spec.CleanupInterval
	cfg.DirectVolumeFileIdleTTL = spec.DirectVolumeFileIdleTTL
	cfg.DefaultCacheSize = spec.DefaultCacheSize
	cfg.CacheDir = spec.CacheDir
	cfg.MetricsEnabled = spec.MetricsEnabled
	cfg.MetricsPort = spec.MetricsPort
	cfg.LogLevel = spec.LogLevel
	cfg.AuditLog = spec.AuditLog
	cfg.AuditFile = spec.AuditFile
	cfg.HTTPReadTimeout = spec.HTTPReadTimeout
	cfg.HTTPWriteTimeout = spec.HTTPWriteTimeout
	cfg.HTTPIdleTimeout = spec.HTTPIdleTimeout
	cfg.MaxOpsPerSecond = spec.MaxOpsPerSecond
	cfg.MaxBytesPerSecond = spec.MaxBytesPerSecond
	cfg.WatchEventsEnabled = spec.WatchEventsEnabled
	cfg.WatchEventQueueSize = spec.WatchEventQueueSize
	cfg.SyncCompactionInterval = spec.SyncCompactionInterval
	cfg.SyncJournalRetainEntries = spec.SyncJournalRetainEntries
	cfg.SyncRequestRetention = spec.SyncRequestRetention
	cfg.RestoreRemountTimeout = spec.RestoreRemountTimeout
	cfg.KubeconfigPath = spec.KubeconfigPath
	return cfg
}

func ToNetd(spec *infrav1alpha1.NetdConfig) *apiconfig.NetdConfig {
	cfg := &apiconfig.NetdConfig{}
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
