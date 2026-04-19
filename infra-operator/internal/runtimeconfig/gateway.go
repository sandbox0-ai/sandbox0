package runtimeconfig

import (
	apiconfig "github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	infrav1alpha1 "github.com/sandbox0-ai/sandbox0/infra-operator/api/v1alpha1"
)

func ToGlobalGateway(spec *infrav1alpha1.GlobalGatewayConfig) *apiconfig.GlobalGatewayConfig {
	cfg := &apiconfig.GlobalGatewayConfig{}
	if spec == nil {
		return cfg
	}

	cfg.HTTPPort = spec.HTTPPort
	cfg.LogLevel = spec.LogLevel
	cfg.DatabaseMaxConns = spec.DatabaseMaxConns
	cfg.DatabaseMinConns = spec.DatabaseMinConns
	cfg.DatabaseSchema = spec.DatabaseSchema
	cfg.ShutdownTimeout = spec.ShutdownTimeout
	cfg.ServerReadTimeout = spec.ServerReadTimeout
	cfg.ServerWriteTimeout = spec.ServerWriteTimeout
	cfg.ServerIdleTimeout = spec.ServerIdleTimeout
	applyGatewayConfig(&cfg.GatewayConfig, spec.GatewayConfig)
	return cfg
}

func ToRegionalGateway(spec *infrav1alpha1.RegionalGatewayConfig) *apiconfig.RegionalGatewayConfig {
	cfg := &apiconfig.RegionalGatewayConfig{}
	if spec == nil {
		return cfg
	}

	cfg.Edition = spec.Edition
	cfg.AuthMode = spec.AuthMode
	cfg.HTTPPort = spec.HTTPPort
	cfg.LogLevel = spec.LogLevel
	cfg.DatabaseMaxConns = spec.DatabaseMaxConns
	cfg.DatabaseMinConns = spec.DatabaseMinConns
	cfg.SchedulerEnabled = spec.SchedulerEnabled
	cfg.SchedulerURL = spec.SchedulerURL
	cfg.InternalAuthTTL = spec.InternalAuthTTL
	cfg.InternalAuthCaller = spec.InternalAuthCaller
	cfg.ClusterCacheTTL = spec.ClusterCacheTTL
	cfg.ProxyTimeout = spec.ProxyTimeout
	cfg.ShutdownTimeout = spec.ShutdownTimeout
	cfg.ServerReadTimeout = spec.ServerReadTimeout
	cfg.ServerWriteTimeout = spec.ServerWriteTimeout
	cfg.ServerIdleTimeout = spec.ServerIdleTimeout
	applyGatewayConfig(&cfg.GatewayConfig, spec.GatewayConfig)
	return cfg
}

func ToSSHGateway(spec *infrav1alpha1.SSHGatewayConfig) *apiconfig.SSHGatewayConfig {
	cfg := &apiconfig.SSHGatewayConfig{}
	if spec == nil {
		return cfg
	}

	cfg.SSHPort = spec.SSHPort
	cfg.LogLevel = spec.LogLevel
	cfg.DatabaseMaxConns = spec.DatabaseMaxConns
	cfg.DatabaseMinConns = spec.DatabaseMinConns
	cfg.InternalAuthTTL = spec.InternalAuthTTL
	cfg.InternalAuthCaller = spec.InternalAuthCaller
	cfg.ResumeTimeout = spec.ResumeTimeout
	cfg.ResumePollInterval = spec.ResumePollInterval
	cfg.ShutdownTimeout = spec.ShutdownTimeout
	return cfg
}

func ToScheduler(spec *infrav1alpha1.SchedulerConfig) *apiconfig.SchedulerConfig {
	cfg := &apiconfig.SchedulerConfig{}
	if spec == nil {
		return cfg
	}

	cfg.HTTPPort = spec.HTTPPort
	cfg.LogLevel = spec.LogLevel
	cfg.ReconcileInterval = spec.ReconcileInterval
	cfg.PodsPerNode = spec.PodsPerNode
	cfg.ShutdownTimeout = spec.ShutdownTimeout
	cfg.ReadTimeout = spec.ReadTimeout
	cfg.WriteTimeout = spec.WriteTimeout
	cfg.IdleTimeout = spec.IdleTimeout
	cfg.ProxyTimeout = spec.ProxyTimeout
	cfg.DatabasePool = apiconfig.DatabasePoolConfig{
		MaxConns:        spec.DatabasePool.MaxConns,
		MinConns:        spec.DatabasePool.MinConns,
		MaxConnLifetime: spec.DatabasePool.MaxConnLifetime,
		MaxConnIdleTime: spec.DatabasePool.MaxConnIdleTime,
	}
	return cfg
}

func ToClusterGateway(spec *infrav1alpha1.ClusterGatewayConfig) *apiconfig.ClusterGatewayConfig {
	cfg := &apiconfig.ClusterGatewayConfig{}
	if spec == nil {
		return cfg
	}

	cfg.HTTPPort = spec.HTTPPort
	cfg.LogLevel = spec.LogLevel
	cfg.AuthMode = spec.AuthMode
	cfg.AllowedCallers = cloneStrings(spec.AllowedCallers)
	cfg.ShutdownTimeout = spec.ShutdownTimeout
	cfg.HealthCheckPeriod = spec.HealthCheckPeriod
	cfg.ProxyTimeout = spec.ProxyTimeout
	cfg.DatabaseMaxConns = spec.DatabaseMaxConns
	cfg.DatabaseMinConns = spec.DatabaseMinConns
	cfg.SchedulerPermissions = cloneStrings(spec.SchedulerPermissions)
	applyGatewayConfig(&cfg.GatewayConfig, spec.GatewayConfig)
	return cfg
}

func applyGatewayConfig(dst *apiconfig.GatewayConfig, src infrav1alpha1.GatewayConfig) {
	if dst == nil {
		return
	}

	dst.JWTIssuer = src.JWTIssuer
	dst.JWTPrivateKeyPEM = src.JWTPrivateKeyPEM
	dst.JWTPublicKeyPEM = src.JWTPublicKeyPEM
	dst.JWTPrivateKeyFile = src.JWTPrivateKeyFile
	dst.JWTPublicKeyFile = src.JWTPublicKeyFile
	dst.JWTAccessTokenTTL = src.JWTAccessTokenTTL
	dst.JWTRefreshTokenTTL = src.JWTRefreshTokenTTL
	dst.RateLimitRPS = src.RateLimitRPS
	dst.RateLimitBurst = src.RateLimitBurst
	dst.RateLimitCleanupInterval = src.RateLimitCleanupInterval
	dst.DefaultTeamName = src.DefaultTeamName
	dst.BuiltInAuth = apiconfig.BuiltInAuthConfig{
		Enabled:                   src.BuiltInAuth.Enabled,
		AllowRegistration:         src.BuiltInAuth.AllowRegistration,
		EmailVerificationRequired: src.BuiltInAuth.EmailVerificationRequired,
		AdminOnly:                 src.BuiltInAuth.AdminOnly,
	}
	dst.OIDCProviders = make([]apiconfig.OIDCProviderConfig, 0, len(src.OIDCProviders))
	for _, provider := range src.OIDCProviders {
		var teamMapping *apiconfig.TeamMappingConfig
		if provider.TeamMapping != nil {
			teamMapping = &apiconfig.TeamMappingConfig{
				Domain:        provider.TeamMapping.Domain,
				DefaultRole:   provider.TeamMapping.DefaultRole,
				DefaultTeamID: provider.TeamMapping.DefaultTeamID,
			}
		}
		dst.OIDCProviders = append(dst.OIDCProviders, apiconfig.OIDCProviderConfig{
			ID:                      provider.ID,
			Name:                    provider.Name,
			Enabled:                 provider.Enabled,
			ClientID:                provider.ClientID,
			ClientSecret:            provider.ClientSecret,
			DiscoveryURL:            provider.DiscoveryURL,
			TokenEndpointAuthMethod: provider.TokenEndpointAuthMethod,
			Scopes:                  cloneStrings(provider.Scopes),
			AutoProvision:           provider.AutoProvision,
			TeamMapping:             teamMapping,
			ExternalAuthPortalURL:   provider.ExternalAuthPortalURL,
		})
	}
	dst.OIDCStateTTL = src.OIDCStateTTL
	dst.OIDCStateCleanupInterval = src.OIDCStateCleanupInterval
	dst.BaseURL = src.BaseURL
}

func cloneStrings(src []string) []string {
	if len(src) == 0 {
		return nil
	}
	cloned := make([]string, len(src))
	copy(cloned, src)
	return cloned
}
