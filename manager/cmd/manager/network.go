package main

import (
	"strings"

	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/namespacepolicy"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/network"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/networkpolicy"
	"go.uber.org/zap"
	coreinformers "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
	networkinglisters "k8s.io/client-go/listers/networking/v1"
)

type managerNetworkComponents struct {
	policyService   *networkpolicy.NetworkPolicyService
	namespacePolicy namespacepolicy.TemplateNamespaceReconciler
	provider        network.Provider
}

func buildManagerNetworkComponents(
	cfg *config.ManagerConfig,
	k8sClient kubernetes.Interface,
	podInformer coreinformers.PodInformer,
	podLister corelisters.PodLister,
	networkPolicyLister networkinglisters.NetworkPolicyLister,
	logger *zap.Logger,
) managerNetworkComponents {
	components := managerNetworkComponents{
		policyService: networkpolicy.NewNetworkPolicyService(logger),
		provider:      network.NewNoopProvider(),
	}
	baseline, err := namespacepolicy.NewReconciler(k8sClient, networkPolicyLister, namespacepolicy.Config{
		SystemNamespace: cfg.NetworkMITMCASecretNamespace,
		ProcdPort:       cfg.ProcdConfig.HTTPPort,
	}, logger)
	if err != nil {
		logger.Warn("Template namespace ingress baseline disabled", zap.Error(err))
	} else {
		components.namespacePolicy = baseline
		logger.Info("Template namespace ingress baseline enabled",
			zap.String("systemNamespace", cfg.NetworkMITMCASecretNamespace),
			zap.Int("procdPort", cfg.ProcdConfig.HTTPPort),
		)
	}

	switch strings.TrimSpace(strings.ToLower(cfg.NetworkPolicyProvider)) {
	case "", "noop":
		logger.Info("Network provider set to noop")
	case "ctld":
		components.provider = network.NewCtldProvider(podInformer, podLister, network.CtldProviderConfig{
			ApplyTimeout: cfg.NetworkPolicyApplyTimeout.Duration,
			PollInterval: cfg.NetworkPolicyApplyPollInterval.Duration,
		}, logger)
		logger.Info("Network provider set to ctld network runtime",
			zap.Duration("applyTimeout", cfg.NetworkPolicyApplyTimeout.Duration),
			zap.Duration("pollInterval", cfg.NetworkPolicyApplyPollInterval.Duration),
		)
	default:
		logger.Warn("Unknown network policy provider, falling back to noop",
			zap.String("provider", cfg.NetworkPolicyProvider),
		)
	}
	return components
}
