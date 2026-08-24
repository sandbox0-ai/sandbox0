package main

import (
	"github.com/sandbox0-ai/sandbox0/manager/pkg/networkpolicy"
	"go.uber.org/zap"
)

type managerNetworkComponents struct {
	policyService *networkpolicy.NetworkPolicyService
}

func buildNomadManagerNetworkComponents(logger *zap.Logger) managerNetworkComponents {
	return managerNetworkComponents{
		policyService: networkpolicy.NewNetworkPolicyService(logger),
	}
}
