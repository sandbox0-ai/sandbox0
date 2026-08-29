package main

import (
	"testing"

	"github.com/sandbox0-ai/sandbox0/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestConfigureNodePoolAutoscalerDisabled(t *testing.T) {
	worker, err := configureNodePoolAutoscaler(&config.ManagerConfig{}, nil)
	require.NoError(t, err)
	require.Nil(t, worker)
}

func TestConfigureNodePoolAutoscalerRejectsNonAliyunProviderBeforeCloudAccess(t *testing.T) {
	worker, err := configureNodePoolAutoscaler(&config.ManagerConfig{
		NodePoolAutoscaler: config.NodePoolAutoscalerConfig{Enabled: true, Provider: "aws"},
	}, nil)
	require.Nil(t, worker)
	require.ErrorContains(t, err, "store is required")
}
