package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestProcessOutputLogsEnabledDefaultsOn(t *testing.T) {
	t.Setenv(processOutputLogsEnvVar, "")

	enabled, err := processOutputLogsEnabled()
	require.NoError(t, err)
	require.True(t, enabled)
}

func TestProcessOutputLogsEnabledAllowsOptOut(t *testing.T) {
	t.Setenv(processOutputLogsEnvVar, "false")

	enabled, err := processOutputLogsEnabled()
	require.NoError(t, err)
	require.False(t, enabled)
}

func TestProcessOutputLogsEnabledRejectsInvalidValues(t *testing.T) {
	t.Setenv(processOutputLogsEnvVar, "disabled")

	_, err := processOutputLogsEnabled()
	require.Error(t, err)
}
