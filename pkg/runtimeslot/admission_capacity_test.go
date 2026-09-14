package runtimeslot

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNodeAdmissionBudgetsPreservePhysicalCapacityAndLegacyWire(t *testing.T) {
	capacity := NodeChannelCapacity{CPUMillicores: 14000, MemoryBytes: 56 << 30,
		CPUSetCPUs: "2-15", CPUSetMems: "0", TTLMilliseconds: 90000}
	require.NoError(t, capacity.Validate())
	cpu, memory := capacity.AdmissionLimits()
	require.Equal(t, capacity.CPUMillicores, cpu)
	require.Equal(t, capacity.MemoryBytes, memory)
	encoded, err := json.Marshal(capacity)
	require.NoError(t, err)
	require.NotContains(t, string(encoded), "admission_")
	capacity.AdmissionCPUMillicores, capacity.AdmissionMemoryBytes = 35000, 70<<30
	require.NoError(t, capacity.Validate())
	encoded, err = json.Marshal(capacity)
	require.NoError(t, err)
	var restored NodeChannelCapacity
	require.NoError(t, json.Unmarshal(encoded, &restored))
	require.Equal(t, capacity, restored)
	cpu, memory = restored.AdmissionLimits()
	require.Equal(t, int64(35000), cpu)
	require.Equal(t, int64(70<<30), memory)
	require.Equal(t, int64(14000), restored.CPUMillicores)
	require.Equal(t, int64(56<<30), restored.MemoryBytes)
	for _, invalid := range []NodeChannelCapacity{
		{CPUMillicores: 14000, MemoryBytes: 56 << 30, AdmissionCPUMillicores: -1},
		{CPUMillicores: 14000, MemoryBytes: 56 << 30, AdmissionCPUMillicores: 13999},
		{CPUMillicores: 14000, MemoryBytes: 56 << 30, AdmissionCPUMillicores: 224001},
		{CPUMillicores: 14000, MemoryBytes: 56 << 30, AdmissionMemoryBytes: -1},
		{CPUMillicores: 14000, MemoryBytes: 56 << 30, AdmissionMemoryBytes: 55 << 30},
		{CPUMillicores: 14000, MemoryBytes: 56 << 30, AdmissionMemoryBytes: (112 << 30) + 1},
	} {
		invalid.CPUSetCPUs, invalid.CPUSetMems, invalid.TTLMilliseconds = "2-15", "0", 90000
		require.Error(t, invalid.Validate())
	}
}
