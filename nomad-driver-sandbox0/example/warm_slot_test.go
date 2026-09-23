package example

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"github.com/hashicorp/nomad/jobspec2"
	"github.com/sandbox0-ai/sandbox0/pkg/nomadinventory"
	"github.com/stretchr/testify/require"
)

func TestPrivilegedCarrierSizingPreservesStableIdentities(t *testing.T) {
	body, err := os.ReadFile("warm-slot.nomad")
	require.NoError(t, err)
	placements := map[string]string{}
	for _, count := range []int{2, 16, 240, 256} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			groups := map[string]bool{}
			for shard := range nomadinventory.WarmJobShardCount {
				job, err := jobspec2.ParseWithConfig(&jobspec2.ParseConfig{
					Path: "warm-slot.nomad", Body: body, Strict: true,
					ArgVars: []string{"datacenter=ali_ue1", fmt.Sprintf("privileged_slots=%d", count), fmt.Sprintf("warm_shard=%d", shard)},
				})
				require.NoError(t, err)
				job.Canonicalize()
				require.Equal(t, "system", *job.Type)
				require.Equal(t, "sandbox0", *job.NodePool)
				require.Equal(t, []string{"ali_ue1"}, job.Datacenters)
				require.LessOrEqual(t, len(job.TaskGroups), nomadinventory.WarmJobMaxGroups)
				expectedID, err := nomadinventory.WarmJobID("sandbox0-warm-slots", shard)
				require.NoError(t, err)
				require.Equal(t, expectedID, *job.ID)
				encoded, err := json.Marshal(job)
				require.NoError(t, err)
				require.Less(t, len(encoded), 96<<10)
				for _, group := range job.TaskGroups {
					name := *group.Name
					require.False(t, groups[name], "duplicate carrier group")
					groups[name] = true
					if previous, exists := placements[name]; exists {
						require.Equal(t, previous, *job.ID, "resizing must not move a carrier between shards")
					}
					placements[name] = *job.ID
					require.Equal(t, 1, *group.Count)
					require.Equal(t, 0, *group.RestartPolicy.Attempts)
					require.Equal(t, "fail", *group.RestartPolicy.Mode)
					require.Len(t, group.Networks, 1)
					require.Equal(t, "cni/sandbox0", group.Networks[0].Mode)
					require.Len(t, group.Tasks, 1)
					task := group.Tasks[0]
					require.Equal(t, "sandbox0-gvisor", task.Driver)
					require.Equal(t, "/procd", task.Config["command"])
					require.Equal(t, "privileged", task.Config["security_class"])
					require.True(t, *task.LogConfig.Disabled)
					require.Equal(t, 50, *task.Resources.CPU)
					require.Equal(t, 64, *task.Resources.MemoryMB)
					if name == "warm-6" || name == "warm-7" {
						require.Empty(t, group.Constraints)
					} else {
						var index int
						_, err := fmt.Sscanf(name, "privileged-%d", &index)
						require.NoError(t, err)
						require.GreaterOrEqual(t, index, 2)
						require.Len(t, group.Constraints, 1)
						require.Equal(t, "${meta.sandbox0_privileged_carriers}", group.Constraints[0].LTarget)
						require.Equal(t, ">=", group.Constraints[0].Operand)
						require.Equal(t, fmt.Sprint(index+1), group.Constraints[0].RTarget)
					}
				}
			}
			require.Len(t, groups, count)
			require.True(t, groups["warm-6"])
			require.True(t, groups["warm-7"])
			for index := 2; index < count; index++ {
				require.True(t, groups[fmt.Sprintf("privileged-%d", index)])
			}
		})
	}
	for _, variable := range []string{"standard_slots=1", "privileged_slots=-1", "privileged_slots=1.5", "privileged_slots=257", "warm_shard=-1", "warm_shard=24", "warm_shard=1.5"} {
		_, err := jobspec2.ParseWithConfig(&jobspec2.ParseConfig{Path: "warm-slot.nomad", Body: body, Strict: true, ArgVars: []string{variable}})
		require.Error(t, err, variable)
	}
}

func TestAdaptivePrivilegedCarrierCatalogHasTwoEnrollmentAnchors(t *testing.T) {
	body, err := os.ReadFile("warm-slot.nomad")
	require.NoError(t, err)
	anchors, extra := 0, 0
	for shard := range nomadinventory.WarmJobShardCount {
		job, err := jobspec2.ParseWithConfig(&jobspec2.ParseConfig{Path: "warm-slot.nomad", Body: body, Strict: true,
			ArgVars: []string{"privileged_slots=256", "adaptive_carriers=true", fmt.Sprintf("warm_shard=%d", shard)}})
		require.NoError(t, err)
		job.Canonicalize()
		require.Equal(t, "v1", job.Meta["sandbox0_adaptive_carriers"])
		for _, group := range job.TaskGroups {
			if len(group.Constraints) == 0 {
				require.Contains(t, []string{"warm-6", "warm-7"}, *group.Name)
				anchors++
				continue
			}
			extra++
			require.Len(t, group.Constraints, 2)
			require.Equal(t, "${meta.sandbox0_privileged_carriers}", group.Constraints[0].LTarget)
			require.Equal(t, ">=", group.Constraints[0].Operand)
			require.Equal(t, "${node.unique.id}", group.Constraints[1].LTarget)
			require.Equal(t, "set_contains_any", group.Constraints[1].Operand)
			require.Equal(t, "00000000-0000-0000-0000-000000000000", group.Constraints[1].RTarget)
		}
	}
	require.Equal(t, 2, anchors)
	require.Equal(t, 254, extra)
}
