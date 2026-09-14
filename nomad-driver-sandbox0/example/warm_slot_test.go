package example

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/hashicorp/nomad/jobspec2"
	"github.com/stretchr/testify/require"
)

func TestWarmCarrierSizingPreservesDefaultIdentities(t *testing.T) {
	body, err := os.ReadFile("warm-slot.nomad")
	require.NoError(t, err)
	for _, profile := range []struct{ standard, privileged int }{{6, 2}, {100, 2}, {500, 2}, {6, 4}} {
		t.Run(fmt.Sprintf("%d/%d", profile.standard, profile.privileged), func(t *testing.T) {
			count := profile.standard
			job, err := jobspec2.ParseWithConfig(&jobspec2.ParseConfig{
				Path: "warm-slot.nomad", Body: body, Strict: true,
				ArgVars: []string{"datacenter=ali_ue1", fmt.Sprintf("standard_slots=%d", count), fmt.Sprintf("privileged_slots=%d", profile.privileged)},
			})
			require.NoError(t, err)
			job.Canonicalize()
			require.Equal(t, "system", *job.Type)
			require.Equal(t, "sandbox0", *job.NodePool)
			require.Equal(t, []string{"ali_ue1"}, job.Datacenters)
			require.Len(t, job.TaskGroups, count+profile.privileged)
			classes := make(map[string]string)
			for _, group := range job.TaskGroups {
				require.Equal(t, 1, *group.Count)
				require.Equal(t, 0, *group.RestartPolicy.Attempts)
				require.Equal(t, "fail", *group.RestartPolicy.Mode)
				require.Len(t, group.Networks, 1)
				require.Equal(t, "cni/sandbox0", group.Networks[0].Mode)
				require.Len(t, group.Tasks, 1)
				task := group.Tasks[0]
				require.Equal(t, "sandbox0-gvisor", task.Driver)
				require.Equal(t, "/procd", task.Config["command"])
				require.Equal(t, 50, *task.Resources.CPU)
				require.Equal(t, 64, *task.Resources.MemoryMB)
				_, duplicate := classes[*group.Name]
				require.False(t, duplicate)
				classes[*group.Name] = task.Config["security_class"].(string)
				var label int
				if strings.HasPrefix(*group.Name, "warm-") {
					_, err = fmt.Sscanf(*group.Name, "warm-%d", &label)
				} else {
					_, err = fmt.Sscanf(*group.Name, "privileged-%d", &label)
				}
				require.NoError(t, err)
				if strings.HasPrefix(*group.Name, "warm-") && label < 8 {
					require.Empty(t, group.Constraints, "default carrier jobs must remain unchanged")
				} else {
					require.Len(t, group.Constraints, 1)
					class := classes[*group.Name]
					require.Equal(t, "${meta.sandbox0_"+class+"_carriers}", group.Constraints[0].LTarget)
					require.Equal(t, ">=", group.Constraints[0].Operand)
					ordinal := label + 1
					if class == "standard" {
						ordinal -= 2
					}
					require.Equal(t, fmt.Sprint(ordinal), group.Constraints[0].RTarget)
				}
			}
			for index := range count {
				label := index
				if index >= 6 {
					label += 2
				}
				require.Equal(t, "standard", classes[fmt.Sprintf("warm-%d", label)])
			}
			require.Equal(t, "privileged", classes["warm-6"])
			require.Equal(t, "privileged", classes["warm-7"])
			for index := 2; index < profile.privileged; index++ {
				require.Equal(t, "privileged", classes[fmt.Sprintf("privileged-%d", index)])
			}
			require.Len(t, job.Constraints, 2)
			for _, constraint := range job.Constraints {
				require.Contains(t, []string{"${meta.sandbox0_dedicated}", "${meta.sandbox0_admitted}"}, constraint.LTarget)
				require.Equal(t, "true", constraint.RTarget)
			}
		})
	}
	for _, variable := range []string{"standard_slots=-1", "standard_slots=1.5", "standard_slots=513", "privileged_slots=65"} {
		_, err := jobspec2.ParseWithConfig(&jobspec2.ParseConfig{Path: "warm-slot.nomad", Body: body, Strict: true, ArgVars: []string{variable}})
		require.Error(t, err, variable)
	}
}
