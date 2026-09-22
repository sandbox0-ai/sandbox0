package nodepoollifecycle

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/carrierpool"
	"github.com/sandbox0-ai/sandbox0/pkg/nomadinventory"
)

// BenchmarkCarrierPlanRetry exercises the full 18-shard family with roughly
// 64 KiB jobs, as seen in production. This measures HTTP and serialization
// overhead of a pending resize retry, not allocation startup or claim latency.
func BenchmarkCarrierPlanRetry(b *testing.B) {
	jobs := make(map[string]map[string]any)
	catalog := make([]map[string]any, 0, nomadinventory.WarmJobShardCount)
	for shard := range nomadinventory.WarmJobShardCount {
		id, err := nomadinventory.WarmJobID("warm", shard)
		if err != nil {
			b.Fatal(err)
		}
		groups := make([]map[string]any, 0, nomadinventory.WarmJobMaxGroups)
		for offset := range nomadinventory.WarmJobMaxGroups {
			ordinal := shard*nomadinventory.WarmJobMaxGroups + offset
			name := fmt.Sprintf("warm-%d", ordinal)
			if ordinal >= 514 {
				name = fmt.Sprintf("privileged-%d", ordinal-512)
			}
			class, index, err := carrierpool.GroupIndex(name)
			if err != nil {
				b.Fatal(err)
			}
			constraints := []carrierConstraint{}
			if ordinal >= 8 {
				constraints = append(constraints,
					carrierConstraint{LTarget: "${meta.sandbox0_" + class + "_carriers}", Operand: ">=", RTarget: fmt.Sprint(index + 1)},
					carrierConstraint{LTarget: carrierNodeAttribute, Operand: "set_contains_any", RTarget: carrierEmptyNodes})
			}
			groups = append(groups, map[string]any{"Name": name, "Constraints": constraints, "RuntimePayload": strings.Repeat("x", 1728)})
		}
		jobs[id] = map[string]any{"ID": id, "Type": "system", "NodePool": "sandbox0", "Namespace": "default", "JobModifyIndex": 1,
			"Meta": map[string]string{"sandbox0_adaptive_carriers": "v1"}, "TaskGroups": groups}
		catalog = append(catalog, map[string]any{"ID": id})
	}
	client := newInventoryTestClient(b, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			b.Errorf("unexpected write: %s", r.URL.Path)
			http.Error(w, "unexpected write", 500)
			return
		}
		var value any
		if r.URL.Path == "/v1/jobs" {
			value = catalog
		} else {
			value = jobs[strings.TrimPrefix(r.URL.Path, "/v1/job/")]
		}
		if err := json.NewEncoder(w).Encode(value); err != nil {
			b.Error(err)
		}
	})
	allowed := make([]string, 8)
	for i := range allowed {
		allowed[i] = fmt.Sprintf("warm-%d", i)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := client.ApplyCarrierPlan(b.Context(), testCarrierNode, 1, allowed); err != nil {
			b.Fatal(err)
		}
	}
}
