package nodepoollifecycle

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

const testCarrierNode = "11111111-1111-1111-1111-111111111111"

func TestCarrierPlansAreCASFencedAndPreserveRuntimeFields(t *testing.T) {
	groups := []map[string]any{}
	for i := 0; i < 10; i++ {
		constraints := []carrierConstraint{}
		if i >= 8 {
			constraints = append(constraints, carrierConstraint{LTarget: "${meta.sandbox0_standard_carriers}", Operand: ">=", RTarget: fmt.Sprint(i - 1)},
				carrierConstraint{LTarget: carrierNodeAttribute, Operand: "set_contains_any", RTarget: carrierEmptyNodes})
		}
		groups = append(groups, map[string]any{"Name": fmt.Sprintf("warm-%d", i), "Constraints": constraints, "UnrelatedRuntimeField": map[string]string{"preserve": "exact"}})
	}
	current := map[string]any{"ID": "warm", "Type": "system", "NodePool": "sandbox0", "Namespace": "default", "JobModifyIndex": 1,
		"Meta": map[string]string{"sandbox0_adaptive_carriers": "v1", "operator": "preserved"}, "TaskGroups": groups, "UnrelatedJobField": "preserved"}
	posts := 0
	conflict := false
	client := newInventoryTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/jobs":
			require.NoError(t, json.NewEncoder(w).Encode([]map[string]any{{"ID": "warm"}}))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/job/warm":
			require.NoError(t, json.NewEncoder(w).Encode(current))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/node/"+testCarrierNode:
			require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"ID": testCarrierNode, "NodePool": "sandbox0", "Meta": map[string]string{"sandbox0_dedicated": "true", "sandbox0_standard_carriers": "126"}}))
		case r.Method == http.MethodPost && r.URL.Path == "/v1/jobs":
			var request struct {
				Job            map[string]any
				EnforceIndex   bool
				JobModifyIndex uint64
			}
			require.NoError(t, json.NewDecoder(r.Body).Decode(&request))
			require.True(t, request.EnforceIndex)
			if conflict {
				w.WriteHeader(http.StatusConflict)
				return
			}
			encoded, _ := json.Marshal(current["JobModifyIndex"])
			var expected uint64
			require.NoError(t, json.Unmarshal(encoded, &expected))
			require.Equal(t, expected, request.JobModifyIndex)
			posts++
			current = request.Job
			current["JobModifyIndex"] = expected + 1
			require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"EvalID": "test"}))
		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
			http.NotFound(w, r)
		}
	})
	allowed := []string{}
	for i := 0; i < 9; i++ {
		allowed = append(allowed, fmt.Sprintf("warm-%d", i))
	}
	require.NoError(t, client.ApplyCarrierPlan(t.Context(), testCarrierNode, 10, allowed))
	require.Equal(t, 1, posts)
	require.NoError(t, client.ApplyCarrierPlan(t.Context(), testCarrierNode, 10, allowed))
	require.Equal(t, 1, posts, "lost response retry is a no-op")
	require.ErrorContains(t, client.ApplyCarrierPlan(t.Context(), testCarrierNode, 9, allowed), "stale")
	require.ErrorContains(t, client.ApplyCarrierPlan(t.Context(), testCarrierNode, 10, append(allowed, "warm-9")), "conflicting intent")
	require.Equal(t, "preserved", current["UnrelatedJobField"])
	require.Equal(t, "preserved", current["Meta"].(map[string]any)["operator"])
	for _, raw := range current["TaskGroups"].([]any) {
		require.Equal(t, "exact", raw.(map[string]any)["UnrelatedRuntimeField"].(map[string]any)["preserve"])
	}
	conflict = true
	require.ErrorContains(t, client.ApplyCarrierPlan(t.Context(), testCarrierNode, 11, append(allowed, "warm-9")), "HTTP 409")
	require.Equal(t, 1, posts, "a concurrent Nomad update must never be overwritten")
}

func TestCarrierMembersRejectInvalidOrUnboundedIdentity(t *testing.T) {
	for _, raw := range []string{"", "node", testCarrierNode + "," + testCarrierNode, " " + testCarrierNode} {
		_, err := carrierMembers(raw)
		require.Error(t, err)
	}
	members, err := carrierMembers(carrierEmptyNodes)
	require.NoError(t, err)
	require.Empty(t, members)
}
