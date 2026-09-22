package runtimeslotnomad

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/sandbox0-ai/sandbox0/pkg/nomadinventory"
	"github.com/stretchr/testify/require"
)

func TestRefillRequestsOnlyExactFailedCarrierScheduling(t *testing.T) {
	for _, changed := range []string{"", "running", "stop", "already-replaced", "another-job", "another-node"} {
		t.Run("recheck-"+changed, func(t *testing.T) {
			server, resolver, _ := newNomadMTLSTestServer(t, &nomadTestServerState{token: "test-token"})
			defer server.Close()
			evaluations := 0
			server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				require.Equal(t, "test-token", r.Header.Get("X-Nomad-Token"))
				if strings.HasSuffix(r.URL.Path, "/evaluate") {
					evaluations++
					require.Equal(t, http.MethodPost, r.Method)
					require.Equal(t, "/v1/job/"+nomadinventory.DefaultWarmJobID+"/evaluate", r.URL.Path)
					require.Equal(t, "default", r.URL.Query().Get("namespace"))
					require.Zero(t, r.ContentLength, "never force rescheduling healthy siblings")
					_, _ = w.Write([]byte(`{"EvalID":"evaluation"}`))
					return
				}
				require.Equal(t, "/v1/allocations", r.URL.Path, "no client GC or physical cleanup endpoint is permitted")
				a := nomadinventory.Allocation{ID: testHTTPAllocationID, NodeID: "node-1", Namespace: "default", JobID: nomadinventory.DefaultWarmJobID, ClientStatus: "failed", DesiredStatus: "run"}
				if r.URL.Query().Get("prefix") != "" {
					switch changed {
					case "running":
						a.ClientStatus = "running"
					case "stop":
						a.DesiredStatus = "stop"
					case "already-replaced":
						a.NextAllocation = "replacement"
					case "another-job":
						a.JobID = "another-job"
					case "another-node":
						a.NodeID = "another-node"
					}
				}
				_ = json.NewEncoder(w).Encode([]nomadinventory.Allocation{a})
			})
			api, err := NewHTTPAPI(resolver)
			require.NoError(t, err)
			count, _, err := api.RefillFailedCarriers(t.Context(), "cluster-1", "")
			if changed == "another-job" || changed == "another-node" {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			if changed == "" {
				require.Equal(t, 1, evaluations)
				require.Equal(t, 1, count)
			} else {
				require.Zero(t, evaluations)
				require.Zero(t, count)
			}
		})
	}
}

func TestRefillRetriesUncertainEvaluationWithSameJobAndCursor(t *testing.T) {
	server, resolver, _ := newNomadMTLSTestServer(t, &nomadTestServerState{token: "test-token"})
	defer server.Close()
	var operations []string
	server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/evaluate") {
			operations = append(operations, r.URL.Path+"?"+r.URL.RawQuery)
			if len(operations) == 1 {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			_, _ = w.Write([]byte(`{"EvalID":"evaluation"}`))
			return
		}
		if r.URL.Query().Get("prefix") == "" {
			w.Header().Set("X-Nomad-NextToken", "next")
		}
		_ = json.NewEncoder(w).Encode([]nomadinventory.Allocation{{ID: testHTTPAllocationID, NodeID: "node-1", Namespace: "default", JobID: nomadinventory.DefaultWarmJobID, ClientStatus: "failed", DesiredStatus: "run"}})
	})
	api, err := NewHTTPAPI(resolver)
	require.NoError(t, err)
	_, next, err := api.RefillFailedCarriers(t.Context(), "cluster-1", "previous")
	require.Error(t, err)
	require.Equal(t, "previous", next)
	count, next, err := api.RefillFailedCarriers(t.Context(), "cluster-1", "previous")
	require.NoError(t, err)
	require.Equal(t, 1, count)
	require.Equal(t, "next", next)
	require.Len(t, operations, 2)
	require.Equal(t, operations[0], operations[1])
}
