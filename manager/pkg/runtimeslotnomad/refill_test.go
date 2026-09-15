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
			stops := 0
			server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				require.Equal(t, "test-token", r.Header.Get("X-Nomad-Token"))
				if strings.HasSuffix(r.URL.Path, "/stop") {
					stops++
					require.Equal(t, http.MethodPost, r.Method)
					require.Equal(t, "/v1/allocation/"+testHTTPAllocationID+"/stop", r.URL.Path)
					require.Equal(t, "false", r.URL.Query().Get("reschedule"))
					require.Equal(t, "true", r.URL.Query().Get("no_shutdown_delay"))
					require.True(t, strings.HasPrefix(r.URL.Query().Get("idempotency_token"), "carrier-refill-"))
					w.WriteHeader(http.StatusOK)
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
				require.Equal(t, 1, stops)
				require.Equal(t, 1, count)
			} else {
				require.Zero(t, stops)
				require.Zero(t, count)
			}
		})
	}
}

func TestRefillRetriesUncertainStopWithSameOperationAndCursor(t *testing.T) {
	server, resolver, _ := newNomadMTLSTestServer(t, &nomadTestServerState{token: "test-token"})
	defer server.Close()
	var operations []string
	server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/stop") {
			operations = append(operations, r.URL.Query().Get("idempotency_token"))
			if len(operations) == 1 {
				w.WriteHeader(http.StatusServiceUnavailable)
			}
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
