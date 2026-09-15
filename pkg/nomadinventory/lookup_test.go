package nomadinventory

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

const lookupAllocationID = "40539b8a-a8f8-95c9-2e6b-eb123a3a07c6"

func TestGetUsesExactIndexedSummaryWithoutHydratingDenseJob(t *testing.T) {
	allocation := Allocation{ID: lookupAllocationID, NodeID: "node", Namespace: "default", DesiredStatus: "run", ClientStatus: "running"}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/allocations" {
			t.Error("lookup hydrated the full job instead of reading its allocation summary")
			_, _ = w.Write([]byte(`{"Job":{"TaskGroups":[` + strings.Repeat(" ", 3<<20) + `]}}`))
			return
		}
		require.Equal(t, strings.ReplaceAll(lookupAllocationID, "-", ""), r.URL.Query().Get("prefix"))
		require.Equal(t, `ID == "`+lookupAllocationID+`"`, r.URL.Query().Get("filter"))
		require.Equal(t, "default", r.URL.Query().Get("namespace"))
		require.Equal(t, "2", r.URL.Query().Get("per_page"))
		require.Equal(t, "false", r.URL.Query().Get("resources"))
		require.Equal(t, "false", r.URL.Query().Get("task_states"))
		require.Equal(t, "token", r.Header.Get("X-Nomad-Token"))
		_ = json.NewEncoder(w).Encode([]Allocation{allocation})
	}))
	defer server.Close()
	address, err := url.Parse(server.URL)
	require.NoError(t, err)
	observed, err := Get(t.Context(), server.Client(), address, lookupAllocationID, "node", "default", http.Header{"X-Nomad-Token": {"token"}})
	require.NoError(t, err)
	require.Equal(t, &allocation, observed)
}

func TestGetNeverTreatsAmbiguousCatalogResponsesAsAbsence(t *testing.T) {
	for _, kind := range []string{"http-404", "http-403", "truncated", "oversized", "trailing-data", "pagination-empty", "duplicate-headers", "acl-filtered", "duplicate-acl-header", "wrong-id", "wrong-node", "wrong-namespace", "duplicate"} {
		t.Run(kind, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				allocation := Allocation{ID: lookupAllocationID, NodeID: "node", Namespace: "default"}
				switch kind {
				case "http-404":
					w.WriteHeader(http.StatusNotFound)
					return
				case "http-403":
					w.WriteHeader(http.StatusForbidden)
					return
				case "truncated":
					_, _ = w.Write([]byte(`[{"ID":`))
					return
				case "oversized":
					_, _ = w.Write([]byte("[" + strings.Repeat(" ", 2<<20) + "]"))
					return
				case "trailing-data":
					_, _ = w.Write([]byte("[] {}"))
					return
				case "pagination-empty":
					w.Header().Set("X-Nomad-NextToken", "more")
					_, _ = w.Write([]byte("[]"))
					return
				case "duplicate-headers":
					w.Header().Add("X-Nomad-NextToken", "")
					w.Header().Add("X-Nomad-NextToken", "more")
				case "acl-filtered":
					w.Header().Set("X-Nomad-Results-Filtered-By-ACLs", "true")
					_, _ = w.Write([]byte("[]"))
					return
				case "duplicate-acl-header":
					w.Header().Add("X-Nomad-Results-Filtered-By-ACLs", "false")
					w.Header().Add("X-Nomad-Results-Filtered-By-ACLs", "true")
				case "wrong-id":
					allocation.ID = "other"
				case "wrong-node":
					allocation.NodeID = "other"
				case "wrong-namespace":
					allocation.Namespace = "other"
				case "duplicate":
					_ = json.NewEncoder(w).Encode([]Allocation{allocation, allocation})
					return
				}
				_ = json.NewEncoder(w).Encode([]Allocation{allocation})
			}))
			defer server.Close()
			address, err := url.Parse(server.URL)
			require.NoError(t, err)
			observed, err := Get(context.Background(), server.Client(), address, lookupAllocationID, "node", "default", nil)
			require.Error(t, err)
			require.Nil(t, observed)
		})
	}
}

func TestGetAcceptsOnlyCompleteExactNamespaceAbsence(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		_, _ = w.Write([]byte("[]"))
	}))
	defer server.Close()
	address, err := url.Parse(server.URL)
	require.NoError(t, err)
	for _, input := range []struct{ id, namespace string }{{"", "default"}, {lookupAllocationID[:8], "default"}, {lookupAllocationID, "*"}, {lookupAllocationID, ""}} {
		_, err := Get(t.Context(), server.Client(), address, input.id, "node", input.namespace, nil)
		require.Error(t, err)
	}
	require.Zero(t, calls)
	observed, err := Get(t.Context(), server.Client(), address, lookupAllocationID, "node", "default", nil)
	require.NoError(t, err)
	require.Nil(t, observed)
	require.Equal(t, 1, calls)
}
