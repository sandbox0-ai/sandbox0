package nomadinventory

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

func TestFailedWarmPageRejectsUntrustedOrUnboundedInventory(t *testing.T) {
	valid := Allocation{ID: "d0e22a8c-8a41-f97b-f583-3c822cf5f7dd", NodeID: "node", Namespace: "default", JobID: DefaultWarmJobID + "-shard-13", ClientStatus: "failed", DesiredStatus: "run"}
	for _, test := range []struct {
		name          string
		alter         func(*Allocation)
		header, value string
		copies        int
	}{
		{name: "other namespace", alter: func(a *Allocation) { a.Namespace = "other" }},
		{name: "prefix collision", alter: func(a *Allocation) { a.JobID += "-other" }},
		{name: "running guest", alter: func(a *Allocation) { a.ClientStatus = "running" }},
		{name: "already stopped", alter: func(a *Allocation) { a.DesiredStatus = "stop" }},
		{name: "already replaced", alter: func(a *Allocation) { a.NextAllocation = "replacement" }},
		{name: "invalid allocation", alter: func(a *Allocation) { a.ID = "not-a-uuid" }},
		{name: "invalid node", alter: func(a *Allocation) { a.NodeID = "node/other" }},
		{name: "filtered ACL", header: "X-Nomad-Results-Filtered-By-ACLs", value: "true"},
		{name: "repeated cursor", header: "X-Nomad-NextToken", value: "previous"},
		{name: "oversized cursor", header: "X-Nomad-NextToken", value: strings.Repeat("x", 4097)},
		{name: "duplicate identity", copies: 2},
		{name: "oversized page", copies: 9},
	} {
		t.Run(test.name, func(t *testing.T) {
			a := valid
			if test.alter != nil {
				test.alter(&a)
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if test.header != "" {
					w.Header().Set(test.header, test.value)
				}
				batch := []Allocation{a}
				for len(batch) < test.copies {
					batch = append(batch, a)
				}
				_ = json.NewEncoder(w).Encode(batch)
			}))
			defer server.Close()
			base, _ := url.Parse(server.URL)
			batch, next, err := FailedWarmPage(t.Context(), server.Client(), base, DefaultWarmJobID, "previous", nil)
			if err == nil || batch != nil || next != "" {
				t.Fatalf("accepted unsafe page: %v %q %v", batch, next, err)
			}
		})
	}
}

func TestFailedWarmPageUsesSmallAuthenticatedCursorQuery(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		if r.URL.Path != "/v1/allocations" || q.Get("next_token") != "previous" || q.Get("per_page") != "8" || q.Get("namespace") != "default" || q.Get("task_states") != "false" || q.Get("resources") != "false" || r.Header.Get("X-Nomad-Token") != "test-token" {
			t.Error("unbounded or unauthenticated query")
		}
		if !strings.Contains(q.Get("filter"), `ClientStatus == "failed"`) || !strings.Contains(q.Get("filter"), `JobID == "sandbox0-warm-slots-shard-17"`) {
			t.Error("missing scheduling restrictions")
		}
		w.Header().Set("X-Nomad-NextToken", "next")
		_ = json.NewEncoder(w).Encode([]Allocation{{ID: "d0e22a8c-8a41-f97b-f583-3c822cf5f7dd", NodeID: "node", Namespace: "default", JobID: DefaultWarmJobID, ClientStatus: "failed", DesiredStatus: "run"}})
	}))
	defer server.Close()
	base, _ := url.Parse(server.URL)
	batch, next, err := FailedWarmPage(t.Context(), server.Client(), base, DefaultWarmJobID, "previous", http.Header{"X-Nomad-Token": {"test-token"}})
	if err != nil || len(batch) != 1 || next != "next" {
		t.Fatalf("page = %v %q %v", batch, next, err)
	}
}
