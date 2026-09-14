package nodepoollifecycle

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func TestNomadAllocationTerminalUsesClientExecutionTruth(t *testing.T) {
	for _, test := range []struct {
		client, desired string
		terminal        bool
	}{
		{client: "complete", desired: "run", terminal: true},
		{client: "failed", desired: "run", terminal: true},
		{client: "lost", desired: "stop", terminal: true},
		{client: "running", desired: "stop", terminal: false},
		{client: "pending", desired: "run", terminal: false},
	} {
		allocation := nomadAllocation{ClientStatus: test.client, DesiredStatus: test.desired}
		if got := allocation.terminal(); got != test.terminal {
			t.Fatalf("terminal(%s,%s) = %v, want %v", test.client, test.desired, got, test.terminal)
		}
	}
}

func newInventoryTestClient(t *testing.T, handler http.HandlerFunc) *NomadClient {
	t.Helper()
	server := httptest.NewTLSServer(handler)
	t.Cleanup(server.Close)
	address, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	token := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(token, []byte("test-token"), 0600); err != nil {
		t.Fatal(err)
	}
	return &NomadClient{baseURL: address, tokenFile: token, region: "test", warmJobID: "warm", http: server.Client()}
}

func TestNomadInventoryReadsEverySummaryPageBeforeStopping(t *testing.T) {
	const total = 502
	pages, stopped := 0, 0
	client := newInventoryTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Nomad-Token") != "test-token" || r.Header.Get("X-Nomad-Region") != "test" {
			t.Error("missing authenticated region identity")
		}
		switch {
		case r.URL.Path == "/v1/node/node/eligibility":
			w.WriteHeader(http.StatusOK)
		case r.URL.Path == "/v1/allocations":
			q := r.URL.Query()
			if q.Get("filter") != `NodeID == "node"` || q.Get("namespace") != "*" || q.Get("task_states") != "false" || q.Get("resources") != "false" {
				t.Errorf("unexpected inventory query: %v", q)
			}
			start := 0
			if q.Get("next_token") != "" {
				var err error
				start, err = strconv.Atoi(q.Get("next_token"))
				if err != nil {
					t.Error(err)
				}
			}
			end := min(start+nomadAllocationPageSize, total)
			if end < total {
				w.Header().Set("X-Nomad-NextToken", strconv.Itoa(end))
			}
			batch := make([]nomadAllocation, 0, end-start)
			for i := start; i < end; i++ {
				batch = append(batch, nomadAllocation{ID: fmt.Sprintf("alloc-%d", i), NodeID: "node", JobID: "warm", Namespace: "default", ClientStatus: "running"})
			}
			pages++
			if err := json.NewEncoder(w).Encode(batch); err != nil {
				t.Error(err)
			}
		case strings.HasPrefix(r.URL.Path, "/v1/allocation/") && strings.HasSuffix(r.URL.Path, "/stop"):
			if pages != 8 {
				t.Errorf("stopped before complete inventory: %d pages", pages)
			}
			stopped++
			w.WriteHeader(http.StatusOK)
		default:
			t.Errorf("unexpected endpoint: %s", r.URL.Path)
			http.NotFound(w, r)
		}
	})
	if err := client.FenceAndStopWarmAllocations(context.Background(), "node"); err != nil {
		t.Fatal(err)
	}
	if pages != 8 || stopped != total {
		t.Fatalf("pages=%d stopped=%d", pages, stopped)
	}
}

func TestNomadInventoryFailsClosedBeforeStopping(t *testing.T) {
	for _, scenario := range []string{"other-node", "missing-id", "duplicate-id", "repeated-token", "foreign-job", "foreign-namespace", "truncated-page", "oversized-page", "page-limit"} {
		t.Run(scenario, func(t *testing.T) {
			pages, stopped := 0, 0
			client := newInventoryTestClient(t, func(w http.ResponseWriter, r *http.Request) {
				if strings.HasSuffix(r.URL.Path, "/eligibility") {
					w.WriteHeader(http.StatusOK)
					return
				}
				if strings.HasSuffix(r.URL.Path, "/stop") {
					stopped++
					w.WriteHeader(http.StatusOK)
					return
				}
				if r.URL.Path != "/v1/allocations" {
					t.Errorf("unexpected endpoint: %s", r.URL.Path)
					http.NotFound(w, r)
					return
				}
				pages++
				batch := []nomadAllocation{{ID: fmt.Sprintf("alloc-%d", pages), NodeID: "node", JobID: "warm", Namespace: "default", ClientStatus: "running"}}
				if pages == 1 {
					w.Header().Set("X-Nomad-NextToken", "next")
				} else {
					switch scenario {
					case "other-node":
						batch[0].NodeID = "other"
					case "missing-id":
						batch[0].ID = ""
					case "duplicate-id":
						batch[0].ID = "alloc-1"
					case "repeated-token":
						w.Header().Set("X-Nomad-NextToken", "next")
					case "foreign-job":
						batch[0].JobID = "customer"
					case "foreign-namespace":
						batch[0].Namespace = "customer"
					case "truncated-page":
						_, _ = w.Write([]byte(`[{"ID":`))
						return
					case "oversized-page":
						batch = make([]nomadAllocation, nomadAllocationPageSize+1)
					case "page-limit":
						w.Header().Set("X-Nomad-NextToken", fmt.Sprintf("next-%d", pages))
					}
				}
				if err := json.NewEncoder(w).Encode(batch); err != nil {
					t.Error(err)
				}
			})
			if err := client.FenceAndStopWarmAllocations(context.Background(), "node"); err == nil {
				t.Fatal("unsafe inventory accepted")
			}
			if stopped != 0 {
				t.Fatalf("stopped %d allocations despite incomplete or unsafe inventory", stopped)
			}
		})
	}
}

func TestNomadInventoryFindsNonterminalAllocationOnLaterPage(t *testing.T) {
	pages := 0
	client := newInventoryTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		pages++
		status := "complete"
		if pages == 1 {
			w.Header().Set("X-Nomad-NextToken", "second")
		} else {
			status = "running"
		}
		if err := json.NewEncoder(w).Encode([]nomadAllocation{{ID: fmt.Sprint(pages), NodeID: "node", ClientStatus: status}}); err != nil {
			t.Error(err)
		}
	})
	present, err := client.NodeHasNonterminalAllocations(context.Background(), "node")
	if err != nil || !present || pages != 2 {
		t.Fatalf("present=%v pages=%d err=%v", present, pages, err)
	}
}
