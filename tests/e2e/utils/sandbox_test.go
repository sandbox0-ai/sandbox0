package utils

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/apispec"
)

func TestClaimSandboxDetailedUsesLongRunningRequestTimeout(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(30 * time.Millisecond)
		w.WriteHeader(http.StatusCreated)
		_, _ = fmt.Fprint(w, `{"success":true,"data":{"sandbox_id":"sb-1","pod_name":"pod-1","status":"running","template":"default","cluster_id":null}}`)
	}))
	defer server.Close()

	client := server.Client()
	client.Timeout = 5 * time.Millisecond
	session := &Session{
		baseURL: server.URL,
		teamID:  "team-1",
		userID:  "user-1",
		client:  client,
	}
	templateID := "default"
	resp, status, err := session.ClaimSandboxDetailed(
		context.Background(),
		nil,
		apispec.ClaimRequest{Template: &templateID},
	)
	if err != nil {
		t.Fatalf("ClaimSandboxDetailed returned error: %v", err)
	}
	if status != http.StatusCreated {
		t.Fatalf("status = %d, want %d", status, http.StatusCreated)
	}
	if resp == nil || resp.SandboxId != "sb-1" {
		t.Fatalf("response = %#v, want sandbox sb-1", resp)
	}
}

func TestDeleteAllSandboxesEventuallyDeletesUntrackedResources(t *testing.T) {
	t.Parallel()

	remaining := map[string]struct{}{"known": {}, "untracked": {}}
	var mu sync.Mutex
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		switch r.Method {
		case http.MethodGet:
			_, _ = fmt.Fprintf(w, `{"success":true,"data":{"sandboxes":[`)
			first := true
			for id := range remaining {
				if !first {
					_, _ = fmt.Fprint(w, ",")
				}
				first = false
				_, _ = fmt.Fprintf(w, `{"id":%q}`, id)
			}
			_, _ = fmt.Fprintf(w, `],"count":%d,"has_more":false}}`, len(remaining))
		case http.MethodDelete:
			id := r.URL.Path[len("/api/v1/sandboxes/"):]
			delete(remaining, id)
			_, _ = fmt.Fprint(w, `{"success":true}`)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()

	session := &Session{baseURL: server.URL, client: server.Client()}
	if err := session.DeleteAllSandboxesEventually(context.Background(), nil, 2*time.Second); err != nil {
		t.Fatalf("DeleteAllSandboxesEventually returned error: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(remaining) != 0 {
		t.Fatalf("remaining sandboxes = %v, want none", remaining)
	}
}
