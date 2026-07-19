package utils

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestDeleteTeamEventuallyRetriesCleanupConflict(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodDelete || request.URL.Path != "/teams/team-a" {
			t.Fatalf("request = %s %s, want DELETE /teams/team-a", request.Method, request.URL.Path)
		}
		if calls.Add(1) == 1 {
			http.Error(w, `{"error":{"message":"sandbox cleanup pending"}}`, http.StatusConflict)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	session := &Session{baseURL: server.URL, client: server.Client()}
	err := session.DeleteTeamEventually(context.Background(), nil, "team-a", time.Second)
	if err != nil {
		t.Fatalf("DeleteTeamEventually() error = %v", err)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("DELETE calls = %d, want 2", got)
	}
}

func TestDeleteTeamEventuallyAcceptsNotFound(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	session := &Session{baseURL: server.URL, client: server.Client()}
	if err := session.DeleteTeamEventually(context.Background(), nil, "team-a", time.Second); err != nil {
		t.Fatalf("DeleteTeamEventually() error = %v", err)
	}
}

func TestDeleteTeamEventuallyFailsFastForUnexpectedStatus(t *testing.T) {
	for _, status := range []int{http.StatusForbidden, http.StatusInternalServerError} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				calls.Add(1)
				http.Error(w, http.StatusText(status), status)
			}))
			defer server.Close()

			session := &Session{baseURL: server.URL, client: server.Client()}
			err := session.DeleteTeamEventually(context.Background(), nil, "team-a", time.Second)
			wantStatus := fmt.Sprintf("status %d", status)
			if err == nil || !strings.Contains(err.Error(), wantStatus) {
				t.Fatalf("DeleteTeamEventually() error = %v, want %q", err, wantStatus)
			}
			if got := calls.Load(); got != 1 {
				t.Fatalf("DELETE calls = %d, want 1", got)
			}
		})
	}
}

func TestDeleteTeamEventuallyReportsLastCleanupConflictOnTimeout(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if calls.Add(1) == 1 {
			http.Error(w, "last cleanup conflict", http.StatusConflict)
			return
		}
		<-request.Context().Done()
	}))
	defer server.Close()

	session := &Session{baseURL: server.URL, client: server.Client()}
	err := session.DeleteTeamEventually(context.Background(), nil, "team-a", 750*time.Millisecond)
	if err == nil {
		t.Fatal("DeleteTeamEventually() error = nil")
	}
	for _, want := range []string{
		context.DeadlineExceeded.Error(),
		fmt.Sprintf("status %d", http.StatusConflict),
		"last cleanup conflict",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("DeleteTeamEventually() error = %q, want %q", err, want)
		}
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("DELETE calls = %d, want 2", got)
	}
}
