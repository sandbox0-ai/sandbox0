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

func TestDeleteTeamEventuallyRetriesConflicts(t *testing.T) {
	t.Parallel()

	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if attempts.Add(1) < 2 {
			w.WriteHeader(http.StatusConflict)
			_, _ = fmt.Fprint(w, `{"error":{"message":"team has resources"}}`)
			return
		}
		_, _ = fmt.Fprint(w, `{}`)
	}))
	defer server.Close()

	session := &Session{baseURL: server.URL, client: server.Client()}
	if err := session.DeleteTeamEventually(context.Background(), nil, "team-1", 2*time.Second); err != nil {
		t.Fatalf("DeleteTeamEventually returned error: %v", err)
	}
	if got := attempts.Load(); got != 2 {
		t.Fatalf("attempts = %d, want 2", got)
	}
}

func TestDeleteTeamEventuallyDoesNotRetryOtherFailures(t *testing.T) {
	t.Parallel()

	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = fmt.Fprint(w, `{"error":{"message":"database unavailable"}}`)
	}))
	defer server.Close()

	session := &Session{baseURL: server.URL, client: server.Client()}
	err := session.DeleteTeamEventually(context.Background(), nil, "team-1", 2*time.Second)
	if err == nil || !strings.Contains(err.Error(), "database unavailable") {
		t.Fatalf("DeleteTeamEventually error = %v, want database failure", err)
	}
	if got := attempts.Load(); got != 1 {
		t.Fatalf("attempts = %d, want 1", got)
	}
}

func TestDeleteTeamEventuallyReturnsLastConflictAtDeadline(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusConflict)
		_, _ = fmt.Fprint(w, `{"error":{"message":"team has resources"}}`)
	}))
	defer server.Close()

	session := &Session{baseURL: server.URL, client: server.Client()}
	err := session.DeleteTeamEventually(context.Background(), nil, "team-1", 20*time.Millisecond)
	if err == nil || !strings.Contains(err.Error(), "team has resources") {
		t.Fatalf("DeleteTeamEventually error = %v, want last conflict", err)
	}
}
