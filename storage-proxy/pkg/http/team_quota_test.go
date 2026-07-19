package http

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/teamquota"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/storagequota"
	storagequotatest "github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/storagequota/testutil"
)

type testStorageOperationQuota struct {
	mu    sync.Mutex
	teams []string
	err   error
}

func (q *testStorageOperationQuota) Admit(_ context.Context, teamID string) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.teams = append(q.teams, teamID)
	return q.err
}

func (*testStorageOperationQuota) Close() error { return nil }

func newTestStorageOperationQuota() *testStorageOperationQuota {
	return &testStorageOperationQuota{}
}

func (q *testStorageOperationQuota) admittedTeams() []string {
	q.mu.Lock()
	defer q.mu.Unlock()
	return append([]string(nil), q.teams...)
}

func newTestStorageQuota() *storagequota.Service {
	return storagequotatest.NewService("test-cluster")
}

func TestStorageOperationAdmission(t *testing.T) {
	t.Run("allowed", func(t *testing.T) {
		quota := newTestStorageOperationQuota()
		server := &Server{storageOperations: quota}
		recorder := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodPost, "/sandboxvolumes", nil)

		if !server.admitStorageOperation(recorder, request, "team-1") {
			t.Fatalf("admission denied with status %d", recorder.Code)
		}
		if len(quota.teams) != 1 || quota.teams[0] != "team-1" {
			t.Fatalf("admitted teams = %#v, want [team-1]", quota.teams)
		}
	})

	t.Run("rate exceeded", func(t *testing.T) {
		quota := &testStorageOperationQuota{err: &teamquota.RateExceededError{
			TeamID:     "team-1",
			Key:        teamquota.KeyStorageOperations,
			RetryAfter: 1500 * time.Millisecond,
		}}
		server := &Server{storageOperations: quota}
		recorder := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodPost, "/sandboxvolumes", nil)

		if server.admitStorageOperation(recorder, request, "team-1") {
			t.Fatal("rate-exceeded admission succeeded")
		}
		if recorder.Code != http.StatusTooManyRequests {
			t.Fatalf("status = %d, want %d", recorder.Code, http.StatusTooManyRequests)
		}
		if got := recorder.Header().Get("Retry-After"); got != "2" {
			t.Fatalf("Retry-After = %q, want 2", got)
		}
	})

	t.Run("missing quota fails closed", func(t *testing.T) {
		server := &Server{}
		recorder := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodPost, "/sandboxvolumes", nil)

		if server.admitStorageOperation(recorder, request, "team-1") {
			t.Fatal("admission without quota succeeded")
		}
		if recorder.Code != http.StatusServiceUnavailable {
			t.Fatalf("status = %d, want %d", recorder.Code, http.StatusServiceUnavailable)
		}
		if got := recorder.Header().Get("Retry-After"); got != "1" {
			t.Fatalf("Retry-After = %q, want 1", got)
		}
	})
}

func TestWriteVolumeFileErrorMapsTeamQuotaDecisions(t *testing.T) {
	server := &Server{}
	tests := []struct {
		name       string
		err        error
		status     int
		retryAfter string
	}{
		{
			name: "exceeded",
			err: &teamquota.ExceededError{
				TeamID:    "team-1",
				Key:       teamquota.KeyVolumeStorageBytes,
				Limit:     1,
				Requested: 2,
			},
			status: http.StatusTooManyRequests,
		},
		{
			name: "unavailable",
			err: &teamquota.UnavailableError{
				Operation: "reserve volume storage",
				Err:       errors.New("database unavailable"),
			},
			status:     http.StatusServiceUnavailable,
			retryAfter: "1",
		},
		{
			name: "rate exceeded",
			err: &teamquota.RateExceededError{
				TeamID:     "team-1",
				Key:        teamquota.KeyStorageOperations,
				RetryAfter: time.Second,
			},
			status:     http.StatusTooManyRequests,
			retryAfter: "1",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			server.writeVolumeFileError(recorder, test.err)
			if recorder.Code != test.status {
				t.Fatalf("status = %d, want %d", recorder.Code, test.status)
			}
			if got := recorder.Header().Get("Retry-After"); got != test.retryAfter {
				t.Fatalf("Retry-After = %q, want %q", got, test.retryAfter)
			}
		})
	}
}
