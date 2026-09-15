// Copyright 2026 Sandbox0 Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package rootfswriterauthority

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/containerd/errdefs"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/rootfswriterauthority"
)

func consumeTestStage() rootfshandoff.StageRequest {
	stage := crashAbandonClientTestStage()
	stage.Identity.WriterGrantToken = "writer-token"
	return stage
}

func consumeTestClient(t *testing.T, handler http.HandlerFunc) *ManagerClient {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	baseURL, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte("projected-token"), 0o600); err != nil {
		t.Fatal(err)
	}
	return &ManagerClient{baseURL: baseURL, tokenFile: tokenFile, http: server.Client()}
}

// An acknowledged consume can lose its response. Replay must use the same
// one-time binding and accept only the authority's original lease expiry.
func TestConsumeWriterGrantRecoversLostAcknowledgement(t *testing.T) {
	for _, failure := range []string{"connection_closed", "client_timeout", "gateway_timeout", "unavailable"} {
		t.Run(failure, func(t *testing.T) {
			var attempts atomic.Int32
			releaseResponse := make(chan struct{})
			defer close(releaseResponse)
			var mu sync.Mutex
			var originalBody string
			now := time.Now().UTC()
			committed := protocol.LeaseObservation{
				ServerTime: now, RenewAfter: now.Add(10 * time.Second), LeaseExpiresAt: now.Add(time.Minute),
			}
			client := consumeTestClient(t, func(w http.ResponseWriter, r *http.Request) {
				body, err := io.ReadAll(r.Body)
				if err != nil {
					t.Error(err)
				}
				if r.Method != http.MethodPut || r.URL.Path != protocol.ConsumePath("grant-1") || r.Header.Get("Authorization") != "Bearer projected-token" {
					t.Error("consume request changed its authenticated target")
				}
				attempt := attempts.Add(1)
				mu.Lock()
				if attempt == 1 {
					originalBody = string(body)
				} else if originalBody != string(body) {
					t.Error("consume replay changed the exact grant binding")
				}
				mu.Unlock()
				if attempt == 1 {
					switch failure {
					case "connection_closed":
						connection, _, err := w.(http.Hijacker).Hijack()
						if err != nil {
							t.Error(err)
							return
						}
						_ = connection.Close()
					case "client_timeout":
						select {
						case <-r.Context().Done():
						case <-releaseResponse:
						}
					case "gateway_timeout":
						w.WriteHeader(http.StatusGatewayTimeout)
					case "unavailable":
						w.WriteHeader(http.StatusServiceUnavailable)
					}
					return
				}
				_ = json.NewEncoder(w).Encode(committed)
			})
			if failure == "client_timeout" {
				client.http.Timeout = 50 * time.Millisecond
			}
			observed, err := client.ConsumeWriterGrant(t.Context(), consumeTestStage())
			if err != nil || !observed.LeaseExpiresAt.Equal(committed.LeaseExpiresAt) || attempts.Load() != 2 {
				t.Fatalf("consume error=%v, attempts=%d, expiry=%v; want original committed lease on exact replay", err, attempts.Load(), observed.LeaseExpiresAt)
			}
		})
	}
}

func TestConsumeWriterGrantBoundsRetriesAndPreservesRejections(t *testing.T) {
	for _, test := range []struct {
		status   int
		attempts int32
		want     error
	}{
		{http.StatusServiceUnavailable, 3, errdefs.ErrUnavailable},
		{http.StatusGatewayTimeout, 3, errdefs.ErrUnavailable},
		{http.StatusBadRequest, 1, errdefs.ErrInvalidArgument},
		{http.StatusForbidden, 1, errdefs.ErrPermissionDenied},
		{http.StatusNotFound, 1, errdefs.ErrNotFound},
		{http.StatusPreconditionFailed, 1, errdefs.ErrFailedPrecondition},
	} {
		t.Run(http.StatusText(test.status), func(t *testing.T) {
			var attempts atomic.Int32
			client := consumeTestClient(t, func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				attempts.Add(1)
				w.WriteHeader(test.status)
			})
			_, err := client.ConsumeWriterGrant(t.Context(), consumeTestStage())
			if !errors.Is(err, test.want) || attempts.Load() != test.attempts {
				t.Fatalf("consume error=%v attempts=%d; want %v attempts=%d", err, attempts.Load(), test.want, test.attempts)
			}
		})
	}
}

func TestConsumeWriterGrantStopsWhenCallerCancels(t *testing.T) {
	for _, inFlight := range []bool{false, true} {
		t.Run(map[bool]string{false: "backoff", true: "in_flight"}[inFlight], func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			var attempts atomic.Int32
			client := consumeTestClient(t, func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				attempts.Add(1)
				if inFlight {
					cancel()
					return
				}
				w.WriteHeader(http.StatusServiceUnavailable)
				w.(http.Flusher).Flush()
				cancel()
			})
			_, err := client.ConsumeWriterGrant(ctx, consumeTestStage())
			if !errors.Is(err, context.Canceled) || attempts.Load() != 1 {
				t.Fatalf("consume error=%v attempts=%d; want caller cancellation without replay", err, attempts.Load())
			}
		})
	}
}

func TestManagerClientAcceptsFullBatchAndRejectsUnrequestedGrant(t *testing.T) {
	for _, wrongGrant := range []bool{false, true} {
		t.Run(fmt.Sprintf("wrong_grant_%v", wrongGrant), func(t *testing.T) {
			stages := make([]rootfshandoff.StageRequest, protocol.MaxBatchRenewItems)
			observation := protocol.LeaseObservation{
				ServerTime: time.Date(2026, 9, 15, 0, 0, 0, 123456789, time.FixedZone("offset", -4*3600)),
			}
			observation.RenewAfter = observation.ServerTime.Add(15 * time.Second)
			observation.LeaseExpiresAt = observation.ServerTime.Add(30 * time.Second)
			response := protocol.BatchRenewResponse{}
			for i := range stages {
				stages[i] = consumeTestStage().WithoutWriterGrantToken()
				stages[i].Identity.WriterGrantID = fmt.Sprintf("grant-%064d", i)
				response.Results = append(response.Results, protocol.BatchRenewResult{GrantID: stages[i].Identity.WriterGrantID, Observation: &observation})
			}
			if wrongGrant {
				response.Results[0].GrantID = "not-requested"
			}
			payload, err := json.Marshal(response)
			if err != nil {
				t.Fatal(err)
			}
			if !wrongGrant && len(payload) <= maxResponseBytes {
				t.Fatal("fixture does not exceed single-grant response limit")
			}
			client := consumeTestClient(t, func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != protocol.BatchRenewPath {
					t.Error("wrong batch path")
				}
				var request protocol.BatchRenewRequest
				if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
					t.Error(err)
				}
				if len(request.Items) != protocol.MaxBatchRenewItems {
					t.Error("batch request was truncated")
				}
				_, _ = w.Write(payload)
			})
			got, err := client.RenewWriterGrants(t.Context(), stages)
			if wrongGrant {
				if !errdefs.IsUnavailable(err) {
					t.Fatalf("unrequested grant error=%v", err)
				}
			} else if err != nil || len(got.Results) != protocol.MaxBatchRenewItems {
				t.Fatalf("full batch results=%d error=%v", len(got.Results), err)
			}
		})
	}
}
