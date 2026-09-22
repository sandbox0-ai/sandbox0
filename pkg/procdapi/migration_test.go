package procdapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
)

func TestMigrationClientRejectsUnboundOrMalformedResponses(t *testing.T) {
	source := runtimecontrol.Assignment{SandboxID: "sandbox-1", TeamID: "team-1", RuntimeGeneration: 1, SecurityClass: "standard"}
	revision, err := source.Revision()
	if err != nil {
		t.Fatal(err)
	}
	source.RuntimeGeneration = 2
	request := RuntimeMigrationRequest{Action: MigrationRestore, InstanceID: uuid.NewString(), LifecycleEpoch: 3,
		Assignment: runtimecontrol.MigrationAssignment{OperationID: "migration-1", SourceGeneration: 1, SourceRevision: revision, Target: source}}
	digest, err := request.Digest()
	if err != nil {
		t.Fatal(err)
	}
	valid := RuntimeMigrationResponse{InstanceID: request.InstanceID, RequestDigest: digest, RuntimeGeneration: 2, State: "ready"}
	for _, tc := range []struct {
		name string
		edit func(*RuntimeMigrationResponse)
		raw  string
	}{
		{name: "other_instance", edit: func(r *RuntimeMigrationResponse) { r.InstanceID = uuid.NewString() }},
		{name: "other_request", edit: func(r *RuntimeMigrationResponse) { r.RequestDigest = strings.Repeat("a", 64) }},
		{name: "old_generation", edit: func(r *RuntimeMigrationResponse) { r.RuntimeGeneration = 1 }},
		{name: "not_ready", edit: func(r *RuntimeMigrationResponse) { r.State = "prepared" }},
		{name: "missing_success", raw: `{"success":false}`},
		{name: "missing_data", raw: `{"success":true}`},
		{name: "oversized", raw: strings.Repeat("x", (1<<20)+1)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			host := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodPut || r.URL.Path != RuntimeMigrationPath || r.Header.Get("X-Internal-Token") != "exact-token" {
					t.Error("client did not send internal migration request")
				}
				var received RuntimeMigrationRequest
				if err := json.NewDecoder(r.Body).Decode(&received); err != nil {
					t.Error(err)
				}
				if got, _ := received.Digest(); got != digest {
					t.Error("client changed request")
				}
				if tc.raw != "" {
					_, _ = w.Write([]byte(tc.raw))
					return
				}
				response := valid
				tc.edit(&response)
				_ = spec.WriteSuccess(w, http.StatusOK, response)
			}))
			defer host.Close()
			if _, err := NewProcdClient(ProcdClientConfig{}).MigrateRuntime(context.Background(), host.URL, request, "exact-token"); err == nil {
				t.Fatal("invalid response accepted")
			}
		})
	}
}
