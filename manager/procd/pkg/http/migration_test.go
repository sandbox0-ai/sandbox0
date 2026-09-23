package http

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	ctxpkg "github.com/sandbox0-ai/sandbox0/manager/procd/pkg/context"
	"github.com/sandbox0-ai/sandbox0/manager/procd/pkg/runtimecontroller"
	"github.com/sandbox0-ai/sandbox0/manager/procd/pkg/session"
	"github.com/sandbox0-ai/sandbox0/manager/procd/pkg/webhook"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/procdapi"
	"github.com/sandbox0-ai/sandbox0/pkg/procdconfig"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
	"go.uber.org/zap"
)

type migrationHTTPFixture struct {
	s          *Server
	controller *runtimecontroller.Controller
	request    procdapi.RuntimeMigrationRequest
	key        ed25519.PrivateKey
}

func TestMigrationHTTPCancellationFencesReorderedPreparation(t *testing.T) {
	for _, prepareFirst := range []bool{false, true} {
		f := newMigrationHTTPFixture(t, nil)
		if prepareFirst {
			w := f.send(t, context.Background(), f.request, f.token(t, f.request))
			if w.Code != http.StatusOK {
				t.Fatalf("prepare status: %d", w.Code)
			}
		}
		cancel := f.request
		cancel.Action = procdapi.MigrationCancel
		for range 2 {
			w := f.send(t, context.Background(), cancel, f.token(t, cancel))
			if w.Code != http.StatusOK {
				t.Fatalf("cancel status: %d", w.Code)
			}
		}
		for _, old := range []procdapi.RuntimeMigrationRequest{f.request, func() procdapi.RuntimeMigrationRequest {
			r := f.request
			r.Assignment.OperationID = "same-epoch-replacement"
			return r
		}()} {
			if w := f.send(t, context.Background(), old, f.token(t, old)); w.Code != http.StatusConflict {
				t.Fatalf("canceled epoch accepted preparation: %d", w.Code)
			}
		}
		next := f.request
		next.LifecycleEpoch++
		next.Assignment.OperationID = "next-migration"
		if w := f.send(t, context.Background(), next, f.token(t, next)); w.Code != http.StatusOK {
			t.Fatalf("new migration rejected: %d", w.Code)
		}
		next.Action = procdapi.MigrationCancel
		if w := f.send(t, context.Background(), next, f.token(t, next)); w.Code != http.StatusOK {
			t.Fatalf("new cancellation rejected: %d", w.Code)
		}
		if w := f.send(t, context.Background(), f.request, f.token(t, f.request)); w.Code != http.StatusConflict {
			t.Fatalf("older canceled epoch accepted: %d", w.Code)
		}
		if ready, _ := f.controller.CanServe(); !ready {
			t.Fatal("delayed prepare closed source admission")
		}
	}
}

func TestMigrationHTTPCancelDoesNotClearAnUnownedBarrier(t *testing.T) {
	f := newMigrationHTTPFixture(t, nil)
	r := httptest.NewRequest(http.MethodPut, "/internal/v1/lifecycle/barrier", nil)
	if _, err := f.s.barrier.setActive(r, lifecycleBarrierRequest{Active: true, Epoch: 10, RuntimeGeneration: 1}); err != nil {
		t.Fatal(err)
	}
	cancel := f.request
	cancel.Action = procdapi.MigrationCancel
	for range 2 {
		if w := f.send(t, context.Background(), cancel, f.token(t, cancel)); w.Code != http.StatusOK {
			t.Fatalf("cancellation status: %d", w.Code)
		}
		if release, accepted := f.s.barrier.enter(r); accepted {
			release()
			t.Fatal("preparation-free cancellation cleared another lifecycle's barrier")
		}
	}
}

func newMigrationHTTPFixture(t *testing.T, supervisor *session.Supervisor) migrationHTTPFixture {
	return newMigrationHTTPFixtureWithWebhook(t, supervisor, nil, nil)
}

func newMigrationHTTPFixtureWithWebhook(t *testing.T, supervisor *session.Supervisor, dispatcher *webhook.Dispatcher, config *runtimecontrol.WebhookConfig) migrationHTTPFixture {
	t.Helper()
	publicKey, privateKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	var contextManager *ctxpkg.Manager
	if supervisor != nil {
		contextManager = ctxpkg.NewManagerWithSupervisor(supervisor)
	}
	controller := runtimecontroller.New(contextManager, supervisor, nil, dispatcher, 49983, zap.NewNop())
	source := runtimecontrol.Assignment{SandboxID: "sandbox-1", TeamID: "team-1", RuntimeGeneration: 1, SecurityClass: "standard", Webhook: config}
	if err := controller.Activate(context.Background(), source); err != nil {
		t.Fatal(err)
	}
	revision, _ := source.Revision()
	source.RuntimeGeneration++
	validator := internalauth.NewValidator(internalauth.DefaultValidatorConfig(internalauth.ServiceProcd, publicKey))
	s := NewServer(&procdconfig.Config{}, contextManager, supervisor, nil, validator, nil, zap.NewNop(), nil,
		controller.Probe, controller.CanServe, WithMigrationController(controller))
	return migrationHTTPFixture{s: s, controller: controller, key: privateKey,
		request: procdapi.RuntimeMigrationRequest{Action: procdapi.MigrationPrepare, InstanceID: s.instanceID, LifecycleEpoch: 9,
			Assignment: runtimecontrol.MigrationAssignment{OperationID: "migration-1", SourceGeneration: 1, SourceRevision: revision, Target: source}}}
}

func (f migrationHTTPFixture) token(t *testing.T, request procdapi.RuntimeMigrationRequest) string {
	t.Helper()
	permission, err := request.Permission()
	if err != nil {
		t.Fatal(err)
	}
	return f.scopedToken(t, internalauth.ServiceManager, request.Assignment.Target.TeamID,
		request.Assignment.Target.SandboxID, []string{permission}, false)
}

func (f migrationHTTPFixture) scopedToken(t *testing.T, caller, team, sandbox string, permissions []string, system bool) string {
	t.Helper()
	g := internalauth.NewGenerator(internalauth.GeneratorConfig{Caller: caller, PrivateKey: f.key})
	opts := internalauth.GenerateOptions{SandboxID: sandbox, Permissions: permissions}
	var token string
	var err error
	if system {
		token, err = g.GenerateSystem(internalauth.ServiceProcd, opts)
	} else {
		token, err = g.Generate(internalauth.ServiceProcd, team, "", opts)
	}
	if err != nil {
		t.Fatal(err)
	}
	return token
}

func (f migrationHTTPFixture) send(t *testing.T, ctx context.Context, request procdapi.RuntimeMigrationRequest, token string) *httptest.ResponseRecorder {
	t.Helper()
	payload, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest(http.MethodPut, procdapi.RuntimeMigrationPath, bytes.NewReader(payload)).WithContext(ctx)
	r.Header.Set(internalauth.DefaultTokenHeader, token)
	w := httptest.NewRecorder()
	f.s.router.ServeHTTP(w, r)
	return w
}

func TestMigrationRouteRequiresExactManagerAuthorization(t *testing.T) {
	f := newMigrationHTTPFixture(t, nil)
	permission, _ := f.request.Permission()
	for _, tc := range []struct{ name, token string }{
		{"missing", ""},
		{"gateway", f.scopedToken(t, internalauth.ServiceClusterGateway, "team-1", "sandbox-1", []string{permission}, false)},
		{"unscoped_manager", f.scopedToken(t, internalauth.ServiceManager, "team-1", "sandbox-1", nil, false)},
		{"wildcard", f.scopedToken(t, internalauth.ServiceManager, "team-1", "sandbox-1", []string{"*", "runtime:migration:*"}, false)},
		{"other_team", f.scopedToken(t, internalauth.ServiceManager, "team-2", "sandbox-1", []string{permission}, false)},
		{"other_sandbox", f.scopedToken(t, internalauth.ServiceManager, "team-1", "sandbox-2", []string{permission}, false)},
		{"generic_system", f.scopedToken(t, internalauth.ServiceManager, "", "sandbox-1", []string{permission}, true)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := f.send(t, context.Background(), f.request, tc.token)
			if w.Code != http.StatusForbidden && w.Code != http.StatusUnauthorized {
				t.Fatalf("unauthorized status = %d", w.Code)
			}
			if ready, _ := f.controller.CanServe(); !ready {
				t.Fatal("rejected request changed readiness")
			}
		})
	}
	changed := f.request
	changed.LifecycleEpoch++
	if w := f.send(t, context.Background(), changed, f.token(t, f.request)); w.Code != http.StatusForbidden {
		t.Fatalf("token accepted changed epoch: %d", w.Code)
	}
	changed = f.request
	changed.InstanceID = uuid.NewString()
	if w := f.send(t, context.Background(), changed, f.token(t, changed)); w.Code != http.StatusConflict {
		t.Fatalf("token accepted wrong process instance: %d", w.Code)
	}
}

func TestMigrationHTTPHandoverAndLegacyControlFencing(t *testing.T) {
	f := newMigrationHTTPFixture(t, nil)
	host := httptest.NewServer(f.s.router)
	defer host.Close()
	client := procdapi.NewProcdClient(procdapi.ProcdClientConfig{})
	prepareToken := f.token(t, f.request)
	for range 2 {
		if _, err := client.MigrateRuntime(context.Background(), host.URL, f.request, prepareToken); err != nil {
			t.Fatal(err)
		}
	}
	for _, path := range []string{procdapi.LifecycleBarrierPath, procdapi.SandboxPausePath, procdapi.SandboxResumePath} {
		method := http.MethodPost
		if path == procdapi.LifecycleBarrierPath {
			method = http.MethodPut
		}
		r := httptest.NewRequest(method, path, bytes.NewBufferString(`{"active":false}`))
		r.Header.Set(internalauth.DefaultTokenHeader, prepareToken)
		w := httptest.NewRecorder()
		f.s.router.ServeHTTP(w, r)
		// The activation/readiness gate runs before the lifecycle migration
		// guard and rejects a prepared source before it reaches the handler.
		if w.Code != http.StatusServiceUnavailable {
			t.Fatalf("legacy control %s bypassed readiness fencing: %d", path, w.Code)
		}
		guarded := f.s.migrationLifecycleMiddleware(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
			t.Fatal("legacy lifecycle handler ran while migration owned the process")
		}))
		w = httptest.NewRecorder()
		guarded.ServeHTTP(w, r)
		if w.Code != http.StatusConflict {
			t.Fatalf("legacy control %s bypassed migration fencing: %d", path, w.Code)
		}
	}
	if _, err := client.ProbeCommandReady(context.Background(), host.URL, prepareToken); err == nil {
		t.Fatal("command-ready probe bypassed migration gate")
	}
	restore := f.request
	restore.Action = procdapi.MigrationRestore
	if w := f.send(t, context.Background(), restore, prepareToken); w.Code != http.StatusForbidden {
		t.Fatalf("prepare permission authorized restore: %d", w.Code)
	}
	for range 2 {
		if _, err := client.MigrateRuntime(context.Background(), host.URL, restore, f.token(t, restore)); err != nil {
			t.Fatal(err)
		}
	}
	probe, err := client.ProbeCommandReady(context.Background(), host.URL, prepareToken)
	if err != nil || probe.InstanceID != f.request.InstanceID {
		t.Fatalf("restored probe = %#v, %v", probe, err)
	}
	if generation := f.controller.State().RuntimeGeneration; generation != 2 {
		t.Fatalf("generation = %d", generation)
	}
	cancel := f.request
	cancel.Action = procdapi.MigrationCancel
	if _, err := client.MigrateRuntime(context.Background(), host.URL, cancel, f.token(t, cancel)); err == nil {
		t.Fatal("cancel rolled back restored runtime")
	}
}

func TestMigrationDrainFailureKeepsGateClosed(t *testing.T) {
	f := newMigrationHTTPFixture(t, nil)
	release, ok := f.s.barrier.enter(httptest.NewRequest(http.MethodPost, "/api/v1/functions/execute", nil))
	if !ok {
		t.Fatal("cannot enter source operation")
	}
	defer release()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	w := f.send(t, ctx, f.request, f.token(t, f.request))
	if w.Code != http.StatusConflict {
		t.Fatalf("incomplete drain succeeded: %d", w.Code)
	}
	if ready, _ := f.controller.CanServe(); ready {
		t.Fatal("failed drain reopened readiness")
	}
	restore := f.request
	restore.Action = procdapi.MigrationRestore
	if w := f.send(t, context.Background(), restore, f.token(t, restore)); w.Code != http.StatusConflict {
		t.Fatalf("restore accepted incomplete drain: %d", w.Code)
	}
	release()
	w = f.send(t, context.Background(), f.request, f.token(t, f.request))
	if w.Code != http.StatusOK {
		t.Fatalf("drain retry failed: %d %s", w.Code, w.Body.String())
	}
	response, apiErr, err := spec.DecodeResponse[procdapi.RuntimeMigrationResponse](w.Body)
	if err != nil || apiErr != nil {
		t.Fatalf("decode prepared response: %v, %v", err, apiErr)
	}
	if err := response.ValidateFor(f.request); err != nil {
		t.Fatal(err)
	}
	cancellation := f.request
	cancellation.Action = procdapi.MigrationCancel
	w = f.send(t, context.Background(), cancellation, f.token(t, cancellation))
	if w.Code != http.StatusOK {
		t.Fatalf("source cancellation failed: %d", w.Code)
	}
	if ready, _ := f.controller.CanServe(); !ready {
		t.Fatal("cancel did not restore source admission")
	}
}
