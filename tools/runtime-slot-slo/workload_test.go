package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
)

func commandResponse(id, stdout string) map[string]any {
	return map[string]any{"id": id, "type": "cmd", "running": false, "paused": false,
		"created_at": "2026-09-05T00:00:00Z", "state": "stopped", "exit_code": 0, "stdout": stdout}
}

func assertCommandRequest(t *testing.T, r *http.Request, argv []string, ttl int) {
	t.Helper()
	var body struct {
		Type string `json:"type"`
		Cmd  struct {
			Command []string `json:"command"`
		} `json:"cmd"`
		Wait bool `json:"wait_until_done"`
		TTL  int  `json:"ttl_sec"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		t.Error(err)
	}
	if body.Type != "cmd" || !body.Wait || body.TTL != ttl || !reflect.DeepEqual(body.Cmd.Command, argv) {
		t.Errorf("command request = %+v; argv=%q ttl=%d", body, argv, ttl)
	}
	if r.Header.Get("Authorization") != "Bearer test-token" || r.Header.Get("Content-Type") != "application/json" {
		t.Errorf("missing public command authentication/content type")
	}
	if r.Header.Get("Idempotency-Key") != "" || r.Header.Get("X-Idempotency-Key") != "" {
		t.Error("command POST must not be marked replayable")
	}
}

func serveDefaultCommand(t *testing.T, w http.ResponseWriter, r *http.Request) bool {
	if r.Method != http.MethodPost || !strings.HasSuffix(r.URL.Path, "/contexts") {
		return false
	}
	step := shellWorkload().Steps[0]
	assertCommandRequest(t, r, step.Argv, 15)
	_ = spec.WriteSuccess(w, http.StatusCreated, commandResponse("ctx-shell", step.ExpectStdout))
	return true
}

// The fake ingress only implements public APIs. Its counters also prove that a
// failed/ambiguous create is never retried and sandbox cleanup still converges.
func commandServer(t *testing.T, command http.HandlerFunc) (config, *atomic.Int64, *atomic.Int64) {
	t.Helper()
	var posts, deletes atomic.Int64
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/sandboxes", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Server-Timing", "sandbox0-command-ready;dur=0.001")
		w.Header().Set("Sandbox0-Command-Ready-SLO", "met")
		_ = spec.WriteSuccess(w, http.StatusCreated, claimResponse{SandboxID: "sandbox-1"})
	})
	mux.HandleFunc("POST /api/v1/sandboxes/sandbox-1/contexts", func(w http.ResponseWriter, r *http.Request) {
		posts.Add(1)
		command(w, r)
	})
	mux.HandleFunc("DELETE /api/v1/sandboxes/sandbox-1", func(w http.ResponseWriter, r *http.Request) {
		deletes.Add(1)
		_ = spec.WriteSuccess(w, http.StatusAccepted, struct{}{})
	})
	mux.HandleFunc("GET /api/v1/sandboxes/sandbox-1", func(w http.ResponseWriter, r *http.Request) {
		if deletes.Load() == 0 {
			t.Error("absence checked before DELETE")
		}
		_ = spec.WriteError(w, http.StatusNotFound, spec.CodeNotFound, "sandbox is absent")
	})
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	cfg := config{
		endpoint: server.URL + "/api/v1/sandboxes", token: "test-token", body: []byte(`{"template":"default"}`),
		batches: 1, concurrency: 1, requestTimeout: time.Second,
		cleanupTimeout: time.Second, cleanupPoll: 10 * time.Millisecond,
		hardLimit: time.Second, p50Target: 500 * time.Millisecond, firstCommandHardLimit: time.Second,
		workload: shellWorkload(), contextTTL: 15 * time.Second, client: server.Client(),
	}
	return cfg, &posts, &deletes
}

func TestRunRequiresRealCommandEvidenceAndCleansFailures(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(map[string]any)
	}{
		{"nonzero-exit", func(v map[string]any) { v["exit_code"] = 7 }},
		{"missing-exit", func(v map[string]any) { delete(v, "exit_code") }},
		{"null-exit", func(v map[string]any) { v["exit_code"] = nil }},
		{"stdout-mismatch", func(v map[string]any) { v["stdout"] = "wrong" }},
		{"missing-stdout", func(v map[string]any) { delete(v, "stdout") }},
		{"raw-output-only", func(v map[string]any) { v["output_raw"] = v["stdout"]; delete(v, "stdout") }},
		{"running", func(v map[string]any) { v["running"] = true }},
		{"missing-running", func(v map[string]any) { delete(v, "running") }},
		{"missing-state", func(v map[string]any) { delete(v, "state") }},
		{"killed", func(v map[string]any) { v["state"] = "killed" }},
		{"missing-id", func(v map[string]any) { delete(v, "id") }},
		{"unsafe-id", func(v map[string]any) { v["id"] = "../../other" }},
		{"repl", func(v map[string]any) { v["type"] = "repl" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, posts, deletes := commandServer(t, func(w http.ResponseWriter, r *http.Request) {
				assertCommandRequest(t, r, shellWorkload().Steps[0].Argv, 15)
				body := commandResponse("ctx-1", shellWorkload().Steps[0].ExpectStdout)
				tc.mutate(body)
				_ = spec.WriteSuccess(w, http.StatusCreated, body)
			})
			result, err := run(context.Background(), cfg)
			if err == nil || result.Passed || result.Errors != 1 || result.FirstCommandErrors != 1 ||
				result.FirstCommand.Count != 0 || result.WorkloadWall.Count != 0 || result.CommandReady.Count != 1 ||
				result.Samples[0].Error == "" || result.Samples[0].Steps[0].Passed ||
				result.CleanupErrors != 0 || result.Cleanup.Count != 1 || posts.Load() != 1 || deletes.Load() != 1 {
				t.Fatalf("report=%+v error=%v posts=%d deletes=%d", result, err, posts.Load(), deletes.Load())
			}
		})
	}
}

func TestRunRejectsContextHTTPAndEnvelopeFailures(t *testing.T) {
	for _, tc := range []struct {
		name    string
		status  int
		payload string
	}{
		{"server-error", 503, `{"success":false,"error":{"code":"unavailable"}}`},
		{"wrong-status", 200, `{"success":true,"data":{}}`},
		{"api-error", 201, `{"success":false,"error":{"code":"create_failed"}}`},
		{"missing-data", 201, `{"success":true}`},
		{"readiness-only", 201, `{"success":true,"data":{"ready":true}}`},
		{"malformed", 201, `{"success":`},
		{"trailing-json", 201, `{"success":true,"data":{}} {}`},
		{"oversized", 201, strings.Repeat("x", (1<<20)+1)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, posts, deletes := commandServer(t, func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.payload))
			})
			result, err := run(context.Background(), cfg)
			if err == nil || result.Passed || result.FirstCommandErrors != 1 || result.CleanupErrors != 0 ||
				posts.Load() != 1 || deletes.Load() != 1 {
				t.Fatalf("report=%+v error=%v posts=%d deletes=%d", result, err, posts.Load(), deletes.Load())
			}
		})
	}
}

func TestCommandTimeoutAndCancellationDoNotRetryAndStillClean(t *testing.T) {
	for _, mode := range []string{"timeout-before-headers", "timeout-in-body", "cancel-run"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			release := make(chan struct{})
			cfg, posts, deletes := commandServer(t, func(w http.ResponseWriter, r *http.Request) {
				assertCommandRequest(t, r, shellWorkload().Steps[0].Argv, 15)
				if mode == "timeout-in-body" {
					w.WriteHeader(http.StatusCreated)
					_, _ = w.Write([]byte(`{"success":true,"data":`))
					w.(http.Flusher).Flush()
				}
				if mode == "cancel-run" {
					cancel()
				}
				<-release
			})
			defer close(release)
			cfg.requestTimeout = 100 * time.Millisecond
			if mode == "cancel-run" {
				cfg.batches = 2
				cfg.settle = time.Second
			}
			result, err := run(ctx, cfg)
			if err == nil || result.Passed || result.FirstCommand.Count != 0 || result.CleanupErrors != 0 ||
				result.Cleanup.Count != 1 || posts.Load() != 1 || deletes.Load() != 1 || result.CompletedAt.IsZero() ||
				!strings.Contains(result.Samples[0].Error, "context") || result.RequestTimeout != cfg.requestTimeout {
				t.Fatalf("report=%+v error=%v posts=%d deletes=%d", result, err, posts.Load(), deletes.Load())
			}
			if mode == "cancel-run" && (result.Errors != 2 || result.FirstCommandErrors != 2) {
				t.Fatalf("unattempted sample was not counted: %+v", result)
			}
		})
	}
}

func TestCommandConnectionLossIsAmbiguousAndNeverRetried(t *testing.T) {
	cfg, posts, deletes := commandServer(t, func(w http.ResponseWriter, r *http.Request) {
		conn, _, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Error(err)
			return
		}
		_ = conn.Close()
	})
	result, err := run(context.Background(), cfg)
	if err == nil || result.Passed || posts.Load() != 1 || deletes.Load() != 1 || result.Cleanup.Count != 1 {
		t.Fatalf("report=%+v error=%v posts=%d deletes=%d", result, err, posts.Load(), deletes.Load())
	}
}

func TestCommandRedirectDoesNotReplayPublicPOST(t *testing.T) {
	var redirected atomic.Int64
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		redirected.Add(1)
		_ = spec.WriteSuccess(w, http.StatusCreated, commandResponse("ctx-redirected", shellWorkload().Steps[0].ExpectStdout))
	}))
	defer target.Close()
	cfg, posts, deletes := commandServer(t, func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusTemporaryRedirect)
	})
	cfg.client = newHTTPClient(1, cfg.requestTimeout)
	t.Cleanup(cfg.client.CloseIdleConnections)
	result, err := run(context.Background(), cfg)
	if err == nil || result.Passed || result.FirstCommandErrors != 1 || redirected.Load() != 0 ||
		posts.Load() != 1 || deletes.Load() != 1 || result.Cleanup.Count != 1 {
		t.Fatalf("report=%+v error=%v redirected=%d", result, err, redirected.Load())
	}
}

func TestPublicMetricsIncludeBothCompleteResponseBodies(t *testing.T) {
	var claimBodyWait, commandBodyWait atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/contexts"):
			w.WriteHeader(http.StatusCreated)
			w.(http.Flusher).Flush()
			started := time.Now()
			<-time.After(20 * time.Millisecond)
			commandBodyWait.Store(int64(time.Since(started)))
			// Headers are already sent; write just the normal success envelope body.
			_ = json.NewEncoder(w).Encode(map[string]any{"success": true, "data": commandResponse("ctx-body", shellWorkload().Steps[0].ExpectStdout)})
		case r.Method == http.MethodPost:
			w.Header().Set("Server-Timing", "sandbox0-command-ready;dur=0.001")
			w.Header().Set("Sandbox0-Command-Ready-SLO", "met")
			w.WriteHeader(http.StatusCreated)
			w.(http.Flusher).Flush()
			started := time.Now()
			<-time.After(20 * time.Millisecond)
			claimBodyWait.Store(int64(time.Since(started)))
			_ = json.NewEncoder(w).Encode(map[string]any{"success": true, "data": claimResponse{SandboxID: "sandbox-body"}})
		case r.Method == http.MethodDelete:
			_ = spec.WriteSuccess(w, http.StatusAccepted, struct{}{})
		case r.Method == http.MethodGet:
			_ = spec.WriteError(w, http.StatusNotFound, spec.CodeNotFound, "sandbox is absent")
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()
	cfg := config{endpoint: server.URL + "/api/v1/sandboxes", token: "test-token", body: []byte(`{}`),
		batches: 1, concurrency: 1, requestTimeout: time.Second, cleanupTimeout: time.Second,
		cleanupPoll: 10 * time.Millisecond, hardLimit: time.Second, p50Target: 500 * time.Millisecond, client: server.Client()}
	result, err := run(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	s := result.Samples[0]
	if result.CommandReady.Max != time.Microsecond || s.WallDuration < time.Duration(claimBodyWait.Load()) ||
		s.Steps[0].Duration < time.Duration(commandBodyWait.Load()) || s.FirstCommandDuration < s.WallDuration+s.Steps[0].Duration {
		t.Fatalf("metric boundaries=%+v claim_body_wait=%d command_body_wait=%d", s, claimBodyWait.Load(), commandBodyWait.Load())
	}
	step := s.Steps[0]
	if s.ClaimStartedAt.IsZero() || !s.ClaimCompletedAt.After(s.ClaimStartedAt) ||
		step.StartedAt.Before(s.ClaimCompletedAt) || !step.CompletedAt.After(step.StartedAt) ||
		s.ClaimCompletedAt.Sub(s.ClaimStartedAt) < time.Duration(claimBodyWait.Load()) ||
		step.CompletedAt.Sub(step.StartedAt) < time.Duration(commandBodyWait.Load()) {
		t.Fatalf("diagnostic timestamps must span response bodies and separate claim from command: %+v", s)
	}
}

func TestMissingCanonicalTimingStillFailsAndCleansClaim(t *testing.T) {
	var posts, deletes atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			posts.Add(1)
			_ = spec.WriteSuccess(w, http.StatusCreated, claimResponse{SandboxID: "sandbox-no-timing"})
		case http.MethodDelete:
			deletes.Add(1)
			_ = spec.WriteSuccess(w, http.StatusAccepted, struct{}{})
		case http.MethodGet:
			_ = spec.WriteError(w, http.StatusNotFound, spec.CodeNotFound, "sandbox is absent")
		}
	}))
	defer server.Close()
	cfg := config{endpoint: server.URL + "/api/v1/sandboxes", token: "test-token", body: []byte(`{}`),
		batches: 1, concurrency: 1, requestTimeout: time.Second, cleanupTimeout: time.Second,
		cleanupPoll: 10 * time.Millisecond, hardLimit: time.Second, p50Target: 500 * time.Millisecond, client: server.Client()}
	result, err := run(context.Background(), cfg)
	if err == nil || result.Passed || result.CommandReady.Count != 0 || result.FirstCommandErrors != 1 ||
		len(result.Samples[0].Steps) != 0 || posts.Load() != 1 || deletes.Load() != 1 || result.Cleanup.Count != 1 {
		t.Fatalf("report=%+v error=%v", result, err)
	}
}

func TestRealCommandLimitIsSeparateFromCanonicalReadiness(t *testing.T) {
	cfg, _, _ := commandServer(t, func(w http.ResponseWriter, r *http.Request) { serveDefaultCommand(t, w, r) })
	cfg.firstCommandHardLimit = time.Nanosecond
	result, err := run(context.Background(), cfg)
	if err == nil || result.Passed || result.Errors != 0 || result.SLOMisses != 0 || result.WallMisses != 0 ||
		result.FirstCommandMisses != 1 || result.FirstCommandErrors != 0 || result.FirstCommand.Count != 1 ||
		result.CommandReady.Max != time.Microsecond {
		t.Fatalf("report=%+v error=%v", result, err)
	}
	s := result.Samples[0]
	if s.FirstCommandDuration < s.WallDuration+s.Steps[0].Duration || s.WorkloadDuration != s.FirstCommandDuration ||
		result.FirstCommand.Max != s.FirstCommandDuration || result.Wall.Max != s.WallDuration {
		t.Fatalf("metric boundaries incorrect: %+v", s)
	}
}

func TestOrderedWorkloadPreservesArgvAndRequiresEveryStep(t *testing.T) {
	for _, failure := range []string{"none", "second-exit", "duplicate-context"} {
		t.Run(failure, func(t *testing.T) {
			work := workload{Name: "custom-agent", Steps: []commandStep{
				{Name: "node", Argv: []string{"/opt/node", "literal space", `$(touch /tmp/not-executed); '" $HOME`, "\n", ""}, ExpectStdout: "node ready\n"},
				{Name: "codex", Argv: []string{"/opt/codex", "--version"}, StdoutContains: "codex-cli"},
			}}
			var index atomic.Int64
			cfg, posts, deletes := commandServer(t, func(w http.ResponseWriter, r *http.Request) {
				i := int(index.Add(1)) - 1
				if i >= len(work.Steps) {
					t.Error("unexpected retry")
					w.WriteHeader(500)
					return
				}
				assertCommandRequest(t, r, work.Steps[i].Argv, 3)
				body := commandResponse(fmt.Sprintf("ctx-%d", i), "node ready\n")
				if i == 1 {
					body["stdout"] = "codex-cli 1.2.3\n"
					if failure == "second-exit" {
						body["exit_code"] = 3
					}
					if failure == "duplicate-context" {
						body["id"] = "ctx-0"
					}
				}
				_ = spec.WriteSuccess(w, http.StatusCreated, body)
			})
			cfg.workload, cfg.contextTTL = work, 3*time.Second
			result, err := run(context.Background(), cfg)
			if (err == nil) != (failure == "none") || result.Passed != (failure == "none") ||
				result.FirstCommand.Count != 1 || result.FirstCommandErrors != 0 || posts.Load() != 2 || deletes.Load() != 1 ||
				result.Samples[0].WorkloadDuration <= result.Samples[0].FirstCommandDuration {
				t.Fatalf("report=%+v error=%v", result, err)
			}
			if failure != "none" && (result.Errors != 1 || result.WorkloadWall.Count != 0) {
				t.Fatalf("later failed step was accepted: %+v", result)
			}
			data, _ := json.Marshal(work)
			digest := sha256.Sum256(data)
			if result.WorkloadSHA256 != fmt.Sprintf("%x", digest[:]) || !reflect.DeepEqual(result.Workload, work) {
				t.Fatalf("workload identity missing: %+v", result)
			}
		})
	}
}

type commandRoundTripFunc func(*http.Request) (*http.Response, error)

func (f commandRoundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

func TestCodingAgentSharesTenSecondBudgetAndCountsLaterTimeout(t *testing.T) {
	work, err := loadWorkload("coding-agent", "")
	if err != nil {
		t.Fatal(err)
	}
	cfg, posts, deletes := commandServer(t, func(w http.ResponseWriter, r *http.Request) {
		assertCommandRequest(t, r, work.Steps[0].Argv, 15)
		_ = spec.WriteSuccess(w, http.StatusCreated, commandResponse("ctx-node", work.Steps[0].ExpectStdout))
	})
	cfg.workload, cfg.requestTimeout = work, 10*time.Second
	transport := cfg.client.Transport
	var deadlines []time.Time
	var sawTimeout bool
	cfg.client.Transport = commandRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/contexts") {
			deadline, ok := r.Context().Deadline()
			if !ok || time.Until(deadline) > 10*time.Second {
				t.Error("command request lacks its bounded ten-second deadline")
			}
			deadlines = append(deadlines, deadline)
			if len(deadlines) == 2 {
				assertCommandRequest(t, r, work.Steps[1].Argv, 15)
				// Inject the transport timeout deterministically without waiting ten
				// seconds. Exact deadline equality proves the second step's HTTP
				// request inherits the first step's budget instead of renewing it.
				sawTimeout = true
				return nil, context.DeadlineExceeded
			}
		}
		return transport.RoundTrip(r)
	})
	result, err := run(context.Background(), cfg)
	if len(deadlines) != 2 || !deadlines[0].Equal(deadlines[1]) {
		t.Fatalf("Node/Codex did not share one deadline: %v", deadlines)
	}
	if err == nil || result.Passed || !sawTimeout || result.Errors != 1 || result.FirstCommandErrors != 0 ||
		result.FirstCommand.Count != 1 || result.WorkloadWall.Count != 0 || result.RequestTimeout != 10*time.Second ||
		result.CleanupErrors != 0 || result.Cleanup.Count != 1 || posts.Load() != 1 || deletes.Load() != 1 {
		t.Fatalf("report=%+v error=%v", result, err)
	}
	steps := result.Samples[0].Steps
	if len(steps) != 2 || !steps[0].Passed || steps[1].Passed || steps[1].ExitCode != nil ||
		!strings.Contains(steps[1].Error, context.DeadlineExceeded.Error()) ||
		!strings.Contains(result.Samples[0].Error, "codex-version") {
		t.Fatalf("later timeout evidence=%+v", result.Samples[0])
	}
}

func TestMissingCommandEvidenceCannotPassAccounting(t *testing.T) {
	cfg, _, _ := commandServer(t, func(w http.ResponseWriter, r *http.Request) { serveDefaultCommand(t, w, r) })
	result, err := run(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*sample){
		func(s *sample) { s.Steps = nil },
		func(s *sample) { s.FirstCommandDuration = 0 },
		func(s *sample) { s.Steps[0].ExitCode = nil },
		func(s *sample) { s.Steps[0].Stdout = nil },
		func(s *sample) { s.Steps[0].Passed = false },
	} {
		s := result.Samples[0]
		s.Steps = append([]commandSample(nil), s.Steps...)
		mutate(&s)
		if first, all := successfulCommands(s, cfg.workload); first || all {
			t.Fatalf("missing command evidence passed: %+v", s)
		}
	}
}

func TestWorkloadConfigurationBoundsAndBuiltins(t *testing.T) {
	for _, name := range []string{"none", "shell", "coding-agent"} {
		work, err := loadWorkload(name, "")
		if err != nil || work.validate() != nil {
			t.Fatalf("work=%+v error=%v", work, err)
		}
	}
	if _, err := loadWorkload("unknown", ""); err == nil {
		t.Fatal("unknown workload accepted")
	}
	for _, payload := range []string{
		`{}`, `null`, `{"name":"empty","steps":[]}`,
		`{"name":"bad","steps":[{"name":"x","argv":[],"expect_stdout":"x"}]}`,
		`{"name":"bad","steps":[{"name":"x","argv":["echo"]}]}`,
		`{"name":"bad","steps":[{"name":"x","argv":["echo"],"expect_stdout":"x","stdout_contains":"x"}]}`,
		`{"name":"bad","steps":[{"name":"x","argv":["echo\u0000"],"expect_stdout":"x"}]}`,
		`{"name":"bad","stepz":[]}`, `{} {}`, strings.Repeat("x", (64<<10)+1),
	} {
		path := filepath.Join(t.TempDir(), "workload.json")
		if err := os.WriteFile(path, []byte(payload), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := loadWorkload("shell", path); err == nil {
			t.Fatalf("invalid workload accepted: %.100s", payload)
		}
	}
	for _, ttl := range []time.Duration{-time.Second, 0, time.Millisecond, 1500 * time.Millisecond, 61 * time.Second} {
		cfg := config{workload: shellWorkload(), contextTTL: ttl, firstCommandHardLimit: time.Second}
		if cfg.validateWorkload() == nil {
			t.Fatalf("invalid TTL accepted: %s", ttl)
		}
	}
	work := shellWorkload()
	work.Steps = append(work.Steps, work.Steps[0])
	if work.validate() == nil {
		t.Fatal("duplicate step name accepted")
	}
}

func TestCommandEnvironmentIsLiteralScopedAndConfirmed(t *testing.T) {
	want := map[string]string{"MODE": "$(touch ignored); $HOME `ignored`\n", "EMPTY": ""}
	for _, mode := range []string{"confirmed", "missing", "wrong", "missing-empty"} {
		t.Run(mode, func(t *testing.T) {
			cfg, posts, deletes := commandServer(t, func(w http.ResponseWriter, r *http.Request) {
				var body struct {
					Cmd struct {
						Command []string `json:"command"`
					} `json:"cmd"`
					EnvVars map[string]string `json:"env_vars"`
				}
				decoder := json.NewDecoder(r.Body)
				if err := decoder.Decode(&body); err != nil || !reflect.DeepEqual(body.EnvVars, want) || !reflect.DeepEqual(body.Cmd.Command, []string{"node", "-v"}) {
					t.Errorf("literal command/environment not preserved: %+v, %v", body, err)
				}
				response := commandResponse("ctx-env", "v22.0.0\n")
				env := map[string]string{"MODE": want["MODE"], "EMPTY": "", "UNREQUESTED_SECRET": "do-not-record"}
				switch mode {
				case "missing":
					env = nil
				case "wrong":
					env["MODE"] = "not-requested"
				case "missing-empty":
					delete(env, "EMPTY")
				}
				response["env_vars"] = env
				_ = spec.WriteSuccess(w, http.StatusCreated, response)
			})
			originalClaim := append([]byte(nil), cfg.body...)
			cfg.workload = workload{Name: "node", Steps: []commandStep{{Name: "node", Argv: []string{"node", "-v"}, ExpectStdout: "v22.0.0\n", EnvVars: want}}}
			result, err := run(context.Background(), cfg)
			if (err == nil) != (mode == "confirmed") || result.Passed != (mode == "confirmed") || posts.Load() != 1 || deletes.Load() != 1 || !reflect.DeepEqual(cfg.body, originalClaim) {
				t.Fatalf("report=%+v err=%v posts=%d deletes=%d", result, err, posts.Load(), deletes.Load())
			}
			encoded, _ := json.Marshal(result)
			if strings.Contains(string(encoded), "UNREQUESTED_SECRET") || strings.Contains(string(encoded), "do-not-record") {
				t.Fatal("inherited environment leaked to report")
			}
			if mode == "confirmed" {
				step := &result.Samples[0].Steps[0]
				if !reflect.DeepEqual(step.EnvVars, want) {
					t.Fatalf("environment evidence = %+v", step.EnvVars)
				}
				step.EnvVars = nil
				if first, all := successfulCommands(result.Samples[0], cfg.workload); first || all {
					t.Fatal("missing environment evidence passed final accounting")
				}
			}
		})
	}
}

func TestCommandEnvironmentValidationAndWorkloadHash(t *testing.T) {
	base := workload{Name: "node", Steps: []commandStep{{Name: "node", Argv: []string{"node", "-v"}, StdoutContains: "v"}}}
	before := workloadSHA(base)
	for _, env := range []map[string]string{{"": "x"}, {"A=B": "x"}, {"A\x00": "x"}, {"A": "x\x00"}, {"A": strings.Repeat("x", 64<<10)}} {
		base.Steps[0].EnvVars = env
		if base.validate() == nil {
			t.Fatalf("invalid environment accepted: keys=%d", len(env))
		}
	}
	base.Steps[0].EnvVars = map[string]string{}
	if base.validate() != nil || workloadSHA(base) != before {
		t.Fatal("empty optional environment changed existing workload identity")
	}
	base.Steps[0].EnvVars = map[string]string{"MODE": "on", "EMPTY": ""}
	if base.validate() != nil || workloadSHA(base) == before {
		t.Fatal("environment missing from workload identity")
	}
	body, _ := json.Marshal(base)
	path := filepath.Join(t.TempDir(), "workload.json")
	if err := os.WriteFile(path, body, 0600); err != nil {
		t.Fatal(err)
	}
	got, err := loadWorkload("shell", path)
	if err != nil || !reflect.DeepEqual(got, base) {
		t.Fatalf("workload environment round trip: %+v, %v", got, err)
	}
}
