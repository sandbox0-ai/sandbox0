package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
)

func mixedTestCases() []claimCase {
	result := make([]claimCase, 2)
	for index, name := range []string{"node-a", "node-b"} {
		result[index] = claimCase{Name: name, ClaimBody: json.RawMessage(`{"template":"` + name + `"}`),
			Workload: workload{Name: name, Steps: []commandStep{{Name: "node", Argv: []string{"node", "-v"}, ExpectStdout: name + "\n"}}}}
	}
	return result
}

func TestMixedCasesUseOwnBodiesAndCompletionEvidence(t *testing.T) {
	for _, mismatch := range []bool{false, true} {
		t.Run(fmt.Sprint(mismatch), func(t *testing.T) {
			cases := mixedTestCases()
			var claims, commands, deletes atomic.Int64
			var identities sync.Map
			mux := http.NewServeMux()
			mux.HandleFunc("POST /api/v1/sandboxes", func(w http.ResponseWriter, r *http.Request) {
				body, _ := io.ReadAll(r.Body)
				name := ""
				for _, current := range cases {
					if string(body) == string(current.ClaimBody) {
						name = current.Name
					}
				}
				if name == "" {
					t.Error("claim body does not match a case")
				}
				id := fmt.Sprintf("sandbox-%d", claims.Add(1))
				identities.Store(id, name)
				w.Header().Set("Server-Timing", "sandbox0-command-ready;dur=0.001")
				w.Header().Set("Sandbox0-Command-Ready-SLO", "met")
				_ = spec.WriteSuccess(w, http.StatusCreated, claimResponse{SandboxID: id})
			})
			mux.HandleFunc("POST /api/v1/sandboxes/{id}/contexts", func(w http.ResponseWriter, r *http.Request) {
				commands.Add(1)
				assertCommandRequest(t, r, []string{"node", "-v"}, 15)
				name, _ := identities.Load(r.PathValue("id"))
				stdout := name.(string) + "\n"
				if mismatch && name == "node-b" {
					stdout = "node-a\n"
				}
				_ = spec.WriteSuccess(w, http.StatusCreated, commandResponse("ctx-"+r.PathValue("id"), stdout))
			})
			mux.HandleFunc("DELETE /api/v1/sandboxes/{id}", func(w http.ResponseWriter, r *http.Request) {
				deletes.Add(1)
				_ = spec.WriteSuccess(w, http.StatusAccepted, struct{}{})
			})
			mux.HandleFunc("GET /api/v1/sandboxes/{id}", func(w http.ResponseWriter, r *http.Request) {
				_ = spec.WriteError(w, http.StatusNotFound, spec.CodeNotFound, "sandbox is absent")
			})
			server := httptest.NewServer(mux)
			defer server.Close()
			cfg := config{endpoint: server.URL + "/api/v1/sandboxes", token: "test-token", cases: cases,
				batches: 2, concurrency: 3, requestTimeout: time.Second, contextTTL: 15 * time.Second,
				cleanupTimeout: time.Second, cleanupPoll: 10 * time.Millisecond, hardLimit: time.Second,
				p50Target: 500 * time.Millisecond, client: server.Client()}
			result, err := run(t.Context(), cfg)
			if (err != nil) != mismatch || result.Passed == mismatch || !result.StartupPassed ||
				result.WorkloadPassed == mismatch || result.Version != 8 || result.CleanupErrors != 0 ||
				result.ClaimBodySHA256 != "" || result.WorkloadSHA256 != "" || len(result.Workload.Steps) != 0 ||
				claims.Load() != 6 || commands.Load() != 6 || deletes.Load() != 6 {
				t.Fatalf("report=%+v err=%v claims=%d commands=%d deletes=%d", result, err, claims.Load(), commands.Load(), deletes.Load())
			}
			for index, current := range result.Samples {
				selected := cases[index%len(cases)]
				if current.CaseName != selected.Name || current.ClaimBodySHA256 != digestBytes(selected.ClaimBody) ||
					current.WorkloadSHA256 != workloadSHA(selected.Workload) || current.Steps[0].StdoutMatched == (mismatch && current.CaseName == "node-b") {
					t.Fatalf("sample %d case evidence differs: %+v", index, current)
				}
			}
			for _, evidence := range result.Cases {
				if evidence.ExpectedSamples != 3 || evidence.WorkloadSHA256 != workloadSHA(evidence.Workload) {
					t.Fatalf("inventory=%+v", evidence)
				}
			}
			if mismatch && (result.Errors != 3 || result.FirstCommandErrors != 3 || result.FirstCommand.Count != 3) {
				t.Fatalf("case failures hidden: %+v", result)
			}
		})
	}
}

func TestMixedCasesRejectInvalidConfiguration(t *testing.T) {
	for _, mode := range []string{"single", "width", "duplicate", "blank", "array-body", "large-body", "no-command", "invalid-workload"} {
		t.Run(mode, func(t *testing.T) {
			cfg := config{cases: mixedTestCases(), concurrency: 2, contextTTL: 15 * time.Second}
			switch mode {
			case "single":
				cfg.cases = cfg.cases[:1]
			case "width":
				cfg.concurrency = 1
			case "duplicate":
				cfg.cases[1].Name = cfg.cases[0].Name
			case "blank":
				cfg.cases[0].Name = " "
			case "array-body":
				cfg.cases[0].ClaimBody = json.RawMessage(`[]`)
			case "large-body":
				cfg.cases[0].ClaimBody = json.RawMessage(`{"value":"` + strings.Repeat("x", 1<<20) + `"}`)
			case "no-command":
				cfg.cases[0].Workload = workload{Name: "none"}
			case "invalid-workload":
				cfg.cases[0].Workload.Steps[0].ExpectStdout = ""
			}
			if cfg.validateWorkload() == nil {
				t.Fatal("invalid mixed configuration accepted")
			}
		})
	}
}

func TestLoadCasesIsStrictAndHashesSentBytes(t *testing.T) {
	payload, _ := json.MarshalIndent(struct {
		Version int         `json:"version"`
		Cases   []claimCase `json:"cases"`
	}{1, mixedTestCases()}, "", "    ")
	path := filepath.Join(t.TempDir(), "cases.json")
	for _, content := range []string{string(payload), string(payload) + `{}`, `{"version":2,"cases":[]}`,
		`{"version":1,"cases":[],"extra":true}`, `{"version":1,"cases":[]}`, strings.Repeat("x", (4<<20)+1)} {
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
		cases, err := loadCases(path)
		if content != string(payload) {
			if err == nil {
				t.Fatal("invalid cases file accepted")
			}
			continue
		}
		if err != nil || len(cases) != 2 || string(cases[0].ClaimBody) != string(mixedTestCases()[0].ClaimBody) {
			t.Fatalf("cases=%+v error=%v", cases, err)
		}
	}
}

func TestCanceledMixedRunRetainsUnattemptedCaseIdentity(t *testing.T) {
	cfg, posts, deletes := commandServer(t, nil)
	cfg.cases, cfg.concurrency = mixedTestCases(), 2
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	result, err := run(ctx, cfg)
	if err == nil || result.ClaimErrors != 2 || posts.Load() != 0 || deletes.Load() != 0 {
		t.Fatalf("report=%+v error=%v", result, err)
	}
	for index, current := range result.Samples {
		if current.CaseName != cfg.cases[index].Name || current.ClaimBodySHA256 == "" || !current.ClaimStartedAt.IsZero() {
			t.Fatalf("unattempted evidence=%+v", current)
		}
	}
}
