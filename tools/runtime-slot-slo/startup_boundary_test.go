package main

import (
	"net/http"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
)

func TestStartupOnlyDoesNotDispatchOrInventCommandEvidence(t *testing.T) {
	cfg, posts, deletes := commandServer(t, func(w http.ResponseWriter, r *http.Request) {
		t.Error("startup-only acceptance must not execute a user program")
	})
	cfg.workload = workload{Name: "none"}
	cfg.firstCommandHardLimit = 0
	result, err := run(t.Context(), cfg)
	if err != nil || !result.Passed || !result.StartupPassed || result.ClaimErrors != 0 ||
		result.WorkloadMeasured || result.WorkloadPassed || result.FirstCommand.Count != 0 ||
		result.FirstCommandErrors != 0 || result.FirstCommandMisses != 0 || result.WorkloadWall.Count != 0 ||
		posts.Load() != 0 || deletes.Load() != 1 || result.Cleanup.Count != 1 || len(result.Samples[0].Steps) != 0 {
		t.Fatalf("report=%+v error=%v commands=%d deletes=%d", result, err, posts.Load(), deletes.Load())
	}
}

func TestSlowUserExecutableDoesNotFailSandboxStartup(t *testing.T) {
	cfg, _, _ := commandServer(t, func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(150 * time.Millisecond)
		serveDefaultCommand(t, w, r)
	})
	cfg.hardLimit = 100 * time.Millisecond
	cfg.p50Target = 50 * time.Millisecond
	cfg.firstCommandHardLimit = 0
	result, err := run(t.Context(), cfg)
	if err != nil || !result.StartupPassed || !result.Passed || !result.WorkloadPassed ||
		result.FirstCommand.Max <= cfg.hardLimit || result.FirstCommandMisses != 0 || result.FirstCommandHardLimit != 0 {
		t.Fatalf("report=%+v error=%v", result, err)
	}
}

func TestUserWorkloadFailureHasIndependentResult(t *testing.T) {
	for _, mode := range []string{"bad-exit", "explicit-workload-limit", "timeout"} {
		t.Run(mode, func(t *testing.T) {
			cfg, _, deletes := commandServer(t, func(w http.ResponseWriter, r *http.Request) {
				if mode == "timeout" {
					select {
					case <-r.Context().Done():
					case <-time.After(200 * time.Millisecond):
					}
					return
				}
				response := commandResponse("ctx-shell", shellWorkload().Steps[0].ExpectStdout)
				if mode == "bad-exit" {
					response["exit_code"] = 1
				}
				_ = spec.WriteSuccess(w, http.StatusCreated, response)
			})
			cfg.firstCommandHardLimit = 0
			if mode == "explicit-workload-limit" {
				cfg.firstCommandHardLimit = time.Nanosecond
			}
			if mode == "timeout" {
				cfg.requestTimeout = 100 * time.Millisecond
			}
			result, err := run(t.Context(), cfg)
			if err == nil || result.Passed || !result.StartupPassed || result.ClaimErrors != 0 ||
				!result.WorkloadMeasured || result.WorkloadPassed || deletes.Load() != 1 || result.CleanupErrors != 0 {
				t.Fatalf("report=%+v error=%v", result, err)
			}
		})
	}
}

func TestEngineeringP50TargetIsNotTheStartupHardLimit(t *testing.T) {
	cfg, _, _ := commandServer(t, func(w http.ResponseWriter, r *http.Request) {
		serveDefaultCommand(t, w, r)
	})
	cfg.p50Target = time.Nanosecond
	cfg.firstCommandHardLimit = 0
	result, err := run(t.Context(), cfg)
	if err != nil || !result.StartupPassed || !result.Passed || result.P50TargetMet {
		t.Fatalf("report=%+v error=%v", result, err)
	}
}

func TestWorkloadLimitRequiresMeasuredCommands(t *testing.T) {
	for _, cfg := range []config{
		{workload: workload{Name: "none"}, contextTTL: 15 * time.Second, firstCommandHardLimit: time.Second},
		{workload: shellWorkload(), contextTTL: 15 * time.Second, firstCommandHardLimit: -time.Second},
	} {
		if cfg.validateWorkload() == nil {
			t.Fatalf("invalid workload limit accepted: %+v", cfg)
		}
	}
	if err := (workload{Name: "none", Steps: shellWorkload().Steps}).validate(); err == nil {
		t.Fatal("startup-only workload accepted executable steps")
	}
}
