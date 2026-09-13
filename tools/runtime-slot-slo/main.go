// Command runtime-slot-slo validates the public regional claim route against
// the trusted ingress-to-procd timer, with optional public executable diagnostics.
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
)

const commandReadyTimingMetric = "sandbox0-command-ready"

type config struct {
	endpoint              string
	token                 string
	body                  []byte
	batches               int
	concurrency           int
	requestTimeout        time.Duration
	cleanupTimeout        time.Duration
	cleanupPoll           time.Duration
	settle                time.Duration
	hardLimit             time.Duration
	p50Target             time.Duration
	label                 string
	client                *http.Client
	workload              workload
	contextTTL            time.Duration
	firstCommandHardLimit time.Duration
	cases                 []claimCase
	caseName              string
}

type sample struct {
	Index                int             `json:"index"`
	Batch                int             `json:"batch"`
	Lane                 int             `json:"lane"`
	CaseName             string          `json:"case_name,omitempty"`
	ClaimBodySHA256      string          `json:"claim_body_sha256,omitempty"`
	WorkloadSHA256       string          `json:"workload_sha256,omitempty"`
	SandboxID            string          `json:"sandbox_id,omitempty"`
	ClaimStartedAt       time.Time       `json:"claim_started_at"`
	ClaimCompletedAt     time.Time       `json:"claim_completed_at"`
	WallDuration         time.Duration   `json:"wall_duration_ns"`
	CommandDuration      time.Duration   `json:"command_ready_duration_ns"`
	WithinSLO            bool            `json:"within_slo"`
	Error                string          `json:"error,omitempty"`
	CleanupError         string          `json:"cleanup_error,omitempty"`
	CleanupDuration      time.Duration   `json:"cleanup_duration_ns,omitempty"`
	ClaimSucceeded       bool            `json:"claim_succeeded"`
	FirstCommandDuration time.Duration   `json:"first_command_duration_ns"`
	WorkloadDuration     time.Duration   `json:"workload_duration_ns"`
	Steps                []commandSample `json:"steps"`
}

type distribution struct {
	Count int           `json:"count"`
	Min   time.Duration `json:"min_ns"`
	P50   time.Duration `json:"p50_ns"`
	P95   time.Duration `json:"p95_ns"`
	P99   time.Duration `json:"p99_ns"`
	Max   time.Duration `json:"max_ns"`
}

type report struct {
	Cases                 []caseEvidence `json:"cases,omitempty"`
	Version               int            `json:"version"`
	ExecutableSHA256      string         `json:"executable_sha256"`
	ClaimBodySHA256       string         `json:"claim_body_sha256"`
	Label                 string         `json:"label,omitempty"`
	StartedAt             time.Time      `json:"started_at"`
	CompletedAt           time.Time      `json:"completed_at"`
	Endpoint              string         `json:"endpoint"`
	Batches               int            `json:"batches"`
	Concurrency           int            `json:"concurrency"`
	RequestTimeout        time.Duration  `json:"request_timeout_ns"`
	CleanupTimeout        time.Duration  `json:"cleanup_timeout_ns"`
	CleanupPoll           time.Duration  `json:"cleanup_poll_ns"`
	BatchSettle           time.Duration  `json:"batch_settle_ns"`
	HardLimit             time.Duration  `json:"hard_limit_ns"`
	P50Target             time.Duration  `json:"p50_target_ns"`
	CommandReady          distribution   `json:"command_ready"`
	Wall                  distribution   `json:"wall"`
	Cleanup               distribution   `json:"cleanup"`
	Errors                int            `json:"errors"`
	SLOMisses             int            `json:"slo_misses"`
	WallMisses            int            `json:"wall_misses"`
	CleanupErrors         int            `json:"cleanup_errors"`
	Passed                bool           `json:"passed"`
	StartupPassed         bool           `json:"startup_passed"`
	ClaimErrors           int            `json:"claim_errors"`
	P50TargetMet          bool           `json:"p50_target_met"`
	WorkloadMeasured      bool           `json:"workload_measured"`
	WorkloadPassed        bool           `json:"workload_passed"`
	Samples               []sample       `json:"samples"`
	Workload              workload       `json:"workload"`
	WorkloadSHA256        string         `json:"workload_sha256"`
	ContextTTL            time.Duration  `json:"context_ttl_ns"`
	FirstCommandHardLimit time.Duration  `json:"first_command_hard_limit_ns"`
	FirstCommand          distribution   `json:"first_command"`
	WorkloadWall          distribution   `json:"workload_wall"`
	FirstCommandErrors    int            `json:"first_command_errors"`
	FirstCommandMisses    int            `json:"first_command_misses"`
}

type claimResponse struct {
	SandboxID string `json:"sandbox_id"`
}

var (
	executableSHAOnce sync.Once
	executableSHA     string
	executableSHAErr  error
)

func main() {
	var (
		endpoint          = flag.String("url", "", "full regional POST /api/v1/sandboxes URL")
		tokenFile         = flag.String("token-file", "", "file containing the public API bearer token")
		templateID        = flag.String("template", "default", "sandbox template ID")
		bodyFile          = flag.String("body-file", "", "optional complete claim JSON body")
		casesFile         = flag.String("cases-file", "", "version 1 mixed claim/workload cases; exclusive with template, body-file, and workload flags")
		batches           = flag.Int("batches", 1000, "number of synchronized request batches")
		concurrent        = flag.Int("concurrency", 1, "requests in each synchronized batch")
		timeout           = flag.Duration("request-timeout", 15*time.Second, "per-request timeout")
		cleanupTimeout    = flag.Duration("cleanup-timeout", 2*time.Minute, "maximum time for each DELETE to converge to public absence")
		cleanupPoll       = flag.Duration("cleanup-poll", 100*time.Millisecond, "public GET interval while waiting for cleanup convergence")
		settle            = flag.Duration("batch-settle", 0, "delay after cleanup before the next batch")
		hardLimit         = flag.Duration("hard-limit", time.Second, "maximum successful command-ready sample")
		p50Target         = flag.Duration("p50-target", 500*time.Millisecond, "engineering p50 target")
		output            = flag.String("output", "", "optional JSON report path; stdout is always written")
		label             = flag.String("label", "", "optional environment label included in the report")
		workloadName      = flag.String("workload", "shell", "diagnostic workload: none, shell, or coding-agent; none measures sandbox startup only")
		workloadFile      = flag.String("workload-file", "", "JSON workload with ordered command argv and stdout expectations; overrides --workload")
		contextTTL        = flag.Duration("context-ttl", 15*time.Second, "command context lifetime, whole seconds in [1s, 1m]")
		firstCommandLimit = flag.Duration("first-command-hard-limit", 0, "optional workload-only claim-start to first executable completion limit; 0 disables it, independent of sandbox startup")
	)
	flag.Parse()
	if *casesFile != "" {
		flag.Visit(func(f *flag.Flag) {
			switch f.Name {
			case "template", "body-file", "workload", "workload-file":
				fatal(fmt.Errorf("--cases-file cannot be combined with --%s", f.Name))
			}
		})
	}

	work, err := loadWorkload(*workloadName, *workloadFile)
	if err != nil {
		fatal(err)
	}
	body, err := claimBody(*templateID, *bodyFile)
	if err != nil {
		fatal(err)
	}
	token, err := bearerToken(*tokenFile)
	if err != nil {
		fatal(err)
	}
	cfg := config{
		endpoint: *endpoint, token: token, body: body, batches: *batches, concurrency: *concurrent,
		requestTimeout: *timeout, cleanupTimeout: *cleanupTimeout, cleanupPoll: *cleanupPoll,
		settle: *settle, hardLimit: *hardLimit, p50Target: *p50Target,
		label:    strings.TrimSpace(*label),
		workload: work, contextTTL: *contextTTL, firstCommandHardLimit: *firstCommandLimit,
	}
	if *casesFile != "" {
		cfg.cases, err = loadCases(*casesFile)
		if err != nil {
			fatal(err)
		}
	}
	if err := cfg.validate(); err != nil {
		fatal(err)
	}
	cfg.client = newHTTPClient(cfg.concurrency, cfg.requestTimeout)
	result, runErr := run(context.Background(), cfg)
	payload, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		fatal(err)
	}
	_, _ = os.Stdout.Write(append(payload, '\n'))
	if strings.TrimSpace(*output) != "" {
		if err := os.WriteFile(*output, append(payload, '\n'), 0o600); err != nil {
			fatal(fmt.Errorf("write report: %w", err))
		}
	}
	if runErr != nil {
		fatal(runErr)
	}
}

func (c config) validate() error {
	parsed, err := url.Parse(c.endpoint)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.User != nil {
		return errors.New("url must be an absolute HTTPS regional claim endpoint")
	}
	if parsed.Path != "/api/v1/sandboxes" || parsed.RawPath != "" {
		return errors.New("url path must be canonical /api/v1/sandboxes")
	}
	if parsed.RawQuery != "" || parsed.ForceQuery || parsed.Fragment != "" {
		return errors.New("url must not contain a query or fragment")
	}
	if c.token == "" {
		return errors.New("a bearer token is required")
	}
	if len(c.cases) == 0 && (len(c.body) == 0 || len(c.body) > 1<<20 || !json.Valid(c.body)) {
		return errors.New("claim body must be valid non-empty JSON no larger than 1 MiB")
	}
	if c.batches <= 0 || c.concurrency <= 0 || c.batches > 100000 || c.concurrency > 1024 || c.batches*c.concurrency > 1000000 {
		return errors.New("batches, concurrency, or total samples exceed the acceptance harness bounds")
	}
	if c.requestTimeout <= 0 || c.requestTimeout > time.Minute || c.settle < 0 || c.settle > time.Minute {
		return errors.New("request timeout or batch settle duration is outside its supported range")
	}
	if c.cleanupTimeout < time.Second || c.cleanupTimeout > 10*time.Minute ||
		c.cleanupPoll < 10*time.Millisecond || c.cleanupPoll > 5*time.Second || c.cleanupPoll > c.cleanupTimeout {
		return errors.New("cleanup timeout or poll interval is outside its supported range")
	}
	if c.hardLimit <= 0 || c.p50Target <= 0 || c.p50Target > c.hardLimit {
		return errors.New("p50 target must be positive and no greater than the hard limit")
	}
	return c.validateWorkload()
}

func run(ctx context.Context, cfg config) (report, error) {
	if cfg.client == nil {
		return report{}, errors.New("HTTP client is required")
	}
	// Internal callers receive the same default diagnostic workload as the CLI.
	if cfg.workload.Name == "" && cfg.workload.Steps == nil {
		cfg.workload = shellWorkload()
	}
	if cfg.contextTTL == 0 {
		cfg.contextTTL = 15 * time.Second
	}
	if err := cfg.validateWorkload(); err != nil {
		return report{}, err
	}
	executableDigest, err := currentExecutableSHA256()
	if err != nil {
		return report{}, fmt.Errorf("hash acceptance executable: %w", err)
	}
	bodyDigest := sha256.Sum256(cfg.body)
	workloadJSON, _ := json.Marshal(cfg.workload)
	workloadDigest := sha256.Sum256(workloadJSON)
	result := report{
		Version: 7, ExecutableSHA256: executableDigest,
		ClaimBodySHA256: fmt.Sprintf("%x", bodyDigest[:]),
		Label:           cfg.label, StartedAt: time.Now().UTC(), Endpoint: cfg.endpoint,
		Batches: cfg.batches, Concurrency: cfg.concurrency,
		RequestTimeout: cfg.requestTimeout, CleanupTimeout: cfg.cleanupTimeout,
		CleanupPoll: cfg.cleanupPoll, BatchSettle: cfg.settle,
		HardLimit: cfg.hardLimit, P50Target: cfg.p50Target,
		Samples:  make([]sample, cfg.batches*cfg.concurrency),
		Workload: cfg.workload, WorkloadSHA256: fmt.Sprintf("%x", workloadDigest[:]),
		ContextTTL: cfg.contextTTL, FirstCommandHardLimit: cfg.firstCommandHardLimit,
		WorkloadMeasured: len(cfg.workload.Steps) > 0,
	}
	if len(cfg.cases) > 0 {
		result.Version = 8
		result.Cases = cfg.caseInventory()
		// No single body or workload describes a heterogeneous cohort.
		result.ClaimBodySHA256, result.WorkloadSHA256 = "", ""
		result.Workload, result.WorkloadMeasured = workload{}, true
	}
	for index := range result.Samples {
		result.Samples[index] = sample{Index: index, Batch: index / cfg.concurrency, Lane: index % cfg.concurrency,
			Error: "sample was not attempted"}
		annotateCase(&result.Samples[index], cfg.forSample(index))
	}
	var cleanupErrors atomic.Int64
	var seenSandboxIDs sync.Map
batches:
	for batch := range cfg.batches {
		if ctx.Err() != nil {
			break
		}
		var wait sync.WaitGroup
		start := make(chan struct{})
		for lane := range cfg.concurrency {
			index := batch*cfg.concurrency + lane
			wait.Add(1)
			go func() {
				defer wait.Done()
				<-start
				result.Samples[index] = claim(ctx, cfg.forSample(index), index, batch, lane, &seenSandboxIDs)
			}()
		}
		close(start)
		wait.Wait()
		for lane := range cfg.concurrency {
			current := result.Samples[batch*cfg.concurrency+lane]
			if current.SandboxID == "" {
				continue
			}
			wait.Add(1)
			index := batch*cfg.concurrency + lane
			go func(sandboxID string, index int) {
				defer wait.Done()
				started := time.Now()
				// Cancellation of a command/run must not cancel deletion of its sandbox.
				if err := cleanupSandbox(context.WithoutCancel(ctx), cfg, sandboxID); err != nil {
					cleanupErrors.Add(1)
					result.Samples[index].CleanupError = err.Error()
				}
				result.Samples[index].CleanupDuration = time.Since(started)
			}(current.SandboxID, index)
		}
		wait.Wait()
		if cfg.settle > 0 && batch+1 < cfg.batches {
			select {
			case <-ctx.Done():
				// Continue to final accounting; unattempted samples cannot pass.
				break batches
			case <-time.After(cfg.settle):
			}
		}
	}
	result.CompletedAt = time.Now().UTC()
	result.CleanupErrors = int(cleanupErrors.Load())
	commandDurations := make([]time.Duration, 0, len(result.Samples))
	wallDurations := make([]time.Duration, 0, len(result.Samples))
	cleanupDurations := make([]time.Duration, 0, len(result.Samples))
	firstCommandDurations := make([]time.Duration, 0, len(result.Samples))
	workloadDurations := make([]time.Duration, 0, len(result.Samples))
	for index := range result.Samples {
		current := &result.Samples[index]
		if current.CleanupError == "" && current.SandboxID != "" {
			cleanupDurations = append(cleanupDurations, current.CleanupDuration)
		}
		firstOK, workloadOK := successfulCommands(*current, cfg.forSample(index).workload)
		if result.WorkloadMeasured && !workloadOK && current.Error == "" {
			current.Error = "workload completion evidence is missing or invalid"
		}
		if result.WorkloadMeasured && !firstOK {
			result.FirstCommandErrors++
		} else if result.WorkloadMeasured {
			firstCommandDurations = append(firstCommandDurations, current.FirstCommandDuration)
			if cfg.firstCommandHardLimit > 0 && current.FirstCommandDuration > cfg.firstCommandHardLimit {
				result.FirstCommandMisses++
			}
		}
		if result.WorkloadMeasured && workloadOK {
			workloadDurations = append(workloadDurations, current.WorkloadDuration)
		}
		if current.Error != "" || !current.ClaimSucceeded || (result.WorkloadMeasured && !workloadOK) {
			result.Errors++
		}
		if !current.ClaimSucceeded {
			result.ClaimErrors++
			continue
		}
		commandDurations = append(commandDurations, current.CommandDuration)
		wallDurations = append(wallDurations, current.WallDuration)
		if !current.WithinSLO || current.CommandDuration > cfg.hardLimit {
			result.SLOMisses++
		}
		if current.WallDuration > cfg.hardLimit {
			result.WallMisses++
		}
	}
	result.CommandReady = summarize(commandDurations)
	result.Wall = summarize(wallDurations)
	result.Cleanup = summarize(cleanupDurations)
	result.FirstCommand = summarize(firstCommandDurations)
	result.WorkloadWall = summarize(workloadDurations)
	// The startup contract ends at authenticated readiness and the complete
	// claim response. User executable latency and the engineering p50 target
	// are deliberately not part of that hard per-sandbox boundary.
	result.P50TargetMet = result.CommandReady.Count > 0 && result.CommandReady.P50 <= cfg.p50Target
	result.StartupPassed = result.ClaimErrors == 0 && result.SLOMisses == 0 && result.WallMisses == 0 &&
		len(result.Samples) > 0 && result.CommandReady.Count == len(result.Samples) &&
		result.CommandReady.P99 <= cfg.hardLimit && result.CommandReady.Max <= cfg.hardLimit &&
		result.Wall.Count == len(result.Samples) && result.Wall.P99 <= cfg.hardLimit && result.Wall.Max <= cfg.hardLimit
	result.WorkloadPassed = result.WorkloadMeasured && result.FirstCommandErrors == 0 && result.FirstCommandMisses == 0 &&
		result.FirstCommand.Count == len(result.Samples) && result.WorkloadWall.Count == len(result.Samples)
	result.Passed = result.StartupPassed && result.Errors == 0 && result.CleanupErrors == 0 &&
		result.Cleanup.Count == len(result.Samples) && (!result.WorkloadMeasured || result.WorkloadPassed)
	if !result.Passed {
		return result, fmt.Errorf(
			"acceptance failed: startup_passed=%t workload_measured=%t workload_passed=%t samples=%d errors=%d command_misses=%d wall_misses=%d cleanup_errors=%d command_p50=%s command_p99=%s command_max=%s wall_p99=%s wall_max=%s first_command_errors=%d first_command_misses=%d first_command_max=%s",
			result.StartupPassed, result.WorkloadMeasured, result.WorkloadPassed, len(result.Samples), result.Errors, result.SLOMisses, result.WallMisses, result.CleanupErrors,
			result.CommandReady.P50, result.CommandReady.P99, result.CommandReady.Max,
			result.Wall.P99, result.Wall.Max, result.FirstCommandErrors, result.FirstCommandMisses, result.FirstCommand.Max,
		)
	}
	return result, nil
}

func currentExecutableSHA256() (string, error) {
	executableSHAOnce.Do(func() {
		path, err := os.Executable()
		if err != nil {
			executableSHAErr = err
			return
		}
		file, err := os.Open(path)
		if err != nil {
			executableSHAErr = err
			return
		}
		defer file.Close()
		info, err := file.Stat()
		if err != nil {
			executableSHAErr = err
			return
		}
		if !info.Mode().IsRegular() || info.Size() <= 0 {
			executableSHAErr = errors.New("acceptance executable is not a non-empty regular file")
			return
		}
		digest := sha256.New()
		if _, err := io.Copy(digest, file); err != nil {
			executableSHAErr = err
			return
		}
		executableSHA = fmt.Sprintf("%x", digest.Sum(nil))
	})
	return executableSHA, executableSHAErr
}

func claim(ctx context.Context, cfg config, index, batch, lane int, seenSandboxIDs *sync.Map) sample {
	result := sample{Index: index, Batch: batch, Lane: lane}
	annotateCase(&result, cfg)
	requestCtx, cancel := context.WithTimeout(ctx, cfg.requestTimeout)
	defer cancel()
	request, err := http.NewRequestWithContext(requestCtx, http.MethodPost, cfg.endpoint, bytes.NewReader(cfg.body))
	if err != nil {
		result.Error = err.Error()
		return result
	}
	request.Header.Set("Authorization", "Bearer "+cfg.token)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-Request-ID", fmt.Sprintf("runtime-slot-slo-%d-%d-%d", time.Now().UnixNano(), batch, lane))
	started := time.Now()
	result.ClaimStartedAt = started.UTC()
	response, err := cfg.client.Do(request)
	result.WallDuration = time.Since(started)
	result.ClaimCompletedAt = time.Now().UTC()
	if err != nil {
		result.Error = err.Error()
		return result
	}
	defer response.Body.Close()
	payload, readErr := io.ReadAll(io.LimitReader(response.Body, (1<<20)+1))
	result.WallDuration = time.Since(started)
	result.ClaimCompletedAt = time.Now().UTC()
	if readErr != nil {
		result.Error = readErr.Error()
		return result
	}
	if len(payload) > 1<<20 {
		result.Error = "claim response exceeds 1 MiB"
		return result
	}
	if response.StatusCode != http.StatusCreated {
		result.Error = fmt.Sprintf("claim status %d: %s", response.StatusCode, truncate(string(payload), 512))
		return result
	}
	decoded, apiErr, err := spec.DecodeResponse[claimResponse](bytes.NewReader(payload))
	if err != nil || apiErr != nil || decoded == nil || !validClaimSandboxID(decoded.SandboxID) {
		result.Error = fmt.Sprintf("invalid claim response: decode=%v api=%v", err, apiErr)
		return result
	}
	result.SandboxID = decoded.SandboxID
	// Admit each claimed identity once before any executable is dispatched, even
	// when duplicate responses arrive concurrently in a synchronized batch.
	if firstIndex, duplicate := seenSandboxIDs.LoadOrStore(result.SandboxID, index); duplicate {
		result.Error = fmt.Sprintf("claim sandbox_id duplicates sample %d", firstIndex)
		return result
	}
	duration, err := commandReadyDuration(strings.Join(response.Header.Values("Server-Timing"), ","))
	if err != nil {
		result.Error = err.Error()
		return result
	}
	result.CommandDuration = duration
	withinSLO, err := commandReadyWithinSLO(response.Header.Values("Sandbox0-Command-Ready-SLO"))
	if err != nil {
		result.Error = err.Error()
		return result
	}
	result.WithinSLO = withinSLO
	result.ClaimSucceeded = true
	// Command requests have their own unchanged request budget. The end-to-end
	// clock continues from before claim, including its complete response body.
	runCommands(ctx, cfg, started, &result)
	return result
}

func validClaimSandboxID(value string) bool {
	return value != "" && value == strings.TrimSpace(value) && len(value) <= 512 &&
		value != "." && value != ".." && url.PathEscape(value) == value
}

func commandReadyWithinSLO(values []string) (bool, error) {
	if len(values) != 1 {
		return false, errors.New("claim response must contain exactly one command-ready SLO header")
	}
	switch values[0] {
	case "met":
		return true, nil
	case "missed":
		return false, nil
	default:
		return false, errors.New("claim response lacks the canonical command-ready SLO header")
	}
}

func cleanupSandbox(ctx context.Context, cfg config, sandboxID string) error {
	endpoint, err := url.JoinPath(cfg.endpoint, sandboxID)
	if err != nil {
		return err
	}
	cleanupCtx, cleanupCancel := context.WithTimeout(ctx, cfg.cleanupTimeout)
	defer cleanupCancel()
	status, payload, err := sandboxRequest(cleanupCtx, cfg, http.MethodDelete, endpoint)
	if err != nil {
		return fmt.Errorf("request sandbox %s deletion: %w", sandboxID, err)
	}
	if status < 200 || status >= 300 {
		return fmt.Errorf("delete sandbox %s returned status %d: %s", sandboxID, status, truncate(string(payload), 512))
	}
	for {
		status, payload, err = sandboxRequest(cleanupCtx, cfg, http.MethodGet, endpoint)
		if err != nil {
			if cleanupCtx.Err() != nil {
				return fmt.Errorf("sandbox %s cleanup did not converge: %w", sandboxID, cleanupCtx.Err())
			}
			return fmt.Errorf("observe sandbox %s cleanup: %w", sandboxID, err)
		}
		switch status {
		case http.StatusNotFound:
			_, apiErr, decodeErr := spec.DecodeResponse[json.RawMessage](bytes.NewReader(payload))
			if decodeErr != nil || apiErr == nil || apiErr.Code != spec.CodeNotFound {
				return fmt.Errorf("sandbox %s absence response is not a canonical not_found envelope", sandboxID)
			}
			return nil
		case http.StatusOK:
		default:
			return fmt.Errorf("observe sandbox %s cleanup returned status %d: %s", sandboxID, status, truncate(string(payload), 512))
		}
		timer := time.NewTimer(cfg.cleanupPoll)
		select {
		case <-cleanupCtx.Done():
			timer.Stop()
			return fmt.Errorf("sandbox %s cleanup did not converge: %w", sandboxID, cleanupCtx.Err())
		case <-timer.C:
		}
	}
}

func sandboxRequest(ctx context.Context, cfg config, method, endpoint string) (int, []byte, error) {
	requestCtx, cancel := context.WithTimeout(ctx, cfg.requestTimeout)
	defer cancel()
	request, err := http.NewRequestWithContext(requestCtx, method, endpoint, nil)
	if err != nil {
		return 0, nil, err
	}
	request.Header.Set("Authorization", "Bearer "+cfg.token)
	response, err := cfg.client.Do(request)
	if err != nil {
		return 0, nil, err
	}
	defer response.Body.Close()
	payload, err := io.ReadAll(io.LimitReader(response.Body, (1<<20)+1))
	if err != nil {
		return 0, nil, err
	}
	if len(payload) > 1<<20 {
		return 0, nil, errors.New("sandbox response exceeds 1 MiB")
	}
	return response.StatusCode, payload, nil
}

func commandReadyDuration(value string) (time.Duration, error) {
	foundMetric := false
	var duration time.Duration
	for _, metric := range strings.Split(value, ",") {
		parts := strings.Split(metric, ";")
		if strings.TrimSpace(parts[0]) != commandReadyTimingMetric {
			continue
		}
		if foundMetric {
			return 0, errors.New("command-ready Server-Timing metric is duplicated")
		}
		foundMetric = true
		foundDuration := false
		for _, parameter := range parts[1:] {
			name, raw, found := strings.Cut(strings.TrimSpace(parameter), "=")
			if !found || name != "dur" {
				continue
			}
			if foundDuration {
				return 0, errors.New("command-ready Server-Timing duration is duplicated")
			}
			milliseconds, err := strconv.ParseFloat(raw, 64)
			if err != nil || math.IsNaN(milliseconds) || math.IsInf(milliseconds, 0) || milliseconds < 0 ||
				milliseconds > float64(math.MaxInt64)/float64(time.Millisecond) {
				return 0, errors.New("command-ready Server-Timing duration is invalid")
			}
			duration = time.Duration(milliseconds * float64(time.Millisecond))
			foundDuration = true
		}
		if !foundDuration {
			return 0, errors.New("command-ready Server-Timing metric lacks a duration")
		}
	}
	if !foundMetric {
		return 0, errors.New("claim response lacks sandbox0-command-ready Server-Timing")
	}
	return duration, nil
}

func summarize(values []time.Duration) distribution {
	if len(values) == 0 {
		return distribution{}
	}
	sorted := append([]time.Duration(nil), values...)
	sort.Slice(sorted, func(left, right int) bool { return sorted[left] < sorted[right] })
	return distribution{
		Count: len(sorted), Min: sorted[0], P50: percentile(sorted, 50), P95: percentile(sorted, 95),
		P99: percentile(sorted, 99), Max: sorted[len(sorted)-1],
	}
}

func percentile(sorted []time.Duration, percent int) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	index := (percent*len(sorted) + 99) / 100
	if index < 1 {
		index = 1
	}
	return sorted[index-1]
}

func claimBody(templateID, bodyFile string) ([]byte, error) {
	if strings.TrimSpace(bodyFile) != "" {
		payload, err := os.ReadFile(bodyFile)
		if err != nil {
			return nil, fmt.Errorf("read claim body: %w", err)
		}
		if len(payload) > 1<<20 || !json.Valid(payload) {
			return nil, errors.New("claim body file must be valid JSON no larger than 1 MiB")
		}
		return payload, nil
	}
	templateID = strings.TrimSpace(templateID)
	if templateID == "" {
		return nil, errors.New("template is required")
	}
	return json.Marshal(map[string]string{"template": templateID})
}

func bearerToken(path string) (string, error) {
	if strings.TrimSpace(path) == "" {
		value := strings.TrimSpace(os.Getenv("SANDBOX0_API_TOKEN"))
		if value == "" {
			return "", errors.New("set --token-file or SANDBOX0_API_TOKEN")
		}
		return value, nil
	}
	payload, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read token file: %w", err)
	}
	value := strings.TrimSpace(string(payload))
	if value == "" || strings.ContainsAny(value, "\r\n") {
		return "", errors.New("token file must contain one non-empty bearer token")
	}
	return value, nil
}

func newHTTPClient(concurrency int, timeout time.Duration) *http.Client {
	dialer := &net.Dialer{Timeout: min(timeout, 5*time.Second), KeepAlive: 30 * time.Second}
	return &http.Client{
		Timeout: timeout,
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return http.ErrUseLastResponse
		},
		Transport: &http.Transport{
			Proxy: nil, DialContext: dialer.DialContext, ForceAttemptHTTP2: true,
			MaxIdleConns: max(32, concurrency*2), MaxIdleConnsPerHost: max(16, concurrency*2),
			IdleConnTimeout: 90 * time.Second,
			TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12},
		},
	}
}

func truncate(value string, limit int) string {
	if len(value) <= limit {
		return value
	}
	return value[:limit]
}

func fatal(err error) {
	_, _ = fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
