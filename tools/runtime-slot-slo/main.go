// Command runtime-slot-slo validates the public regional claim route against
// the trusted ingress-to-procd timer emitted by the Nomad manager backend.
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
	"path"
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
	endpoint       string
	token          string
	body           []byte
	batches        int
	concurrency    int
	requestTimeout time.Duration
	cleanupTimeout time.Duration
	cleanupPoll    time.Duration
	settle         time.Duration
	hardLimit      time.Duration
	p50Target      time.Duration
	label          string
	client         *http.Client
}

type sample struct {
	Index           int           `json:"index"`
	Batch           int           `json:"batch"`
	Lane            int           `json:"lane"`
	SandboxID       string        `json:"sandbox_id,omitempty"`
	WallDuration    time.Duration `json:"wall_duration_ns"`
	CommandDuration time.Duration `json:"command_ready_duration_ns"`
	WithinSLO       bool          `json:"within_slo"`
	Error           string        `json:"error,omitempty"`
	CleanupError    string        `json:"cleanup_error,omitempty"`
	CleanupDuration time.Duration `json:"cleanup_duration_ns,omitempty"`
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
	Version          int           `json:"version"`
	ExecutableSHA256 string        `json:"executable_sha256"`
	Label            string        `json:"label,omitempty"`
	StartedAt        time.Time     `json:"started_at"`
	CompletedAt      time.Time     `json:"completed_at"`
	Endpoint         string        `json:"endpoint"`
	Batches          int           `json:"batches"`
	Concurrency      int           `json:"concurrency"`
	HardLimit        time.Duration `json:"hard_limit_ns"`
	P50Target        time.Duration `json:"p50_target_ns"`
	CleanupTimeout   time.Duration `json:"cleanup_timeout_ns"`
	CommandReady     distribution  `json:"command_ready"`
	Wall             distribution  `json:"wall"`
	Cleanup          distribution  `json:"cleanup"`
	Errors           int           `json:"errors"`
	SLOMisses        int           `json:"slo_misses"`
	WallMisses       int           `json:"wall_misses"`
	CleanupErrors    int           `json:"cleanup_errors"`
	Passed           bool          `json:"passed"`
	Samples          []sample      `json:"samples"`
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
		endpoint       = flag.String("url", "", "full regional POST /api/v1/sandboxes URL")
		tokenFile      = flag.String("token-file", "", "file containing the public API bearer token")
		templateID     = flag.String("template", "default", "sandbox template ID")
		bodyFile       = flag.String("body-file", "", "optional complete claim JSON body")
		batches        = flag.Int("batches", 1000, "number of synchronized request batches")
		concurrent     = flag.Int("concurrency", 1, "requests in each synchronized batch")
		timeout        = flag.Duration("request-timeout", 15*time.Second, "per-request timeout")
		cleanupTimeout = flag.Duration("cleanup-timeout", 2*time.Minute, "maximum time for each DELETE to converge to public absence")
		cleanupPoll    = flag.Duration("cleanup-poll", 100*time.Millisecond, "public GET interval while waiting for cleanup convergence")
		settle         = flag.Duration("batch-settle", 0, "delay after cleanup before the next batch")
		hardLimit      = flag.Duration("hard-limit", time.Second, "maximum successful command-ready sample")
		p50Target      = flag.Duration("p50-target", 500*time.Millisecond, "engineering p50 target")
		output         = flag.String("output", "", "optional JSON report path; stdout is always written")
		label          = flag.String("label", "", "optional environment label included in the report")
	)
	flag.Parse()

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
		label: strings.TrimSpace(*label),
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
	if path.Clean(parsed.Path) != "/api/v1/sandboxes" {
		return errors.New("url path must be /api/v1/sandboxes")
	}
	if parsed.RawQuery != "" || parsed.Fragment != "" {
		return errors.New("url must not contain a query or fragment")
	}
	if c.token == "" {
		return errors.New("a bearer token is required")
	}
	if len(c.body) == 0 || len(c.body) > 1<<20 || !json.Valid(c.body) {
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
	return nil
}

func run(ctx context.Context, cfg config) (report, error) {
	if cfg.client == nil {
		return report{}, errors.New("HTTP client is required")
	}
	executableDigest, err := currentExecutableSHA256()
	if err != nil {
		return report{}, fmt.Errorf("hash acceptance executable: %w", err)
	}
	result := report{
		Version: 4, ExecutableSHA256: executableDigest,
		Label: cfg.label, StartedAt: time.Now().UTC(), Endpoint: cfg.endpoint,
		Batches: cfg.batches, Concurrency: cfg.concurrency, HardLimit: cfg.hardLimit,
		P50Target: cfg.p50Target, CleanupTimeout: cfg.cleanupTimeout,
		Samples: make([]sample, cfg.batches*cfg.concurrency),
	}
	var cleanupErrors atomic.Int64
	seenSandboxIDs := make(map[string]int, len(result.Samples))
	for batch := range cfg.batches {
		var wait sync.WaitGroup
		start := make(chan struct{})
		for lane := range cfg.concurrency {
			index := batch*cfg.concurrency + lane
			wait.Add(1)
			go func() {
				defer wait.Done()
				<-start
				result.Samples[index] = claim(ctx, cfg, index, batch, lane)
			}()
		}
		close(start)
		wait.Wait()
		for lane := range cfg.concurrency {
			index := batch*cfg.concurrency + lane
			current := &result.Samples[index]
			if current.SandboxID == "" {
				continue
			}
			if firstIndex, duplicate := seenSandboxIDs[current.SandboxID]; duplicate {
				if current.Error == "" {
					current.Error = fmt.Sprintf("claim sandbox_id duplicates sample %d", firstIndex)
				}
				continue
			}
			seenSandboxIDs[current.SandboxID] = index
		}
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
				if err := cleanupSandbox(ctx, cfg, sandboxID); err != nil {
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
				return result, ctx.Err()
			case <-time.After(cfg.settle):
			}
		}
	}
	result.CompletedAt = time.Now().UTC()
	result.CleanupErrors = int(cleanupErrors.Load())
	commandDurations := make([]time.Duration, 0, len(result.Samples))
	wallDurations := make([]time.Duration, 0, len(result.Samples))
	cleanupDurations := make([]time.Duration, 0, len(result.Samples))
	for _, current := range result.Samples {
		if current.CleanupError == "" && current.SandboxID != "" {
			cleanupDurations = append(cleanupDurations, current.CleanupDuration)
		}
		if current.Error != "" {
			result.Errors++
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
	result.Passed = result.Errors == 0 && result.SLOMisses == 0 && result.WallMisses == 0 && result.CleanupErrors == 0 &&
		result.CommandReady.Count == len(result.Samples) && result.CommandReady.P50 <= cfg.p50Target &&
		result.CommandReady.P99 <= cfg.hardLimit && result.CommandReady.Max <= cfg.hardLimit &&
		result.Wall.Count == len(result.Samples) && result.Wall.P99 <= cfg.hardLimit && result.Wall.Max <= cfg.hardLimit &&
		result.Cleanup.Count == len(result.Samples)
	if !result.Passed {
		return result, fmt.Errorf(
			"SLO acceptance failed: samples=%d errors=%d command_misses=%d wall_misses=%d cleanup_errors=%d command_p50=%s command_p99=%s command_max=%s wall_p99=%s wall_max=%s",
			len(result.Samples), result.Errors, result.SLOMisses, result.WallMisses, result.CleanupErrors,
			result.CommandReady.P50, result.CommandReady.P99, result.CommandReady.Max,
			result.Wall.P99, result.Wall.Max,
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

func claim(ctx context.Context, cfg config, index, batch, lane int) sample {
	result := sample{Index: index, Batch: batch, Lane: lane}
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
	response, err := cfg.client.Do(request)
	result.WallDuration = time.Since(started)
	if err != nil {
		result.Error = err.Error()
		return result
	}
	defer response.Body.Close()
	payload, readErr := io.ReadAll(io.LimitReader(response.Body, (1<<20)+1))
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
	if err != nil || apiErr != nil || decoded == nil || strings.TrimSpace(decoded.SandboxID) == "" {
		result.Error = fmt.Sprintf("invalid claim response: decode=%v api=%v", err, apiErr)
		return result
	}
	if decoded.SandboxID != strings.TrimSpace(decoded.SandboxID) || len(decoded.SandboxID) > 512 {
		result.Error = "claim response contains a noncanonical sandbox_id"
		return result
	}
	result.SandboxID = decoded.SandboxID
	duration, err := commandReadyDuration(strings.Join(response.Header.Values("Server-Timing"), ","))
	if err != nil {
		result.Error = err.Error()
		return result
	}
	result.CommandDuration = duration
	switch response.Header.Get("Sandbox0-Command-Ready-SLO") {
	case "met":
		result.WithinSLO = true
	case "missed":
		result.WithinSLO = false
	default:
		result.Error = "claim response lacks the canonical command-ready SLO header"
	}
	return result
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
