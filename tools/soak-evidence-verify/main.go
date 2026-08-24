// Command soak-evidence-verify independently audits completed production
// endurance evidence against the fixed materializer or Bolt acceptance contract.
package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/sandbox0-ai/sandbox0/internal/soakstate"
)

const (
	verificationVersion = 1
	materializerKind    = "materializer"
	boltKind            = "bolt"
	requiredItems       = 10_000
	minimumTestDuration = 10 * time.Second
	enduranceDuration   = 24 * time.Hour
)

type options struct {
	path                     string
	kind                     string
	expectedConfigSHA256     string
	expectedExecutableSHA256 string
	minimumActiveDuration    time.Duration
	output                   string
}

type report struct {
	Version    int                        `json:"version"`
	Kind       string                     `json:"kind"`
	VerifiedAt time.Time                  `json:"verified_at"`
	Passed     bool                       `json:"passed"`
	Evidence   soakstate.Verification     `json:"evidence"`
	Contract   map[string]json.RawMessage `json:"contract"`
}

type materializerConfig struct {
	Duration            string `json:"duration"`
	Generations         int    `json:"generations"`
	BurstCount          int    `json:"burst_count"`
	WorkerInterval      string `json:"worker_interval"`
	SampleInterval      string `json:"sample_interval"`
	MinPackBytes        int64  `json:"min_pack_bytes"`
	MaxDelay            string `json:"max_delay"`
	PhysicalByteLimit   int64  `json:"physical_byte_limit"`
	PhysicalFileLimit   int64  `json:"physical_file_limit"`
	DatabaseGrowthLimit int64  `json:"database_growth_limit"`
	TerminalRetention   string `json:"terminal_retention"`
	UploadingStale      string `json:"uploading_stale"`
	GarbageInterval     string `json:"garbage_interval"`
	RustFSEndpoint      string `json:"rustfs_endpoint"`
	RustFSBucket        string `json:"rustfs_bucket"`
	RustFSDataDir       string `json:"rustfs_data_dir"`
	ProxyListen         string `json:"proxy_listen"`
}

type materializerFinal struct {
	Passed     *bool    `json:"passed"`
	Violations []string `json:"violations"`
	Counters   struct {
		Generated            int `json:"generated"`
		Materialized         int `json:"materialized"`
		RetainedBatches      int `json:"retained_batches"`
		ExpectedWorkerErrors int `json:"expected_worker_errors"`
	} `json:"counters"`
	Database struct {
		CompositeGenerations    int64 `json:"composite_generations"`
		MaterializedGenerations int64 `json:"materialized_generations"`
		UploadingBatches        int64 `json:"uploading_batches"`
		AbandonedBatches        int64 `json:"abandoned_batches"`
		CatalogObjects          int64 `json:"catalog_objects"`
		DeletionQueue           int64 `json:"deletion_queue"`
	} `json:"database"`
	Objects struct {
		Objects int64 `json:"objects"`
	} `json:"objects"`
	Bounds struct {
		MaxBatches int   `json:"max_batches"`
		MaxObjects int64 `json:"max_objects"`
	} `json:"bounds"`
	DatabaseGrowthBytes int64 `json:"database_growth_bytes"`
	PhysicalGrowthFiles int64 `json:"physical_growth_files"`
	PhysicalGrowthBytes int64 `json:"physical_growth_bytes"`
}

type materializerCheckpoint struct {
	Version              int      `json:"version"`
	Phase                string   `json:"phase"`
	ActiveElapsedNS      int64    `json:"active_elapsed_ns"`
	NextGeneration       int      `json:"next_generation"`
	FaultPhase           string   `json:"fault_phase"`
	ExpectedWorkerErrors int      `json:"expected_worker_errors"`
	FinalViolations      []string `json:"final_violations"`
}

type boltConfig struct {
	Duration      string `json:"duration"`
	Proofs        int    `json:"proofs"`
	Bursts        int    `json:"bursts"`
	TerminalTTL   string `json:"terminal_ttl"`
	StateDir      string `json:"state_dir"`
	JournalFormat int    `json:"journal_format"`
}

type boltFinal struct {
	Passed         *bool `json:"passed"`
	Inserted       int   `json:"inserted"`
	Deleted        int   `json:"deleted"`
	WarmFileBytes  int64 `json:"warm_file_bytes"`
	FinalFileBytes int64 `json:"final_file_bytes"`
	GrowthBytes    int64 `json:"growth_bytes"`
}

type boltCheckpoint struct {
	Version         int    `json:"version"`
	Phase           string `json:"phase"`
	ActiveElapsedNS int64  `json:"active_elapsed_ns"`
	Next            int    `json:"next"`
	Deleted         int    `json:"deleted"`
	Restarted       bool   `json:"restarted"`
	WarmSize        int64  `json:"warm_size"`
}

func main() {
	opts := parseOptions()
	result, err := verify(opts)
	if err != nil {
		fatal(err)
	}
	payload, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		fatal(fmt.Errorf("encode verification report: %w", err))
	}
	payload = append(payload, '\n')
	if opts.output != "" {
		if err := writeReportFile(opts.output, payload); err != nil {
			fatal(err)
		}
	}
	if _, err := os.Stdout.Write(payload); err != nil {
		fatal(fmt.Errorf("write verification report: %w", err))
	}
}

func parseOptions() options {
	var opts options
	flag.StringVar(&opts.path, "path", "", "completed durable JSONL evidence path")
	flag.StringVar(&opts.kind, "kind", "", "acceptance contract: materializer or bolt")
	flag.StringVar(&opts.expectedConfigSHA256, "expected-config-sha256", "", "optional fixed configuration SHA-256")
	flag.StringVar(&opts.expectedExecutableSHA256, "expected-executable-sha256", "", "fixed gate executable SHA-256")
	flag.DurationVar(&opts.minimumActiveDuration, "minimum-active-duration", 24*time.Hour, "minimum accepted active duration")
	flag.StringVar(&opts.output, "output", "", "optional mode-0600 JSON verification report")
	flag.Parse()
	opts.path = strings.TrimSpace(opts.path)
	opts.kind = strings.TrimSpace(opts.kind)
	opts.expectedConfigSHA256 = strings.TrimSpace(opts.expectedConfigSHA256)
	opts.expectedExecutableSHA256 = strings.TrimSpace(opts.expectedExecutableSHA256)
	opts.output = strings.TrimSpace(opts.output)
	return opts
}

func verify(opts options) (report, error) {
	if opts.path == "" || opts.expectedExecutableSHA256 == "" {
		return report{}, fmt.Errorf("path and expected executable SHA-256 are required")
	}
	if opts.kind != materializerKind && opts.kind != boltKind {
		return report{}, fmt.Errorf("kind must be materializer or bolt")
	}
	if opts.minimumActiveDuration < minimumTestDuration || opts.minimumActiveDuration > 7*24*time.Hour {
		return report{}, fmt.Errorf("minimum active duration must be between 10s and 7d")
	}
	if opts.output != "" {
		if !canonicalNonRootAbsolutePath(opts.output) {
			return report{}, fmt.Errorf("verification report path must be a canonical non-root absolute path")
		}
		if filepath.Clean(opts.path) == opts.output {
			return report{}, fmt.Errorf("verification report must not replace the soak evidence")
		}
	}
	verified, err := soakstate.VerifyFile(soakstate.VerifyOptions{
		Path: opts.path, ExpectedConfigSHA256: opts.expectedConfigSHA256,
		ExpectedExecutableSHA256: opts.expectedExecutableSHA256, RequireFinal: true,
	})
	if err != nil {
		return report{}, err
	}
	if verified.ActiveElapsed < opts.minimumActiveDuration {
		return report{}, fmt.Errorf("soak active duration %s is below required %s", verified.ActiveElapsed, opts.minimumActiveDuration)
	}
	var contract map[string]json.RawMessage
	if opts.kind == materializerKind {
		contract, err = validateMaterializer(verified, opts.minimumActiveDuration)
	} else {
		contract, err = validateBolt(verified, opts.minimumActiveDuration)
	}
	if err != nil {
		return report{}, err
	}
	return report{
		Version: verificationVersion, Kind: opts.kind, VerifiedAt: time.Now().UTC(),
		Passed: true, Evidence: verified, Contract: contract,
	}, nil
}

func validateMaterializer(verified soakstate.Verification, duration time.Duration) (map[string]json.RawMessage, error) {
	var config materializerConfig
	if err := decodeStrict(verified.Config, &config); err != nil {
		return nil, fmt.Errorf("decode materializer configuration: %w", err)
	}
	wantDuration := duration.String()
	if config.Duration != wantDuration || config.Generations != requiredItems || config.BurstCount != 20 ||
		config.MinPackBytes != 32<<20 ||
		config.PhysicalByteLimit != 512<<20 || config.PhysicalFileLimit != 4096 ||
		config.DatabaseGrowthLimit != 512<<20 || config.TerminalRetention != (24*time.Hour).String() ||
		config.UploadingStale != time.Hour.String() || config.GarbageInterval != time.Minute.String() {
		return nil, fmt.Errorf("materializer configuration does not match the production acceptance contract")
	}
	if err := validateMaterializerTiming(config, duration); err != nil {
		return nil, err
	}
	if !canonicalRustFSEndpoint(config.RustFSEndpoint) || !canonicalBucket(config.RustFSBucket) ||
		!canonicalNonRootAbsolutePath(config.RustFSDataDir) || !canonicalListenAddress(config.ProxyListen) {
		return nil, fmt.Errorf("materializer physical RustFS configuration is incomplete")
	}
	var final materializerFinal
	if err := json.Unmarshal(verified.LastData, &final); err != nil {
		return nil, fmt.Errorf("decode materializer final data: %w", err)
	}
	maxDelay, err := time.ParseDuration(config.MaxDelay)
	if err != nil || maxDelay <= 0 {
		return nil, fmt.Errorf("materializer maximum delay is invalid")
	}
	maxBatches := int(duration/maxDelay) + 3
	maxObjects := int64(2 + 2*maxBatches)
	if final.Passed == nil || !*final.Passed || len(final.Violations) != 0 ||
		final.Counters.Generated != requiredItems || final.Counters.Materialized != requiredItems ||
		final.Counters.RetainedBatches <= 0 || final.Counters.RetainedBatches > maxBatches ||
		final.Counters.ExpectedWorkerErrors != 2 ||
		final.Database.CompositeGenerations != 0 || final.Database.UploadingBatches != 0 ||
		final.Database.AbandonedBatches != 0 || final.Database.DeletionQueue != 0 ||
		final.Database.MaterializedGenerations != requiredItems+1 ||
		final.Database.CatalogObjects != final.Objects.Objects-1 ||
		final.Objects.Objects <= 2 || final.Objects.Objects > maxObjects ||
		final.Bounds.MaxBatches != maxBatches || final.Bounds.MaxObjects != maxObjects ||
		final.DatabaseGrowthBytes < 0 || final.DatabaseGrowthBytes > 512<<20 ||
		final.PhysicalGrowthFiles < 0 || final.PhysicalGrowthFiles > 4096 ||
		final.PhysicalGrowthBytes < 0 || final.PhysicalGrowthBytes > 512<<20 {
		return nil, fmt.Errorf("materializer final data does not satisfy the production acceptance contract")
	}
	var checkpoint materializerCheckpoint
	if err := json.Unmarshal(verified.LastCheckpoint, &checkpoint); err != nil {
		return nil, fmt.Errorf("decode materializer final checkpoint: %w", err)
	}
	if checkpoint.Version != 1 || checkpoint.Phase != "passed" || checkpoint.NextGeneration != requiredItems ||
		checkpoint.FaultPhase != "recovered" || checkpoint.ExpectedWorkerErrors != 2 ||
		len(checkpoint.FinalViolations) != 0 || checkpoint.ActiveElapsedNS != verified.ActiveElapsed.Nanoseconds() {
		return nil, fmt.Errorf("materializer final checkpoint is incomplete")
	}
	return rawContract(verified), nil
}

func validateMaterializerTiming(config materializerConfig, duration time.Duration) error {
	workerInterval, workerErr := time.ParseDuration(config.WorkerInterval)
	sampleInterval, sampleErr := time.ParseDuration(config.SampleInterval)
	maxDelay, delayErr := time.ParseDuration(config.MaxDelay)
	if workerErr != nil || sampleErr != nil || delayErr != nil {
		return fmt.Errorf("materializer timing configuration is invalid")
	}
	if duration >= enduranceDuration {
		if workerInterval != time.Second || sampleInterval != time.Minute || maxDelay != 5*time.Minute {
			return fmt.Errorf("materializer timing does not match the endurance profile")
		}
		return nil
	}
	if workerInterval < 10*time.Millisecond || workerInterval > time.Second ||
		sampleInterval < time.Second || sampleInterval > time.Minute ||
		maxDelay < time.Second || maxDelay > 5*time.Minute || maxDelay >= duration {
		return fmt.Errorf("materializer timing does not match the accelerated profile")
	}
	return nil
}

func validateBolt(verified soakstate.Verification, duration time.Duration) (map[string]json.RawMessage, error) {
	var config boltConfig
	if err := decodeStrict(verified.Config, &config); err != nil {
		return nil, fmt.Errorf("decode Bolt configuration: %w", err)
	}
	if config.Duration != duration.String() || config.Proofs != requiredItems || config.Bursts != 20 ||
		config.TerminalTTL != time.Hour.String() || config.JournalFormat != 1 ||
		!canonicalNonRootAbsolutePath(config.StateDir) {
		return nil, fmt.Errorf("bolt configuration does not match the production acceptance contract")
	}
	var final boltFinal
	if err := json.Unmarshal(verified.LastData, &final); err != nil {
		return nil, fmt.Errorf("decode Bolt final data: %w", err)
	}
	if final.Passed == nil || !*final.Passed || final.Inserted != requiredItems || final.Deleted != requiredItems ||
		final.WarmFileBytes <= 0 || final.FinalFileBytes <= 0 ||
		final.FinalFileBytes-final.WarmFileBytes != final.GrowthBytes ||
		final.GrowthBytes < 0 || final.GrowthBytes > int64(os.Getpagesize()) {
		return nil, fmt.Errorf("bolt final data does not satisfy the production acceptance contract")
	}
	var checkpoint boltCheckpoint
	if err := json.Unmarshal(verified.LastCheckpoint, &checkpoint); err != nil {
		return nil, fmt.Errorf("decode Bolt final checkpoint: %w", err)
	}
	if checkpoint.Version != 1 || checkpoint.Phase != "complete" || checkpoint.Next != requiredItems ||
		checkpoint.Deleted != requiredItems || !checkpoint.Restarted ||
		checkpoint.WarmSize != final.WarmFileBytes ||
		checkpoint.ActiveElapsedNS != verified.ActiveElapsed.Nanoseconds() {
		return nil, fmt.Errorf("bolt final checkpoint is incomplete")
	}
	return rawContract(verified), nil
}

func rawContract(verified soakstate.Verification) map[string]json.RawMessage {
	return map[string]json.RawMessage{
		"configuration": append(json.RawMessage(nil), verified.Config...),
		"final_data":    append(json.RawMessage(nil), verified.LastData...),
		"checkpoint":    append(json.RawMessage(nil), verified.LastCheckpoint...),
	}
}

func decodeStrict(payload []byte, destination any) error {
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	var extra json.RawMessage
	err := decoder.Decode(&extra)
	if errors.Is(err, io.EOF) {
		return nil
	}
	if err != nil {
		return err
	}
	return fmt.Errorf("multiple JSON values are not allowed")
}

func canonicalNonRootAbsolutePath(path string) bool {
	return filepath.IsAbs(path) && path != string(filepath.Separator) && filepath.Clean(path) == path
}

func canonicalRustFSEndpoint(raw string) bool {
	if raw == "" || strings.TrimSpace(raw) != raw {
		return false
	}
	endpoint, err := url.ParseRequestURI(raw)
	if err != nil || (endpoint.Scheme != "http" && endpoint.Scheme != "https") ||
		endpoint.Hostname() == "" || endpoint.User != nil || endpoint.Opaque != "" ||
		endpoint.Path != "" || endpoint.RawPath != "" || endpoint.RawQuery != "" || endpoint.Fragment != "" ||
		endpoint.String() != raw {
		return false
	}
	port := endpoint.Port()
	if port == "" {
		return endpoint.Host == endpoint.Hostname() || endpoint.Host == "["+endpoint.Hostname()+"]"
	}
	if net.JoinHostPort(endpoint.Hostname(), port) != endpoint.Host {
		return false
	}
	value, err := strconv.ParseUint(port, 10, 16)
	return err == nil && value > 0
}

func canonicalBucket(bucket string) bool {
	return bucket != "" && len(bucket) <= 63 && strings.TrimSpace(bucket) == bucket &&
		!strings.ContainsAny(bucket, "/\\\r\n\t ")
}

func canonicalListenAddress(raw string) bool {
	if raw == "" || strings.TrimSpace(raw) != raw {
		return false
	}
	host, port, err := net.SplitHostPort(raw)
	if err != nil || host == "" || net.JoinHostPort(host, port) != raw {
		return false
	}
	value, err := strconv.ParseUint(port, 10, 16)
	return err == nil && value > 0
}

func writeReportFile(path string, payload []byte) error {
	if info, err := os.Lstat(path); err == nil {
		if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
			return fmt.Errorf("verification report target must be a regular file")
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("inspect verification report target: %w", err)
	}
	directory := filepath.Dir(path)
	temporary, err := os.CreateTemp(directory, "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return fmt.Errorf("create verification report: %w", err)
	}
	temporaryPath := temporary.Name()
	defer func() {
		_ = temporary.Close()
		_ = os.Remove(temporaryPath)
	}()
	if err := temporary.Chmod(0o600); err != nil {
		return fmt.Errorf("secure verification report: %w", err)
	}
	if _, err := temporary.Write(payload); err != nil {
		return fmt.Errorf("write verification report: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		return fmt.Errorf("sync verification report: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close verification report: %w", err)
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return fmt.Errorf("publish verification report: %w", err)
	}
	directoryHandle, err := os.Open(directory)
	if err != nil {
		return fmt.Errorf("open verification report directory: %w", err)
	}
	syncErr := directoryHandle.Sync()
	closeErr := directoryHandle.Close()
	if err := errors.Join(syncErr, closeErr); err != nil {
		return fmt.Errorf("sync verification report directory: %w", err)
	}
	return nil
}

func fatal(err error) {
	_, _ = fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
