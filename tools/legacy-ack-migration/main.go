// Command legacy-ack-migration inventories and validates the frozen ACK-era
// durable manager graph before one-time import into a Nomad region.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"golang.org/x/sys/unix"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/legacyackmigration"
	templatepkg "github.com/sandbox0-ai/sandbox0/pkg/template"
)

const (
	modeInventory   = "inventory"
	modeValidate    = "validate"
	maxDSNFileBytes = 16 << 10
)

type options struct {
	mode            string
	sourceDSNFile   string
	targetClusterID string
	platformArch    string
	platformVariant string
	memoryPerCPU    string
	maxMemory       string
	output          string
	timeout         time.Duration
}

type validationSummary struct {
	Valid                 bool   `json:"valid"`
	Error                 string `json:"error,omitempty"`
	SandboxCount          int    `json:"sandbox_count,omitempty"`
	LayerChainCount       int    `json:"layer_chain_count,omitempty"`
	PinnedBaseImageCount  int    `json:"pinned_base_image_count,omitempty"`
	InferredPlatformCount int    `json:"inferred_platform_count,omitempty"`
}

type report struct {
	FormatVersion   int                          `json:"format_version"`
	CapturedAt      time.Time                    `json:"captured_at"`
	Mode            string                       `json:"mode"`
	TargetClusterID string                       `json:"target_cluster_id"`
	Platform        ocispec.Platform             `json:"platform"`
	Inventory       legacyackmigration.Inventory `json:"inventory"`
	Validation      validationSummary            `json:"validation"`
}

func main() {
	log.SetFlags(0)
	if err := run(os.Args[1:], os.Getenv, os.Stdout); err != nil {
		log.Fatal(err)
	}
}

func run(args []string, getenv func(string) string, stdout io.Writer) error {
	opts, err := parseOptions(args, getenv)
	if err != nil {
		return err
	}
	dsn, err := loadSourceDSN(opts.sourceDSNFile, strings.TrimSpace(getenv("SANDBOX0_LEGACY_SOURCE_DSN")))
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), opts.timeout)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return fmt.Errorf("configure legacy source database: %w", err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		return fmt.Errorf("connect to legacy source database: %w", err)
	}
	catalog, err := legacyackmigration.ReadCatalog(ctx, pool)
	if err != nil {
		return err
	}
	platform := ocispec.Platform{OS: "linux", Architecture: opts.platformArch, Variant: opts.platformVariant}
	result := report{
		FormatVersion: 1, CapturedAt: time.Now().UTC(), Mode: opts.mode,
		TargetClusterID: opts.targetClusterID, Platform: platform,
		Inventory: catalog.BuildInventory(),
	}
	normalized, validationErr := catalog.Normalize(legacyackmigration.NormalizeOptions{
		Platform: platform, TargetClusterID: opts.targetClusterID,
		ResourcePolicy: templatepkg.NewResourcePolicy(opts.memoryPerCPU, opts.maxMemory),
	})
	if validationErr != nil {
		result.Validation.Error = validationErr.Error()
	} else {
		result.Validation = validationSummary{
			Valid: true, SandboxCount: len(normalized.Sandboxes),
			LayerChainCount: len(normalized.LayerChains), PinnedBaseImageCount: len(normalized.PinnedImageRefs),
			InferredPlatformCount: len(normalized.InferredLayers),
		}
	}
	payload, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return fmt.Errorf("encode migration report: %w", err)
	}
	payload = append(payload, '\n')
	if opts.output != "" {
		if err := writeAtomicOwnerOnly(opts.output, payload); err != nil {
			return err
		}
	} else if _, err := stdout.Write(payload); err != nil {
		return fmt.Errorf("write migration report: %w", err)
	}
	if opts.mode == modeValidate && validationErr != nil {
		return fmt.Errorf("migration freeze validation failed: %w", validationErr)
	}
	return nil
}

func parseOptions(args []string, getenv func(string) string) (options, error) {
	var opts options
	set := flag.NewFlagSet("legacy-ack-migration", flag.ContinueOnError)
	set.SetOutput(io.Discard)
	set.StringVar(&opts.mode, "mode", modeInventory, "inventory or validate")
	set.StringVar(&opts.sourceDSNFile, "source-dsn-file", strings.TrimSpace(getenv("SANDBOX0_LEGACY_SOURCE_DSN_FILE")), "owner-only file containing the source PostgreSQL DSN")
	set.StringVar(&opts.targetClusterID, "target-cluster-id", "", "target Nomad cluster ID")
	set.StringVar(&opts.platformArch, "platform-architecture", "amd64", "canonical OCI architecture")
	set.StringVar(&opts.platformVariant, "platform-variant", "", "canonical OCI architecture variant")
	set.StringVar(&opts.memoryPerCPU, "memory-per-cpu", "2Gi", "target resource policy memory per CPU")
	set.StringVar(&opts.maxMemory, "max-memory", "256Gi", "target resource policy sandbox memory ceiling")
	set.StringVar(&opts.output, "output", "", "optional owner-only JSON evidence path")
	set.DurationVar(&opts.timeout, "timeout", 2*time.Minute, "source snapshot timeout")
	if err := set.Parse(args); err != nil {
		return options{}, err
	}
	if set.NArg() != 0 {
		return options{}, fmt.Errorf("unexpected positional arguments")
	}
	opts.mode = strings.TrimSpace(opts.mode)
	if opts.mode != modeInventory && opts.mode != modeValidate {
		return options{}, fmt.Errorf("mode must be %q or %q", modeInventory, modeValidate)
	}
	opts.targetClusterID = strings.TrimSpace(opts.targetClusterID)
	if opts.targetClusterID == "" {
		return options{}, fmt.Errorf("target-cluster-id is required")
	}
	opts.platformArch = strings.TrimSpace(opts.platformArch)
	if opts.platformArch == "" {
		return options{}, fmt.Errorf("platform-architecture is required")
	}
	opts.platformVariant = strings.TrimSpace(opts.platformVariant)
	opts.sourceDSNFile = strings.TrimSpace(opts.sourceDSNFile)
	if opts.sourceDSNFile != "" && strings.TrimSpace(getenv("SANDBOX0_LEGACY_SOURCE_DSN")) != "" {
		return options{}, fmt.Errorf("configure exactly one of source DSN file or SANDBOX0_LEGACY_SOURCE_DSN")
	}
	if opts.sourceDSNFile == "" && strings.TrimSpace(getenv("SANDBOX0_LEGACY_SOURCE_DSN")) == "" {
		return options{}, fmt.Errorf("source database DSN is required through an owner-only file or environment")
	}
	if opts.timeout <= 0 {
		return options{}, fmt.Errorf("timeout must be positive")
	}
	return opts, nil
}

func loadSourceDSN(path, environmentValue string) (string, error) {
	if path == "" {
		if environmentValue == "" {
			return "", fmt.Errorf("source database DSN is required")
		}
		return environmentValue, nil
	}
	clean := filepath.Clean(path)
	if !filepath.IsAbs(clean) || clean != path || clean == string(filepath.Separator) {
		return "", fmt.Errorf("source DSN file path must be canonical and absolute")
	}
	fd, err := unix.Open(clean, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return "", fmt.Errorf("open source DSN file: %w", err)
	}
	file := os.NewFile(uintptr(fd), clean)
	if file == nil {
		_ = unix.Close(fd)
		return "", fmt.Errorf("wrap source DSN file descriptor")
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return "", fmt.Errorf("stat source DSN file: %w", err)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 ||
		info.Size() <= 0 || info.Size() > maxDSNFileBytes || stat.Nlink != 1 || stat.Uid != uint32(os.Geteuid()) {
		return "", fmt.Errorf("source DSN must be an owner-only, expected-owner, single-link regular file within 1..%d bytes", maxDSNFileBytes)
	}
	payload, err := io.ReadAll(io.LimitReader(file, maxDSNFileBytes+1))
	if err != nil {
		return "", fmt.Errorf("read source DSN file: %w", err)
	}
	if int64(len(payload)) != info.Size() {
		return "", fmt.Errorf("source DSN file changed while being read")
	}
	dsn := strings.TrimSpace(string(payload))
	if dsn == "" || strings.ContainsAny(dsn, "\r\n") {
		return "", fmt.Errorf("source DSN file must contain exactly one non-empty line")
	}
	return dsn, nil
}

func writeAtomicOwnerOnly(path string, payload []byte) error {
	clean := filepath.Clean(strings.TrimSpace(path))
	if clean == "." || clean == string(filepath.Separator) {
		return fmt.Errorf("output path must name a file")
	}
	directory := filepath.Dir(clean)
	temporary, err := os.CreateTemp(directory, ".legacy-ack-migration-*.tmp")
	if err != nil {
		return fmt.Errorf("create migration report: %w", err)
	}
	temporaryPath := temporary.Name()
	defer func() { _ = os.Remove(temporaryPath) }()
	if err := temporary.Chmod(0o600); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("protect migration report: %w", err)
	}
	if _, err := temporary.Write(payload); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("write migration report: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("sync migration report: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close migration report: %w", err)
	}
	if err := os.Rename(temporaryPath, clean); err != nil {
		return fmt.Errorf("publish migration report: %w", err)
	}
	return nil
}
