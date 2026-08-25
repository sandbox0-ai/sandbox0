package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/opencontainers/go-digest"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/legacyackmigration"
)

const (
	modeInventory            = "inventory"
	modePreflight            = "preflight"
	modePauseAccess          = "pause-access"
	modePause                = "pause"
	modeValidate             = "validate"
	modeCapture              = "capture"
	modeRetire               = "retire"
	modePrepare              = "prepare"
	modeBuild                = "build"
	modeCommit               = "commit"
	maxDSNFileBytes          = 16 << 10
	maxManagerConfigBytes    = 1 << 20
	defaultControlTimeout    = 2 * time.Minute
	defaultPauseTimeout      = 30 * time.Minute
	defaultBuildTimeout      = 12 * time.Hour
	defaultBuildLeaseTTL     = 2 * time.Minute
	defaultBuildLeaseRenewal = 30 * time.Second
)

type options struct {
	mode                    string
	sessionID               string
	confirmSourceDigest     string
	sourceDSNFile           string
	targetDSNFile           string
	sourceManagerConfigFile string
	targetManagerConfigFile string
	targetClusterID         string
	sourceManagerURL        string
	sourceInternalKeyFile   string
	platformArch            string
	platformVariant         string
	memoryPerCPU            string
	maxMemory               string
	workerID                string
	buildLeaseTTL           time.Duration
	buildLeaseRenewal       time.Duration
	output                  string
	timeout                 time.Duration
}

func parseOptions(args []string, getenv func(string) string) (options, error) {
	var opts options
	set := flag.NewFlagSet("legacy-ack-migration", flag.ContinueOnError)
	set.SetOutput(io.Discard)
	set.StringVar(&opts.mode, "mode", modeInventory, "inventory, preflight, pause-access, pause, validate, capture, retire, prepare, build, or commit")
	set.StringVar(&opts.sessionID, "session-id", "", "immutable migration session ID")
	set.StringVar(&opts.confirmSourceDigest, "confirm-source-catalog-digest", "", "required exact capture digest for destructive retirement")
	set.StringVar(&opts.sourceDSNFile, "source-dsn-file", strings.TrimSpace(getenv("SANDBOX0_LEGACY_SOURCE_DSN_FILE")), "owner-only file containing the source PostgreSQL DSN")
	set.StringVar(&opts.targetDSNFile, "target-dsn-file", strings.TrimSpace(getenv("SANDBOX0_MIGRATION_TARGET_DSN_FILE")), "owner-only file containing the target PostgreSQL DSN")
	set.StringVar(&opts.sourceManagerConfigFile, "source-manager-config-file", "", "owner-only source manager config used to read legacy objects")
	set.StringVar(&opts.targetManagerConfigFile, "target-manager-config-file", "", "owner-only target manager config used by prepare and build")
	set.StringVar(&opts.targetClusterID, "target-cluster-id", "", "target Nomad cluster ID")
	set.StringVar(&opts.sourceManagerURL, "source-manager-url", "", "loopback URL for the frozen ACK manager")
	set.StringVar(&opts.sourceInternalKeyFile, "source-internal-private-key-file", "", "owner-only ACK data-plane signing key")
	set.StringVar(&opts.platformArch, "platform-architecture", "amd64", "canonical OCI architecture")
	set.StringVar(&opts.platformVariant, "platform-variant", "", "canonical OCI architecture variant")
	set.StringVar(&opts.memoryPerCPU, "memory-per-cpu", "2Gi", "target resource policy memory per CPU")
	set.StringVar(&opts.maxMemory, "max-memory", "256Gi", "target resource policy sandbox memory ceiling")
	set.StringVar(&opts.workerID, "worker-id", "", "durable build worker identity")
	set.DurationVar(&opts.buildLeaseTTL, "build-lease-ttl", defaultBuildLeaseTTL, "durable build lease TTL")
	set.DurationVar(&opts.buildLeaseRenewal, "build-lease-renewal", defaultBuildLeaseRenewal, "durable build lease renewal interval")
	set.StringVar(&opts.output, "output", "", "optional owner-only JSON evidence path")
	set.DurationVar(&opts.timeout, "timeout", 0, "operation timeout (defaults to 12h for build and 2m otherwise)")
	if err := set.Parse(args); err != nil {
		return options{}, err
	}
	if set.NArg() != 0 {
		return options{}, fmt.Errorf("unexpected positional arguments")
	}
	opts.mode = strings.TrimSpace(opts.mode)
	if !isSupportedMode(opts.mode) {
		return options{}, fmt.Errorf("unsupported migration mode %q", opts.mode)
	}
	opts.sessionID = strings.TrimSpace(opts.sessionID)
	if modeRequiresSession(opts.mode) && (opts.sessionID == "" || len(opts.sessionID) > 128) {
		return options{}, fmt.Errorf("session-id is required and must not exceed 128 bytes")
	}
	opts.confirmSourceDigest = strings.TrimSpace(opts.confirmSourceDigest)
	if opts.mode == modeRetire {
		parsed, err := digest.Parse(opts.confirmSourceDigest)
		if err != nil || parsed.Algorithm() != digest.SHA256 || parsed.String() != opts.confirmSourceDigest {
			return options{}, fmt.Errorf("confirm-source-catalog-digest must be the canonical SHA-256 capture digest")
		}
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
	opts.sourceManagerURL = strings.TrimSpace(opts.sourceManagerURL)
	opts.sourceInternalKeyFile = strings.TrimSpace(opts.sourceInternalKeyFile)
	opts.sourceDSNFile = strings.TrimSpace(opts.sourceDSNFile)
	opts.targetDSNFile = strings.TrimSpace(opts.targetDSNFile)
	opts.sourceManagerConfigFile = strings.TrimSpace(opts.sourceManagerConfigFile)
	opts.targetManagerConfigFile = strings.TrimSpace(opts.targetManagerConfigFile)
	opts.workerID = strings.TrimSpace(opts.workerID)
	if modeRequiresSource(opts.mode) {
		if err := validateDSNSource("source", opts.sourceDSNFile, strings.TrimSpace(getenv("SANDBOX0_LEGACY_SOURCE_DSN"))); err != nil {
			return options{}, err
		}
	}
	if modeRequiresTarget(opts.mode) {
		if err := validateDSNSource("target", opts.targetDSNFile, strings.TrimSpace(getenv("SANDBOX0_MIGRATION_TARGET_DSN"))); err != nil {
			return options{}, err
		}
	}
	if opts.mode == modePause || opts.mode == modePauseAccess {
		if _, err := parseLoopbackManagerURL(opts.sourceManagerURL); err != nil {
			return options{}, err
		}
		if opts.sourceInternalKeyFile == "" {
			return options{}, fmt.Errorf("source-internal-private-key-file is required for pause mode")
		}
	}
	if (opts.mode == modePrepare || opts.mode == modeBuild) && opts.targetManagerConfigFile == "" {
		return options{}, fmt.Errorf("target-manager-config-file is required for %s mode", opts.mode)
	}
	if opts.mode == modeBuild {
		if opts.sourceManagerConfigFile == "" {
			opts.sourceManagerConfigFile = opts.targetManagerConfigFile
		}
		if opts.workerID == "" {
			hostname, err := os.Hostname()
			if err != nil {
				return options{}, fmt.Errorf("derive build worker identity: %w", err)
			}
			opts.workerID = "legacy-ack." + opts.sessionID + "." + strings.TrimSpace(hostname)
		}
		if len(opts.workerID) > 256 {
			return options{}, fmt.Errorf("worker-id must not exceed 256 bytes")
		}
		if opts.buildLeaseTTL < legacyackmigration.MinTargetBuildLeaseTTL ||
			opts.buildLeaseTTL > legacyackmigration.MaxTargetBuildLeaseTTL || opts.buildLeaseTTL%time.Millisecond != 0 {
			return options{}, fmt.Errorf("build-lease-ttl is outside supported bounds")
		}
		if opts.buildLeaseRenewal <= 0 || opts.buildLeaseRenewal >= opts.buildLeaseTTL ||
			opts.buildLeaseRenewal%time.Millisecond != 0 {
			return options{}, fmt.Errorf("build-lease-renewal must be positive and shorter than the lease TTL")
		}
	}
	if opts.timeout == 0 {
		switch opts.mode {
		case modeBuild:
			opts.timeout = defaultBuildTimeout
		case modePause, modePauseAccess:
			opts.timeout = defaultPauseTimeout
		default:
			opts.timeout = defaultControlTimeout
		}
	}
	if opts.timeout <= 0 {
		return options{}, fmt.Errorf("timeout must be positive")
	}
	return opts, nil
}

func isSupportedMode(mode string) bool {
	switch mode {
	case modeInventory, modePreflight, modePauseAccess, modePause, modeValidate, modeCapture, modeRetire, modePrepare, modeBuild, modeCommit:
		return true
	default:
		return false
	}
}

func modeRequiresSession(mode string) bool {
	return mode == modeCapture || mode == modeRetire || mode == modePrepare || mode == modeBuild || mode == modeCommit
}

func modeRequiresSource(mode string) bool {
	return mode == modeInventory || mode == modePreflight || mode == modePauseAccess || mode == modePause || mode == modeValidate || mode == modeCapture
}

func modeRequiresTarget(mode string) bool {
	return mode == modeCapture || mode == modeRetire || mode == modePrepare || mode == modeBuild || mode == modeCommit
}

func validateDSNSource(role, path, environmentValue string) error {
	if path != "" && environmentValue != "" {
		return fmt.Errorf("configure exactly one of %s DSN file or environment", role)
	}
	if path == "" && environmentValue == "" {
		return fmt.Errorf("%s database DSN is required through an owner-only file or environment", role)
	}
	return nil
}
