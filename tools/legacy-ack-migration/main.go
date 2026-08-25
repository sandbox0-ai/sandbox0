// Command legacy-ack-migration performs a capture-fenced, one-time conversion
// of a frozen ACK-era durable manager graph into the Nomad block-COW schema.
package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/legacyackmigration"
	templatepkg "github.com/sandbox0-ai/sandbox0/pkg/template"
)

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
	ctx, cancel := context.WithTimeout(context.Background(), opts.timeout)
	defer cancel()
	platform := ocispec.Platform{OS: "linux", Architecture: opts.platformArch, Variant: opts.platformVariant}
	normalizeOptions := legacyackmigration.NormalizeOptions{
		Platform: platform, TargetClusterID: opts.targetClusterID,
		ResourcePolicy: templatepkg.NewResourcePolicy(opts.memoryPerCPU, opts.maxMemory),
	}

	var result report
	var operationErr error
	switch opts.mode {
	case modeInventory, modePreflight, modeValidate:
		catalog, readErr := readSourceCatalog(ctx, opts, getenv)
		if readErr != nil {
			return readErr
		}
		result, _, operationErr = catalogReport(opts, platform, normalizeOptions, catalog)
	case modePauseAccess, modePause:
		catalog, readErr := readSourceCatalog(ctx, opts, getenv)
		if readErr != nil {
			return readErr
		}
		preflightOpts := opts
		preflightOpts.mode = modePreflight
		result, _, operationErr = catalogReport(preflightOpts, platform, normalizeOptions, catalog)
		result.Mode = opts.mode
		if operationErr == nil {
			var finalCatalog *legacyackmigration.Catalog
			result.Pause, finalCatalog, operationErr = pauseSourceSandboxes(
				ctx, opts, getenv, normalizeOptions, opts.mode == modePauseAccess,
			)
			if finalCatalog != nil && opts.mode == modePause {
				var finalReport report
				var finalErr error
				finalReport, _, finalErr = catalogReport(opts, platform, normalizeOptions, finalCatalog)
				finalReport.Pause = result.Pause
				result = finalReport
				if operationErr == nil {
					operationErr = finalErr
				}
			}
		}
	case modeCapture:
		catalog, readErr := readSourceCatalog(ctx, opts, getenv)
		if readErr != nil {
			return readErr
		}
		var normalized *legacyackmigration.NormalizedCatalog
		result, normalized, operationErr = catalogReport(opts, platform, normalizeOptions, catalog)
		if operationErr == nil && normalized != nil {
			var captured *legacyackmigration.CapturedCatalog
			captured, operationErr = captureSourceCatalog(ctx, opts, getenv, catalog)
			if captured != nil {
				result.SourceCatalogDigest = captured.SourceCatalogDigest
				result.Capture = &captureSummary{CapturedAt: captured.CapturedAt}
			}
		}
	case modeRetire, modePrepare, modeBuild, modeCommit:
		var target *targetContext
		target, operationErr = loadTargetContext(ctx, opts, getenv, normalizeOptions)
		if operationErr == nil {
			defer target.Close()
			result, _, operationErr = catalogReport(opts, platform, normalizeOptions, &target.capture.Catalog)
			result.SourceCatalogDigest = target.capture.SourceCatalogDigest
		}
		if operationErr == nil {
			switch opts.mode {
			case modeRetire:
				operationErr = retireSourceCatalog(ctx, opts, target, &result)
			case modePrepare:
				result.Preparation, operationErr = prepareTargetCatalog(ctx, opts, target)
			case modeBuild:
				result.Build, operationErr = buildTargetCatalog(ctx, opts, target)
			case modeCommit:
				result.Commit, operationErr = target.store.CommitCatalog(ctx, opts.sessionID, target.normalized)
			}
		}
	default:
		return fmt.Errorf("unsupported migration mode %q", opts.mode)
	}

	if result.FormatVersion != 0 {
		if err := writeReport(opts.output, result, stdout); err != nil {
			return err
		}
	}
	if operationErr != nil {
		if opts.mode == modeInventory {
			return nil
		}
		if opts.mode == modePreflight {
			return fmt.Errorf("migration preflight validation failed: %w", operationErr)
		}
		if opts.mode == modePause {
			return fmt.Errorf("migration pause failed: %w", operationErr)
		}
		if opts.mode == modePauseAccess {
			return fmt.Errorf("migration pause access verification failed: %w", operationErr)
		}
		if opts.mode == modeValidate || opts.mode == modeCapture {
			return fmt.Errorf("migration freeze validation failed: %w", operationErr)
		}
		return operationErr
	}
	return nil
}

func retireSourceCatalog(
	ctx context.Context,
	opts options,
	target *targetContext,
	result *report,
) error {
	if target.capture.SourceCatalogDigest != opts.confirmSourceDigest {
		return fmt.Errorf("retirement confirmation digest does not match the durable capture")
	}
	captures, err := legacyackmigration.NewCaptureStore(target.pool)
	if err != nil {
		return err
	}
	retired, err := captures.RetireLegacyManagerSchema(ctx, opts.sessionID)
	if retired != nil {
		result.Capture = &captureSummary{CapturedAt: retired.CapturedAt, RetiredAt: &retired.RetiredAt}
	}
	return err
}
