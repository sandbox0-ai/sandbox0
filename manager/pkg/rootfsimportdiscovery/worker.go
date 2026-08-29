// Package rootfsimportdiscovery discovers ready image templates and
// idempotently creates their durable OCI-to-block import operations.
package rootfsimportdiscovery

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsimporter"
	"github.com/sandbox0-ai/sandbox0/pkg/sandboxspec"
	"github.com/sandbox0-ai/sandbox0/pkg/template"
	templatestore "github.com/sandbox0-ai/sandbox0/pkg/template/store"
)

const (
	DefaultInterval = 5 * time.Second
	DefaultPageSize = 100
	MaxPageSize     = 1000
)

type ImportStore interface {
	GetReadyRootFSBaseArtifact(
		context.Context,
		string,
		sandboxstore.RootFSArtifactPlatform,
		sandboxstore.ReadyRootFSArtifactRequirements,
	) (*sandboxstore.RootFSBaseArtifact, error)
	BeginRootFSImport(
		context.Context,
		*sandboxstore.BeginRootFSImportRequest,
	) (*sandboxstore.RootFSImportOperation, error)
}

type Config struct {
	Sources          templatestore.ImageSourceStore
	Imports          ImportStore
	Platforms        []sandboxstore.RootFSArtifactPlatform
	FormatGeneration int
	ProcdProtocol    string
	ProcdDigest      string
	BlockOptions     rootfsblock.BuildOptions
	Interval         time.Duration
	PageSize         int
}

type Result struct {
	Templates    int
	Requirements int
	Ready        int
	Ensured      int
	Failed       int
	Wrapped      bool
}

// Worker maintains only an in-process scan cursor. PostgreSQL import
// operations are the durable active-active work authority.
type Worker struct {
	config Config

	mu     sync.Mutex
	cursor templatestore.ImageSourceCursor
}

func New(config Config) (*Worker, error) {
	if config.Sources == nil || config.Imports == nil {
		return nil, fmt.Errorf("template image sources and RootFS import store are required")
	}
	if config.FormatGeneration <= 0 {
		return nil, fmt.Errorf("RootFS format generation must be positive")
	}
	if config.Interval == 0 {
		config.Interval = DefaultInterval
	}
	if config.Interval < 10*time.Millisecond {
		return nil, fmt.Errorf("RootFS import discovery interval must be at least 10ms")
	}
	if config.PageSize == 0 {
		config.PageSize = DefaultPageSize
	}
	if config.PageSize < 1 || config.PageSize > MaxPageSize {
		return nil, fmt.Errorf("RootFS import discovery page size must be within 1..%d", MaxPageSize)
	}

	platforms := append([]sandboxstore.RootFSArtifactPlatform(nil), config.Platforms...)
	sort.Slice(platforms, func(i, j int) bool {
		if platforms[i].OS != platforms[j].OS {
			return platforms[i].OS < platforms[j].OS
		}
		if platforms[i].Architecture != platforms[j].Architecture {
			return platforms[i].Architecture < platforms[j].Architecture
		}
		return platforms[i].Variant < platforms[j].Variant
	})
	unique := platforms[:0]
	for _, platform := range platforms {
		if err := platform.Validate(); err != nil {
			return nil, fmt.Errorf("RootFS import discovery platform: %w", err)
		}
		if len(unique) == 0 || unique[len(unique)-1] != platform {
			unique = append(unique, platform)
		}
	}
	if len(unique) == 0 {
		return nil, fmt.Errorf("at least one RootFS artifact platform is required")
	}
	config.Platforms = append([]sandboxstore.RootFSArtifactPlatform(nil), unique...)
	return &Worker{config: config}, nil
}

// Run continuously scans bounded pages. A malformed historical template is
// reported but cannot permanently starve later keyset pages.
func (w *Worker) Run(ctx context.Context, observe func(Result, error)) {
	if w == nil {
		return
	}
	for {
		result, err := w.RunOnce(ctx)
		if observe != nil {
			observe(result, err)
		}
		timer := time.NewTimer(w.config.Interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

func (w *Worker) RunOnce(ctx context.Context) (Result, error) {
	if w == nil {
		return Result{}, fmt.Errorf("RootFS import discovery worker is not configured")
	}
	w.mu.Lock()
	defer w.mu.Unlock()

	sources, err := w.config.Sources.ListImageSourcesForRootFSImport(ctx, w.cursor, w.config.PageSize)
	if err != nil {
		return Result{}, err
	}
	result := Result{Templates: len(sources)}
	var failures []error
	for _, source := range sources {
		if err := w.ensureSource(ctx, source, &result); err != nil {
			result.Failed++
			failures = append(failures, fmt.Errorf(
				"template %s/%s/%s: %w",
				source.Cursor.Scope,
				source.Cursor.TeamID,
				source.Cursor.TemplateID,
				err,
			))
		}
	}
	if len(sources) < w.config.PageSize {
		w.cursor = templatestore.ImageSourceCursor{}
		result.Wrapped = true
	} else {
		w.cursor = sources[len(sources)-1].Cursor
	}
	return result, errors.Join(failures...)
}

func (w *Worker) ensureSource(
	ctx context.Context,
	source templatestore.ImageSource,
	result *Result,
) error {
	sourceDigest, err := rootfsimporter.PinnedSourceDigest(source.Image)
	if err != nil {
		return fmt.Errorf("source image is not digest-pinned: %w", err)
	}
	spec := sandboxspec.TemplateSpec{}
	spec.MainContainer.Resources.EphemeralStorage = source.EphemeralStorage
	logicalSize, err := template.ResolveRootFSLogicalSize(spec)
	if err != nil {
		return err
	}

	var failures []error
	for _, platform := range w.config.Platforms {
		result.Requirements++
		artifact, readyErr := w.config.Imports.GetReadyRootFSBaseArtifact(
			ctx,
			sourceDigest.String(),
			platform,
			sandboxstore.ReadyRootFSArtifactRequirements{
				FormatGeneration: w.config.FormatGeneration,
				LogicalSizeBytes: logicalSize,
				ProcdProtocol:    w.config.ProcdProtocol,
				ProcdDigest:      w.config.ProcdDigest,
			},
		)
		if readyErr == nil && artifact != nil {
			result.Ready++
			continue
		}
		if readyErr != nil && !errors.Is(readyErr, sandboxstore.ErrRootFSBaseArtifactNotFound) {
			failures = append(failures, fmt.Errorf("inspect %s/%s/%s artifact: %w",
				platform.OS, platform.Architecture, platform.Variant, readyErr))
			continue
		}

		operationSpec := rootfsimporter.OperationSpec{
			SourceOCIRef: source.Image,
			Platform: rootfsimporter.ReadyArtifactPlatform{
				OS: platform.OS, Architecture: platform.Architecture, Variant: platform.Variant,
			},
			FormatGeneration: w.config.FormatGeneration,
			ProcdProtocol:    w.config.ProcdProtocol,
			ProcdDigest:      w.config.ProcdDigest,
			LogicalSizeBytes: logicalSize,
			BlockOptions:     w.config.BlockOptions,
		}
		operationID, normalized, operationErr := rootfsimporter.DeterministicOperation(operationSpec)
		if operationErr != nil {
			failures = append(failures, fmt.Errorf("construct %s/%s/%s import: %w",
				platform.OS, platform.Architecture, platform.Variant, operationErr))
			continue
		}
		operation, operationErr := w.config.Imports.BeginRootFSImport(ctx, &sandboxstore.BeginRootFSImportRequest{
			OperationID: operationID,
			Spec:        normalized,
		})
		if operationErr != nil {
			failures = append(failures, fmt.Errorf("ensure %s/%s/%s import: %w",
				platform.OS, platform.Architecture, platform.Variant, operationErr))
			continue
		}
		if operation == nil {
			failures = append(failures, fmt.Errorf("ensure %s/%s/%s import returned no durable operation",
				platform.OS, platform.Architecture, platform.Variant))
			continue
		}
		switch operation.State {
		case sandboxstore.RootFSImportStatePending, sandboxstore.RootFSImportStateBuilding:
			result.Ensured++
		case sandboxstore.RootFSImportStateReady:
			result.Ready++
		case sandboxstore.RootFSImportStateAbandoned:
			failures = append(failures, fmt.Errorf("%s/%s/%s import %s is abandoned: %s",
				platform.OS, platform.Architecture, platform.Variant,
				operation.ID, operation.AbandonReason))
		default:
			failures = append(failures, fmt.Errorf("%s/%s/%s import %s has invalid state %q",
				platform.OS, platform.Architecture, platform.Variant,
				operation.ID, operation.State))
		}
	}
	return errors.Join(failures...)
}
