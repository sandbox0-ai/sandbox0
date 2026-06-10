package service

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/tarball"
	"github.com/google/go-containerregistry/pkg/v1/types"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/registry"
	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	"go.uber.org/zap"
)

type RootFSCheckpointImage struct {
	PushRef string
	PullRef string
}

type RootFSCheckpointImagePublishRequest struct {
	TeamID      string
	CtldAddress string
	State       *SandboxRootFSState
}

type RootFSCheckpointImagePublisher interface {
	Publish(ctx context.Context, req RootFSCheckpointImagePublishRequest) (*RootFSCheckpointImage, error)
}

type rootFSDiffOpener interface {
	OpenRootFSDiffWithTimeout(ctx context.Context, ctldAddress string, req ctldapi.ReadRootFSDiffRequest, timeout time.Duration) (io.ReadCloser, error)
}

type RegistryRootFSCheckpointImagePublisher struct {
	registry *RegistryService
	ctld     rootFSDiffOpener
	logger   *zap.Logger
}

func NewRegistryRootFSCheckpointImagePublisher(registryService *RegistryService, ctld rootFSDiffOpener, logger *zap.Logger) *RegistryRootFSCheckpointImagePublisher {
	return &RegistryRootFSCheckpointImagePublisher{
		registry: registryService,
		ctld:     ctld,
		logger:   logger,
	}
}

func (p *RegistryRootFSCheckpointImagePublisher) Publish(ctx context.Context, req RootFSCheckpointImagePublishRequest) (*RootFSCheckpointImage, error) {
	if p == nil || p.registry == nil {
		return nil, fmt.Errorf("registry provider is required to publish rootfs checkpoint image")
	}
	if p.ctld == nil {
		return nil, fmt.Errorf("ctld client is required to read rootfs checkpoint diff")
	}
	state := req.State
	if state == nil {
		return nil, fmt.Errorf("rootfs state is required")
	}
	ctldAddress := strings.TrimSpace(req.CtldAddress)
	if ctldAddress == "" {
		return nil, fmt.Errorf("ctld address is required")
	}

	targetImage, err := rootFSCheckpointTargetImage(state)
	if err != nil {
		return nil, err
	}
	teamID := strings.TrimSpace(req.TeamID)
	if teamID == "" {
		teamID = state.TeamID
	}
	creds, err := p.registry.GetPushCredentials(ctx, registry.PushCredentialsRequest{
		TeamID:      teamID,
		TargetImage: targetImage,
	})
	if err != nil {
		return nil, fmt.Errorf("get checkpoint image registry credentials: %w", err)
	}
	if creds == nil || strings.TrimSpace(creds.PushRegistry) == "" {
		return nil, fmt.Errorf("checkpoint image registry returned no push registry")
	}
	pushRef := joinImageReference(creds.PushRegistry, targetImage)
	pullRegistry := strings.TrimSpace(creds.PullRegistry)
	if pullRegistry == "" {
		pullRegistry = creds.PushRegistry
	}
	pullRef := joinImageReference(pullRegistry, targetImage)

	baseImageRef, err := checkpointBaseImageRef(state)
	if err != nil {
		return nil, err
	}
	baseRef, err := name.ParseReference(baseImageRef, name.WeakValidation)
	if err != nil {
		return nil, fmt.Errorf("parse checkpoint base image ref %q: %w", baseImageRef, err)
	}
	baseImage, err := remote.Image(baseRef, remoteOptionsForRegistryCredentials(ctx, creds, baseRef)...)
	if err != nil {
		return nil, fmt.Errorf("pull checkpoint base image %q: %w", baseImageRef, err)
	}

	diffReader, err := p.ctld.OpenRootFSDiffWithTimeout(ctx, ctldAddress, ctldapi.ReadRootFSDiffRequest{
		Descriptor: rootFSDiffDescriptorFromState(state),
	}, sandboxRootFSOperationTimeout)
	if err != nil {
		return nil, fmt.Errorf("open rootfs checkpoint diff: %w", err)
	}
	defer diffReader.Close()

	mediaType := strings.TrimSpace(state.DiffMediaType)
	if mediaType == "" {
		mediaType = "application/vnd.oci.image.layer.v1.tar"
	}
	layer, err := tarball.LayerFromReader(diffReader, tarball.WithMediaType(types.MediaType(mediaType)))
	if err != nil {
		return nil, fmt.Errorf("create checkpoint layer: %w", err)
	}
	image, err := mutate.AppendLayers(baseImage, layer)
	if err != nil {
		return nil, fmt.Errorf("append checkpoint layer: %w", err)
	}

	pushReference, err := name.ParseReference(pushRef, name.WeakValidation)
	if err != nil {
		return nil, fmt.Errorf("parse checkpoint image ref %q: %w", pushRef, err)
	}
	options := []remote.Option{remote.WithContext(ctx)}
	if strings.TrimSpace(creds.Username) != "" || strings.TrimSpace(creds.Password) != "" {
		options = append(options, remote.WithAuth(authn.FromConfig(authn.AuthConfig{
			Username: creds.Username,
			Password: creds.Password,
		})))
	}
	if err := remote.Write(pushReference, image, options...); err != nil {
		return nil, fmt.Errorf("push checkpoint image %q: %w", pushRef, err)
	}
	if p.logger != nil {
		p.logger.Info("Published rootfs checkpoint image",
			zap.String("sandboxID", state.SandboxID),
			zap.String("runtime", state.Runtime),
			zap.String("pushRef", pushRef),
			zap.String("pullRef", pullRef),
		)
	}
	return &RootFSCheckpointImage{PushRef: pushRef, PullRef: pullRef}, nil
}

func remoteOptionsForRegistryCredentials(ctx context.Context, creds *registry.Credential, ref name.Reference) []remote.Option {
	options := []remote.Option{remote.WithContext(ctx)}
	if registryCredentialsMatchReference(creds, ref) {
		options = append(options, remote.WithAuth(authn.FromConfig(authn.AuthConfig{
			Username: creds.Username,
			Password: creds.Password,
		})))
	}
	return options
}

func registryCredentialsMatchReference(creds *registry.Credential, ref name.Reference) bool {
	if creds == nil || ref == nil {
		return false
	}
	if strings.TrimSpace(creds.Username) == "" && strings.TrimSpace(creds.Password) == "" {
		return false
	}
	refHost := strings.ToLower(strings.TrimSpace(ref.Context().RegistryStr()))
	if refHost == "" {
		return false
	}
	for _, registryRef := range []string{creds.PushRegistry, creds.PullRegistry} {
		if registryCredentialHost(registryRef) == refHost {
			return true
		}
	}
	return false
}

func registryCredentialHost(raw string) string {
	raw = strings.ToLower(strings.TrimSpace(raw))
	raw = strings.TrimPrefix(raw, "https://")
	raw = strings.TrimPrefix(raw, "http://")
	if idx := strings.Index(raw, "/"); idx >= 0 {
		raw = raw[:idx]
	}
	return raw
}

func rootFSDiffDescriptorFromState(state *SandboxRootFSState) ctldapi.RootFSDiffDescriptor {
	if state == nil {
		return ctldapi.RootFSDiffDescriptor{}
	}
	return ctldapi.RootFSDiffDescriptor{
		MediaType: state.DiffMediaType,
		Digest:    state.DiffDigest,
		Size:      state.DiffSize,
		ObjectKey: state.DiffObjectKey,
	}
}

func rootFSCheckpointTargetImage(state *SandboxRootFSState) (string, error) {
	if state == nil {
		return "", fmt.Errorf("rootfs state is required")
	}
	sandboxID := sanitizeImagePathComponent(state.SandboxID)
	if sandboxID == "" {
		return "", fmt.Errorf("sandbox id is required")
	}
	shortDigest := shortImageDigest(state.DiffDigest)
	if shortDigest == "" {
		return "", fmt.Errorf("diff digest is required")
	}
	return fmt.Sprintf("sandbox-rootfs/%s:g%d-%s", sandboxID, state.RuntimeGeneration, shortDigest), nil
}

func joinImageReference(registryHost, image string) string {
	return strings.TrimRight(strings.TrimSpace(registryHost), "/") + "/" + strings.TrimLeft(strings.TrimSpace(image), "/")
}

func sanitizeImagePathComponent(raw string) string {
	raw = strings.ToLower(strings.TrimSpace(raw))
	var b strings.Builder
	lastDash := false
	for _, r := range raw {
		allowed := (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '.' || r == '_' || r == '-'
		if allowed {
			b.WriteRune(r)
			lastDash = false
			continue
		}
		if !lastDash {
			b.WriteByte('-')
			lastDash = true
		}
	}
	value := strings.Trim(b.String(), ".-_")
	if value == "" {
		return ""
	}
	if ch := value[0]; ch < 'a' || ch > 'z' {
		if ch < '0' || ch > '9' {
			value = "s" + value
		}
	}
	return value
}

func shortImageDigest(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	if _, value, ok := strings.Cut(raw, ":"); ok {
		raw = value
	}
	raw = strings.TrimSpace(raw)
	if len(raw) > 16 {
		return raw[:16]
	}
	return raw
}
