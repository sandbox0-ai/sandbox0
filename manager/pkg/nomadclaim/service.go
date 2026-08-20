// Package nomadclaim implements the runtime-neutral manager claim API over
// region-authoritative Nomad warm slots.
package nomadclaim

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net"
	"net/url"
	"reflect"
	"strings"
	"time"

	distref "github.com/distribution/reference"
	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/networkpolicy"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/runtimeslotclaim"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/service"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	"github.com/sandbox0-ai/sandbox0/pkg/quota"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
	templatepkg "github.com/sandbox0-ai/sandbox0/pkg/template"
	templatestore "github.com/sandbox0-ai/sandbox0/pkg/template/store"
	"go.uber.org/zap"
)

// Store is the durable sandbox and block-COW product boundary needed before a
// slot can receive writer authority.
type Store interface {
	GetSandbox(context.Context, string) (*sandboxstore.SandboxRecord, error)
	UpsertSandbox(context.Context, *sandboxstore.SandboxRecord) error
	ReserveSandboxClaim(context.Context, *sandboxstore.ReserveSandboxClaimRequest) (*sandboxstore.SandboxRecord, error)
	GetReadyRootFSBaseArtifact(context.Context, string, sandboxstore.RootFSArtifactPlatform, int) (*sandboxstore.RootFSBaseArtifact, error)
	GetReadyRootFSBaseArtifactByDigest(context.Context, string, sandboxstore.RootFSArtifactPlatform) (*sandboxstore.RootFSBaseArtifact, error)
	EnsureInitialRootFSGeneration(context.Context, *sandboxstore.EnsureInitialRootFSGenerationRequest) (*sandboxstore.RootFSFilesystem, *sandboxstore.RootFSGeneration, error)
	GetRootFSSnapshot(context.Context, string, string) (*sandboxstore.RootFSSnapshot, error)
	RestoreRootFSFromSnapshot(context.Context, *sandboxstore.RestoreRootFSFromSnapshotRequest) (*sandboxstore.RootFSFilesystem, error)
}

// QuotaLimitStore resolves region-authoritative team capacity policy.
type QuotaLimitStore interface {
	GetLimit(context.Context, string, quota.Dimension) (*quota.Limit, error)
}

type planner interface {
	Claim(context.Context, runtimeslotclaim.Request) (*runtimeslotclaim.Result, error)
}

// Config defines logical claim policy independently from the node listener.
type Config struct {
	Store           Store
	Templates       templatestore.TemplateStore
	Profiles        *ProfileCatalog
	Planner         planner
	QuotaLimits     QuotaLimitStore
	NetworkPolicies *networkpolicy.NetworkPolicyService
	ResourcePolicy  templatepkg.ResourcePolicy
	DefaultTTL      time.Duration
	Now             func() time.Time
	Logger          *zap.Logger
}

// Service claims Nomad slots without creating or mutating Kubernetes runtime
// objects.
type Service struct {
	store           Store
	templates       templatestore.TemplateStore
	profiles        *ProfileCatalog
	planner         planner
	quotaLimits     QuotaLimitStore
	networkPolicies *networkpolicy.NetworkPolicyService
	resourcePolicy  templatepkg.ResourcePolicy
	defaultTTL      time.Duration
	now             func() time.Time
	logger          *zap.Logger
}

// New validates all claim authorities. There is no partially configured mode.
func New(config Config) (*Service, error) {
	if config.Store == nil || config.Templates == nil || config.Profiles == nil ||
		config.Planner == nil || config.QuotaLimits == nil || config.NetworkPolicies == nil {
		return nil, fmt.Errorf("Nomad claim store, templates, profiles, planner, quota limits, and network policy service are required")
	}
	if config.DefaultTTL < 0 || config.DefaultTTL/time.Second > math.MaxInt32 {
		return nil, fmt.Errorf("default TTL must fit a non-negative int32 second count")
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	if config.Logger == nil {
		config.Logger = zap.NewNop()
	}
	return &Service{
		store: config.Store, templates: config.Templates, profiles: config.Profiles,
		planner: config.Planner, quotaLimits: config.QuotaLimits, networkPolicies: config.NetworkPolicies,
		resourcePolicy: config.ResourcePolicy, defaultTTL: config.DefaultTTL,
		now: config.Now, logger: config.Logger,
	}, nil
}

// ClaimSandbox prepares a durable block-COW filesystem and returns only after
// authenticated procd command readiness has been committed regionally.
func (s *Service) ClaimSandbox(ctx context.Context, request *service.ClaimRequest) (*service.ClaimResponse, error) {
	if request == nil {
		return nil, fmt.Errorf("%w: claim request is required", service.ErrInvalidClaimRequest)
	}
	req := *request
	req.TeamID = strings.TrimSpace(request.TeamID)
	req.UserID = strings.TrimSpace(request.UserID)
	req.OperationID = strings.TrimSpace(request.OperationID)
	canonicalTemplate, err := naming.CanonicalTemplateID(request.Template)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", service.ErrInvalidClaimRequest, err)
	}
	req.Template = canonicalTemplate
	if req.TeamID == "" || req.OperationID == "" {
		return nil, fmt.Errorf("%w: signed team and operation identities are required", service.ErrInvalidClaimRequest)
	}
	req.Config = service.CloneSandboxConfig(request.Config)
	if err := service.NormalizeSandboxConfigForPersistence(req.Config); err != nil {
		return nil, err
	}
	if req.Config != nil && len(req.Config.Services) > 0 {
		return nil, fmt.Errorf("%w: Nomad app-service projection is not configured", service.ErrDataPlaneNotReady)
	}

	tpl, err := s.templates.GetTemplateForTeam(ctx, req.TeamID, req.Template)
	if err != nil {
		return nil, fmt.Errorf("load template: %w", err)
	}
	if tpl == nil {
		return nil, fmt.Errorf("%w: %s", service.ErrTemplateNotFound, req.Template)
	}
	if !tpl.ReadyForClaim() {
		return nil, templatepkg.ErrTemplateNotReady
	}
	quota, err := s.effectiveResources(tpl.Spec, req.Config)
	if err != nil {
		return nil, err
	}
	profile, ok := s.profiles.Resolve(quota.CPU, quota.Memory)
	if !ok {
		return nil, fmt.Errorf("%w: no Nomad warm-slot profile for cpu=%s memory=%s",
			service.ErrDataPlaneNotReady, quota.CPU.String(), quota.Memory.String())
	}
	if tpl.Spec.ClusterId != nil && strings.TrimSpace(*tpl.Spec.ClusterId) != "" &&
		strings.TrimSpace(*tpl.Spec.ClusterId) != profile.ClusterID {
		return nil, fmt.Errorf("%w: template cluster has no matching Nomad warm-slot profile", service.ErrDataPlaneNotReady)
	}
	sandboxID, err := naming.SandboxNameForOperation(profile.ClusterID, req.Template, req.OperationID)
	if err != nil {
		return nil, fmt.Errorf("derive sandbox ID: %w", err)
	}
	if req.SandboxID != "" && req.SandboxID != sandboxID {
		return nil, fmt.Errorf("%w: internal sandbox ID does not match operation", service.ErrClaimConflict)
	}
	req.SandboxID = sandboxID
	if req.RuntimeGeneration <= 0 {
		req.RuntimeGeneration = 1
	}
	if req.RuntimeGeneration != 1 {
		return nil, fmt.Errorf("%w: initial Nomad claim runtime generation must be 1", service.ErrInvalidClaimRequest)
	}

	policy, credentials, err := s.networkPolicy(tpl.Spec, &req)
	if err != nil {
		return nil, err
	}
	if len(credentials) > 0 {
		return nil, fmt.Errorf("%w: Nomad credential binding projection is not configured", service.ErrDataPlaneNotReady)
	}
	assignment := runtimeAssignment(tpl.Spec, &req)
	if err := assignment.Validate(); err != nil {
		return nil, fmt.Errorf("%w: runtime assignment: %v", service.ErrInvalidClaimRequest, err)
	}
	rootFS, err := s.prepareRootFS(ctx, tpl, &req, profile.ArtifactPlatform)
	if err != nil {
		if errors.Is(err, sandboxstore.ErrRootFSBaseArtifactNotFound) {
			return nil, fmt.Errorf("%w: %v", service.ErrDataPlaneNotReady, err)
		}
		return nil, err
	}

	now := s.now().UTC()
	record := s.claimRecord(tpl, &req, profile.ClusterID, now)
	if err := s.ensureClaimRecord(ctx, record); err != nil {
		return nil, err
	}
	if err := s.initializeRootFS(ctx, &req, rootFS); err != nil {
		return nil, err
	}

	result, err := s.planner.Claim(ctx, runtimeslotclaim.Request{
		OperationID: req.OperationID, SandboxID: sandboxID,
		TeamID: req.TeamID, UserID: req.UserID,
		CompatibilityDigest: profile.CompatibilityDigest, ClusterID: profile.ClusterID,
		NetworkPolicy: policy, Runtime: assignment, StartedAt: req.StartedAt,
	})
	if errors.Is(err, sandboxstore.ErrRuntimeSlotUnavailable) {
		return nil, fmt.Errorf("%w: %v", service.ErrDataPlaneNotReady, err)
	}
	if err != nil {
		return nil, err
	}
	if result == nil || result.Slot == nil {
		return nil, fmt.Errorf("Nomad slot planner returned no runtime binding")
	}
	record.CurrentPodNamespace = result.Slot.AllocationNamespace
	record.CurrentPodName = result.Slot.AllocationID
	if err := s.store.UpsertSandbox(ctx, record); err != nil {
		return nil, fmt.Errorf("persist Nomad runtime binding: %w", err)
	}
	s.logger.Info("Claimed Nomad sandbox",
		zap.String("sandboxID", sandboxID), zap.String("operationID", req.OperationID),
		zap.String("slotID", result.Slot.ID), zap.Duration("endToEndDuration", result.Duration),
	)
	clusterID := profile.ClusterID
	return &service.ClaimResponse{
		SandboxID: sandboxID, Status: "running", ProcdAddress: result.ProcdAddress,
		PodName: result.Slot.AllocationID, Template: req.Template, ClusterId: &clusterID,
	}, nil
}

func (s *Service) effectiveResources(spec v1alpha1.SandboxTemplateSpec, config *sandboxstore.SandboxConfig) (v1alpha1.ResourceQuota, error) {
	quota := *spec.MainContainer.Resources.DeepCopy()
	if config != nil && config.Resources != nil {
		memory, err := s.resourcePolicy.ParseMemory(config.Resources.Memory, "config.resources.memory")
		if err != nil {
			return v1alpha1.ResourceQuota{}, fmt.Errorf("%w: %v", service.ErrInvalidClaimRequest, err)
		}
		quota.Memory = memory
		quota.CPU = templatepkg.CPUForMemory(memory, s.resourcePolicy.MemoryPerCPU())
	}
	if err := s.resourcePolicy.ValidateMaxMemory(quota.Memory, "sandbox memory limit"); err != nil {
		return v1alpha1.ResourceQuota{}, fmt.Errorf("%w: %v", service.ErrInvalidClaimRequest, err)
	}
	return v1alpha1.NormalizeSandboxResourceQuota(quota), nil
}

func (s *Service) networkPolicy(spec v1alpha1.SandboxTemplateSpec, req *service.ClaimRequest) (string, []v1alpha1.CredentialBinding, error) {
	var requestPolicy *v1alpha1.SandboxNetworkPolicy
	if req.Config != nil {
		requestPolicy = req.Config.Network
		if req.Config.Webhook != nil && strings.TrimSpace(req.Config.Webhook.URL) != "" {
			requestPolicy = appendWebhookPolicy(requestPolicy, req.Config.Webhook.URL)
		}
	}
	state := s.networkPolicies.BuildNetworkPolicyState(&networkpolicy.BuildNetworkPolicyRequest{
		SandboxID: req.SandboxID, TeamID: req.TeamID,
		TemplateSpec: spec.Network, RequestSpec: requestPolicy,
		TemplateBindings: credentialBindings(spec.Network), RequestBindings: requestCredentialBindings(req.Config),
	})
	if state == nil || state.PolicySpec == nil {
		return "", nil, fmt.Errorf("build Nomad network policy")
	}
	annotation, err := v1alpha1.NetworkPolicyToAnnotation(state.PolicySpec)
	if err != nil {
		return "", nil, fmt.Errorf("serialize Nomad network policy: %w", err)
	}
	return annotation, state.CredentialBindings, nil
}

type rootFSPlan struct {
	snapshotID         string
	sourceRef          string
	sourceDigest       string
	baseArtifactDigest string
}

func (s *Service) prepareRootFS(
	ctx context.Context,
	tpl *templatepkg.Template,
	req *service.ClaimRequest,
	platform sandboxstore.RootFSArtifactPlatform,
) (rootFSPlan, error) {
	if req.SnapshotID != "" {
		snapshotID := strings.TrimSpace(req.SnapshotID)
		if snapshotID != req.SnapshotID || templatepkg.IsBuildSnapshotID(snapshotID) {
			return rootFSPlan{}, sandboxstore.ErrRootFSSnapshotNotFound
		}
		snapshot, err := s.store.GetRootFSSnapshot(ctx, snapshotID, req.TeamID)
		if err != nil {
			return rootFSPlan{}, err
		}
		if snapshot == nil || snapshot.StorageFormat != sandboxstore.RootFSStorageFormatBlockCOWV1 {
			return rootFSPlan{}, sandboxstore.ErrRootFSSnapshotNotFound
		}
		artifact, err := s.store.GetReadyRootFSBaseArtifactByDigest(ctx, snapshot.BaseArtifactDigest, platform)
		if err != nil {
			return rootFSPlan{}, err
		}
		if artifact.SourceOCIDigest != snapshot.SourceOCIDigest || artifact.FormatGeneration != snapshot.FormatGeneration {
			return rootFSPlan{}, fmt.Errorf("%w: snapshot Base artifact attestation changed", sandboxstore.ErrRootFSBaseArtifactConflict)
		}
		return rootFSPlan{snapshotID: snapshotID}, nil
	}
	sourceRef := strings.TrimSpace(tpl.Spec.MainContainer.Image)
	sourceDigest, err := digestPinnedImage(sourceRef)
	if err != nil {
		return rootFSPlan{}, fmt.Errorf("%w: template image: %v", service.ErrInvalidClaimRequest, err)
	}
	artifact, err := s.store.GetReadyRootFSBaseArtifact(ctx, sourceDigest, platform, 0)
	if err != nil {
		return rootFSPlan{}, err
	}
	if artifact == nil {
		return rootFSPlan{}, sandboxstore.ErrRootFSBaseArtifactNotFound
	}
	return rootFSPlan{
		sourceRef: sourceRef, sourceDigest: sourceDigest,
		baseArtifactDigest: artifact.ArtifactDigest,
	}, nil
}

func (s *Service) initializeRootFS(ctx context.Context, req *service.ClaimRequest, plan rootFSPlan) error {
	if plan.snapshotID != "" {
		_, err := s.store.RestoreRootFSFromSnapshot(ctx, &sandboxstore.RestoreRootFSFromSnapshotRequest{
			SandboxID: req.SandboxID, SnapshotID: plan.snapshotID, TeamID: req.TeamID,
			OperationID: req.OperationID + "/initial-restore",
		})
		return err
	}
	_, _, err := s.store.EnsureInitialRootFSGeneration(ctx, &sandboxstore.EnsureInitialRootFSGenerationRequest{
		SandboxID: req.SandboxID, TeamID: req.TeamID, SourceOCIRef: plan.sourceRef,
		SourceOCIDigest: plan.sourceDigest, BaseArtifactDigest: plan.baseArtifactDigest,
	})
	return err
}

func (s *Service) claimRecord(tpl *templatepkg.Template, req *service.ClaimRequest, clusterID string, now time.Time) *sandboxstore.SandboxRecord {
	config := service.CloneSandboxConfig(req.Config)
	if config == nil {
		config = &sandboxstore.SandboxConfig{}
	}
	if config.TTL == nil && s.defaultTTL > 0 {
		seconds := int32(s.defaultTTL / time.Second)
		config.TTL = &seconds
	}
	record := &sandboxstore.SandboxRecord{
		ID: req.SandboxID, TeamID: req.TeamID, UserID: req.UserID,
		TemplateID: tpl.TemplateID, TemplateName: tpl.TemplateID, TemplateNamespace: tpl.Scope,
		ClusterID: clusterID, DesiredState: sandboxstore.SandboxDesiredStateActive,
		Config: *config, TemplateSpec: tpl.Spec, RuntimeGeneration: req.RuntimeGeneration,
		ClaimedAt: now, CreatedAt: now,
	}
	if config.TTL != nil && *config.TTL > 0 {
		record.ExpiresAt = now.Add(time.Duration(*config.TTL) * time.Second)
	}
	if !req.HardExpiresAt.IsZero() {
		record.HardExpiresAt = req.HardExpiresAt.UTC()
	} else if config.HardTTL != nil && *config.HardTTL > 0 {
		record.HardExpiresAt = now.Add(time.Duration(*config.HardTTL) * time.Second)
	}
	return record
}

func (s *Service) ensureClaimRecord(ctx context.Context, expected *sandboxstore.SandboxRecord) error {
	existing, err := s.store.GetSandbox(ctx, expected.ID)
	if err != nil {
		return fmt.Errorf("load retryable sandbox claim: %w", err)
	}
	if existing != nil {
		return validateClaimRecord(existing, expected)
	}
	limit, err := s.quotaLimits.GetLimit(ctx, expected.TeamID, quota.DimensionActiveSandboxes)
	if err != nil {
		return fmt.Errorf("load active sandbox quota: %w", err)
	}
	var activeLimit *int64
	if limit != nil {
		if limit.TeamID != expected.TeamID || limit.Dimension != quota.DimensionActiveSandboxes {
			return fmt.Errorf("active sandbox quota identity does not match claim")
		}
		activeLimit = &limit.LimitValue
	}
	existing, err = s.store.ReserveSandboxClaim(ctx, &sandboxstore.ReserveSandboxClaimRequest{
		Record: expected, ActiveSandboxLimit: activeLimit,
	})
	if errors.Is(err, sandboxstore.ErrActiveSandboxQuotaExceeded) {
		return fmt.Errorf("%w: %v", service.ErrQuotaExceeded, err)
	}
	if errors.Is(err, sandboxstore.ErrSandboxClaimReservationConflict) {
		return fmt.Errorf("%w: %v", service.ErrClaimConflict, err)
	}
	if err != nil {
		return fmt.Errorf("reserve sandbox claim: %w", err)
	}
	return validateClaimRecord(existing, expected)
}

func validateClaimRecord(existing, expected *sandboxstore.SandboxRecord) error {
	if !sameClaimRecord(existing, expected) {
		return fmt.Errorf("%w: operation sandbox identity is already bound to another claim", service.ErrClaimConflict)
	}
	return nil
}

func sameClaimRecord(actual, expected *sandboxstore.SandboxRecord) bool {
	return actual != nil && expected != nil && actual.DeletedAt.IsZero() &&
		actual.ID == expected.ID && actual.TeamID == expected.TeamID && actual.UserID == expected.UserID &&
		actual.TemplateID == expected.TemplateID && actual.ClusterID == expected.ClusterID &&
		actual.DesiredState == sandboxstore.SandboxDesiredStateActive &&
		actual.RuntimeGeneration == expected.RuntimeGeneration &&
		reflect.DeepEqual(actual.Config, expected.Config) && reflect.DeepEqual(actual.TemplateSpec, expected.TemplateSpec)
}

func runtimeAssignment(spec v1alpha1.SandboxTemplateSpec, req *service.ClaimRequest) runtimecontrol.Assignment {
	environment := make(map[string]string, len(spec.EnvVars)+len(spec.MainContainer.Env)+2)
	for _, item := range spec.MainContainer.Env {
		environment[item.Name] = item.Value
	}
	for key, value := range spec.EnvVars {
		environment[key] = value
	}
	if req.Config != nil {
		for key, value := range req.Config.EnvVars {
			environment[key] = value
		}
	}
	environment[runtimecontrol.EnvSandboxID] = req.SandboxID
	assignment := runtimecontrol.Assignment{
		SandboxID: req.SandboxID, TeamID: req.TeamID,
		RuntimeGeneration: req.RuntimeGeneration, EnvVars: environment,
	}
	if req.Config != nil && req.Config.Webhook != nil {
		webhook := *req.Config.Webhook
		assignment.Webhook = &webhook
	}
	return assignment
}

func digestPinnedImage(image string) (string, error) {
	if image == "" || image != strings.TrimSpace(image) {
		return "", fmt.Errorf("image must be non-empty and canonical")
	}
	named, err := distref.ParseNormalizedNamed(image)
	if err != nil {
		return "", err
	}
	digested, ok := named.(distref.Digested)
	if !ok {
		return "", fmt.Errorf("image must be pinned by OCI digest")
	}
	parsed, err := digest.Parse(digested.Digest().String())
	if err != nil || parsed.Algorithm() != digest.SHA256 || parsed.String() != digested.Digest().String() {
		return "", fmt.Errorf("image must use a canonical sha256 digest")
	}
	return parsed.String(), nil
}

func credentialBindings(policy *v1alpha1.SandboxNetworkPolicy) []v1alpha1.CredentialBinding {
	if policy == nil {
		return nil
	}
	return append([]v1alpha1.CredentialBinding(nil), policy.CredentialBindings...)
}

func requestCredentialBindings(config *sandboxstore.SandboxConfig) []v1alpha1.CredentialBinding {
	if config == nil {
		return nil
	}
	return credentialBindings(config.Network)
}

func appendWebhookPolicy(policy *v1alpha1.SandboxNetworkPolicy, rawURL string) *v1alpha1.SandboxNetworkPolicy {
	parsed, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil || parsed.Hostname() == "" {
		return policy
	}
	if policy == nil {
		policy = &v1alpha1.SandboxNetworkPolicy{}
	} else {
		policy = policy.DeepCopy()
	}
	if policy.Egress == nil {
		policy.Egress = &v1alpha1.NetworkEgressPolicy{}
	}
	host := parsed.Hostname()
	if ip := net.ParseIP(host); ip != nil {
		if ip.To4() != nil {
			policy.Egress.AllowedCIDRs = append(policy.Egress.AllowedCIDRs, ip.String()+"/32")
		} else {
			policy.Egress.AllowedCIDRs = append(policy.Egress.AllowedCIDRs, ip.String()+"/128")
		}
	} else {
		policy.Egress.AllowedDomains = append(policy.Egress.AllowedDomains, host)
	}
	return policy
}

var _ service.SandboxClaimer = (*Service)(nil)
