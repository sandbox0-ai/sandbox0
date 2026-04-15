package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/network"
	egressauth "github.com/sandbox0-ai/sandbox0/pkg/egressauth"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
)

type webhookInfo struct {
	URL      string
	Secret   string
	WatchDir string
}

func (s *SandboxService) getWebhookInfo(req *ClaimRequest) *webhookInfo {
	if req == nil || req.Config == nil || req.Config.Webhook == nil {
		return nil
	}
	urlValue := strings.TrimSpace(req.Config.Webhook.URL)
	if urlValue == "" {
		return nil
	}
	return &webhookInfo{
		URL:      urlValue,
		Secret:   strings.TrimSpace(req.Config.Webhook.Secret),
		WatchDir: strings.TrimSpace(req.Config.Webhook.WatchDir),
	}
}

func (s *SandboxService) appendWebhookNetworkPolicy(
	requestNetwork *v1alpha1.SandboxNetworkPolicy,
	webhookURL string,
) *v1alpha1.SandboxNetworkPolicy {
	if webhookURL == "" {
		return requestNetwork
	}
	parsed, err := url.Parse(webhookURL)
	if err != nil {
		s.logger.Warn("Failed to parse webhook URL",
			zap.String("webhook_url", webhookURL),
			zap.Error(err),
		)
		return requestNetwork
	}
	host := parsed.Hostname()
	if host == "" {
		s.logger.Warn("Webhook URL missing hostname",
			zap.String("webhook_url", webhookURL),
		)
		return requestNetwork
	}
	if requestNetwork == nil {
		requestNetwork = &v1alpha1.SandboxNetworkPolicy{}
	}
	if requestNetwork.Egress == nil {
		requestNetwork.Egress = &v1alpha1.NetworkEgressPolicy{}
	}
	if ip := net.ParseIP(host); ip != nil {
		requestNetwork.Egress.AllowedCIDRs = append(requestNetwork.Egress.AllowedCIDRs, formatCIDRForIP(ip))
		return requestNetwork
	}
	requestNetwork.Egress.AllowedDomains = append(requestNetwork.Egress.AllowedDomains, host)
	return requestNetwork
}

func formatCIDRForIP(ip net.IP) string {
	if ip.To4() != nil {
		return ip.String() + "/32"
	}
	return ip.String() + "/128"
}

func (s *SandboxService) applyPoliciesForPod(
	ctx context.Context,
	pod *corev1.Pod,
	template *v1alpha1.SandboxTemplate,
	req *ClaimRequest,
) (*BuildNetworkPolicyResult, error) {
	if s.NetworkPolicyService == nil || pod == nil || template == nil || req == nil {
		return nil, nil
	}

	var requestNetwork *v1alpha1.SandboxNetworkPolicy
	if req.Config != nil {
		requestNetwork = req.Config.Network
	}
	webhookInfo := s.getWebhookInfo(req)
	if webhookInfo != nil {
		requestNetwork = s.appendWebhookNetworkPolicy(requestNetwork, webhookInfo.URL)
	}

	networkState := s.NetworkPolicyService.BuildNetworkPolicyState(&BuildNetworkPolicyRequest{
		SandboxID:        pod.Name,
		TeamID:           req.TeamID,
		TemplateSpec:     template.Spec.Network,
		RequestSpec:      requestNetwork,
		TemplateBindings: templateCredentialBindings(template.Spec.Network),
		RequestBindings:  requestCredentialBindings(req.Config),
	})
	if networkState != nil && networkState.PolicySpec != nil {
		if _, err := s.setNetworkPolicyAnnotations(pod, networkState.PolicySpec); err != nil {
			return nil, err
		}
	}

	return networkState, nil
}

func (s *SandboxService) setNetworkPolicyAnnotations(pod *corev1.Pod, spec *v1alpha1.NetworkPolicySpec) (string, error) {
	if pod.Annotations == nil {
		pod.Annotations = make(map[string]string)
	}
	annotation, err := v1alpha1.NetworkPolicyToAnnotation(spec)
	if err != nil {
		return "", fmt.Errorf("serialize network policy: %w", err)
	}
	pod.Annotations[controller.AnnotationNetworkPolicy] = annotation
	newHash := policyAnnotationHash(annotation)
	oldHash := pod.Annotations[controller.AnnotationNetworkPolicyHash]
	if newHash != "" {
		pod.Annotations[controller.AnnotationNetworkPolicyHash] = newHash
	} else {
		delete(pod.Annotations, controller.AnnotationNetworkPolicyHash)
	}
	if oldHash != newHash {
		delete(pod.Annotations, controller.AnnotationNetworkPolicyAppliedHash)
	}
	return newHash, nil
}

func policySpecFromState(state *BuildNetworkPolicyResult) *v1alpha1.NetworkPolicySpec {
	if state == nil {
		return nil
	}
	return state.PolicySpec
}

func noopCredentialBindingRollback(context.Context) error {
	return nil
}

func requestCredentialBindings(cfg *SandboxConfig) []v1alpha1.CredentialBinding {
	if cfg == nil || cfg.Network == nil || cfg.Network.CredentialBindings == nil {
		return nil
	}
	return append([]v1alpha1.CredentialBinding(nil), cfg.Network.CredentialBindings...)
}

func templateCredentialBindings(policy *v1alpha1.SandboxNetworkPolicy) []v1alpha1.CredentialBinding {
	if policy == nil || policy.CredentialBindings == nil {
		return nil
	}
	return append([]v1alpha1.CredentialBinding(nil), policy.CredentialBindings...)
}

func (s *SandboxService) syncCredentialBindings(
	ctx context.Context,
	pod *corev1.Pod,
	teamID string,
	state *BuildNetworkPolicyResult,
) (func(context.Context) error, error) {
	if s.credentialStore == nil || pod == nil || state == nil {
		return noopCredentialBindingRollback, nil
	}

	previous, err := s.credentialStore.GetBindings(ctx, teamID, pod.Name)
	if err != nil {
		return nil, err
	}
	previous = cloneBindingRecord(previous)

	rollback := func(rollbackCtx context.Context) error {
		if previous == nil || len(previous.Bindings) == 0 {
			return s.credentialStore.DeleteBindings(rollbackCtx, teamID, pod.Name)
		}
		return s.credentialStore.UpsertBindings(rollbackCtx, previous)
	}

	if len(state.CredentialBindings) == 0 {
		if previous == nil || len(previous.Bindings) == 0 {
			return rollback, nil
		}
		if err := s.credentialStore.DeleteBindings(ctx, teamID, pod.Name); err != nil {
			return nil, err
		}
		return rollback, nil
	}

	storeBindings, err := toStoreCredentialBindings(ctx, s.credentialStore, teamID, state.CredentialBindings)
	if err != nil {
		return nil, err
	}

	if err := s.credentialStore.UpsertBindings(ctx, &egressauth.BindingRecord{
		SandboxID: pod.Name,
		TeamID:    teamID,
		Bindings:  storeBindings,
	}); err != nil {
		return nil, err
	}
	return rollback, nil
}

func cloneBindingRecord(record *egressauth.BindingRecord) *egressauth.BindingRecord {
	if record == nil {
		return nil
	}
	cloned := *record
	cloned.Bindings = cloneStoreCredentialBindings(record.Bindings)
	return &cloned
}

func (s *SandboxService) loadCredentialBindings(ctx context.Context, pod *corev1.Pod) ([]v1alpha1.CredentialBinding, error) {
	if s.credentialStore == nil || pod == nil {
		return nil, nil
	}
	record, err := s.credentialStore.GetBindings(ctx, sandboxTeamID(pod), pod.Name)
	if err != nil {
		return nil, err
	}
	if record == nil || len(record.Bindings) == 0 {
		return nil, nil
	}
	return fromStoreCredentialBindings(record.Bindings), nil
}

func sandboxTeamID(pod *corev1.Pod) string {
	if pod != nil && pod.Annotations != nil {
		if teamID := pod.Annotations[controller.AnnotationTeamID]; teamID != "" {
			return teamID
		}
	}
	return ""
}

func toStoreCredentialBindings(
	ctx context.Context,
	store egressauth.BindingStore,
	teamID string,
	in []v1alpha1.CredentialBinding,
) ([]egressauth.CredentialBinding, error) {
	if len(in) == 0 {
		return nil, nil
	}
	out := make([]egressauth.CredentialBinding, 0, len(in))
	for _, binding := range in {
		source, err := store.GetSourceByRef(ctx, teamID, binding.SourceRef)
		if err != nil {
			return nil, fmt.Errorf("resolve credential source %q: %w", binding.SourceRef, err)
		}
		if source == nil {
			return nil, fmt.Errorf("credential source %q not found", binding.SourceRef)
		}
		storeBinding := egressauth.CredentialBinding{
			Ref:           binding.Ref,
			SourceRef:     binding.SourceRef,
			SourceID:      source.ID,
			SourceVersion: source.CurrentVersion,
			Projection:    toStoreProjection(binding.Projection),
			CachePolicy:   toStoreCachePolicy(binding.CachePolicy),
		}
		out = append(out, storeBinding)
	}
	return out, nil
}

func cloneStoreCredentialBindings(in []egressauth.CredentialBinding) []egressauth.CredentialBinding {
	if len(in) == 0 {
		return nil
	}
	out := make([]egressauth.CredentialBinding, 0, len(in))
	for _, binding := range in {
		cloned := egressauth.CredentialBinding{
			Ref:           binding.Ref,
			SourceRef:     binding.SourceRef,
			SourceID:      binding.SourceID,
			SourceVersion: binding.SourceVersion,
			Projection:    cloneStoreProjection(binding.Projection),
			CachePolicy:   cloneStoreCachePolicy(binding.CachePolicy),
		}
		out = append(out, cloned)
	}
	return out
}

func fromStoreCredentialBindings(in []egressauth.CredentialBinding) []v1alpha1.CredentialBinding {
	if len(in) == 0 {
		return nil
	}
	out := make([]v1alpha1.CredentialBinding, 0, len(in))
	for _, binding := range in {
		policyBinding := v1alpha1.CredentialBinding{
			Ref:         binding.Ref,
			SourceRef:   binding.SourceRef,
			Projection:  fromStoreProjection(binding.Projection),
			CachePolicy: fromStoreCachePolicy(binding.CachePolicy),
		}
		out = append(out, policyBinding)
	}
	return out
}

func toStoreProjection(in v1alpha1.ProjectionSpec) egressauth.ProjectionSpec {
	out := egressauth.ProjectionSpec{
		Type: egressauth.CredentialProjectionType(in.Type),
	}
	if in.HTTPHeaders != nil {
		out.HTTPHeaders = &egressauth.HTTPHeadersProjection{
			Headers: make([]egressauth.ProjectedHeader, 0, len(in.HTTPHeaders.Headers)),
		}
		for _, header := range in.HTTPHeaders.Headers {
			out.HTTPHeaders.Headers = append(out.HTTPHeaders.Headers, egressauth.ProjectedHeader{
				Name:          header.Name,
				ValueTemplate: header.ValueTemplate,
			})
		}
	}
	if in.TLSClientCertificate != nil {
		out.TLSClientCertificate = &egressauth.TLSClientCertificateProjection{}
	}
	if in.UsernamePassword != nil {
		out.UsernamePassword = &egressauth.UsernamePasswordProjection{}
	}
	return out
}

func cloneStoreProjection(in egressauth.ProjectionSpec) egressauth.ProjectionSpec {
	out := egressauth.ProjectionSpec{
		Type: in.Type,
	}
	if in.HTTPHeaders != nil {
		out.HTTPHeaders = &egressauth.HTTPHeadersProjection{
			Headers: make([]egressauth.ProjectedHeader, 0, len(in.HTTPHeaders.Headers)),
		}
		out.HTTPHeaders.Headers = append(out.HTTPHeaders.Headers, in.HTTPHeaders.Headers...)
	}
	if in.TLSClientCertificate != nil {
		out.TLSClientCertificate = &egressauth.TLSClientCertificateProjection{}
	}
	if in.UsernamePassword != nil {
		out.UsernamePassword = &egressauth.UsernamePasswordProjection{}
	}
	return out
}

func fromStoreProjection(in egressauth.ProjectionSpec) v1alpha1.ProjectionSpec {
	out := v1alpha1.ProjectionSpec{
		Type: v1alpha1.CredentialProjectionType(in.Type),
	}
	if in.HTTPHeaders != nil {
		out.HTTPHeaders = &v1alpha1.HTTPHeadersProjection{
			Headers: make([]v1alpha1.ProjectedHeader, 0, len(in.HTTPHeaders.Headers)),
		}
		for _, header := range in.HTTPHeaders.Headers {
			out.HTTPHeaders.Headers = append(out.HTTPHeaders.Headers, v1alpha1.ProjectedHeader{
				Name:          header.Name,
				ValueTemplate: header.ValueTemplate,
			})
		}
	}
	if in.TLSClientCertificate != nil {
		out.TLSClientCertificate = &v1alpha1.TLSClientCertificateProjection{}
	}
	if in.UsernamePassword != nil {
		out.UsernamePassword = &v1alpha1.UsernamePasswordProjection{}
	}
	return out
}

func toStoreCachePolicy(in *v1alpha1.CachePolicySpec) *egressauth.CachePolicySpec {
	if in == nil {
		return nil
	}
	return &egressauth.CachePolicySpec{TTL: in.TTL}
}

func cloneStoreCachePolicy(in *egressauth.CachePolicySpec) *egressauth.CachePolicySpec {
	if in == nil {
		return nil
	}
	return &egressauth.CachePolicySpec{TTL: in.TTL}
}

func fromStoreCachePolicy(in *egressauth.CachePolicySpec) *v1alpha1.CachePolicySpec {
	if in == nil {
		return nil
	}
	return &v1alpha1.CachePolicySpec{TTL: in.TTL}
}

func sanitizedNetworkPolicyForPersistence(policy *v1alpha1.SandboxNetworkPolicy) *v1alpha1.SandboxNetworkPolicy {
	if policy == nil {
		return nil
	}
	cloned := policy.DeepCopy()
	cloned.CredentialBindings = nil
	return cloned
}

func (s *SandboxService) applyNetworkProvider(
	ctx context.Context,
	pod *corev1.Pod,
	teamID string,
	networkSpec *v1alpha1.NetworkPolicySpec,
) error {
	if s.networkProvider == nil || pod == nil || networkSpec == nil {
		return nil
	}

	input := network.SandboxPolicyInput{
		SandboxID:     pod.Name,
		Namespace:     pod.Namespace,
		PodName:       pod.Name,
		TeamID:        teamID,
		PodLabels:     pod.Labels,
		NetworkPolicy: networkSpec,
	}
	if err := s.networkProvider.ApplySandboxPolicy(ctx, input); err != nil {
		if errors.Is(err, network.ErrPolicyApplyTimeout) {
			return fmt.Errorf("%w: %v", ErrDataPlaneNotReady, err)
		}
		return err
	}
	return nil
}

// GetNetworkPolicy gets the effective sandbox network policy.
func (s *SandboxService) GetNetworkPolicy(ctx context.Context, sandboxID string) (*v1alpha1.SandboxNetworkPolicy, error) {
	pod, err := s.getSandboxPod(ctx, sandboxID)
	if err != nil {
		return nil, fmt.Errorf("get pod: %w", err)
	}

	annotation := ""
	if pod.Annotations != nil {
		annotation = pod.Annotations[controller.AnnotationNetworkPolicy]
	}
	spec, err := v1alpha1.ParseNetworkPolicyFromAnnotation(annotation)
	if err != nil {
		return nil, fmt.Errorf("parse network policy annotation: %w", err)
	}
	if spec != nil {
		bindings, err := s.loadCredentialBindings(ctx, pod)
		if err != nil {
			return nil, fmt.Errorf("load credential bindings: %w", err)
		}
		return sandboxNetworkPolicyFromParts(spec, bindings), nil
	}

	templateSpec, templateBindings := s.templateNetworkDefaults(pod)
	if templateSpec != nil || len(templateBindings) > 0 {
		return sandboxNetworkPolicyWithBindings(templateSpec, templateBindings), nil
	}

	return &v1alpha1.SandboxNetworkPolicy{Mode: v1alpha1.NetworkModeAllowAll}, nil
}

// UpdateNetworkPolicy updates the sandbox network policy.
func (s *SandboxService) UpdateNetworkPolicy(
	ctx context.Context,
	sandboxID string,
	policy *v1alpha1.SandboxNetworkPolicy,
) (*v1alpha1.SandboxNetworkPolicy, error) {
	if policy == nil {
		return nil, fmt.Errorf("network policy is required")
	}
	if s.NetworkPolicyService == nil {
		return nil, fmt.Errorf("network policy service not configured")
	}

	pod, err := s.getSandboxPod(ctx, sandboxID)
	if err != nil {
		return nil, fmt.Errorf("get pod: %w", err)
	}

	var networkState *BuildNetworkPolicyResult
	var updatedPod *corev1.Pod
	var rollbackBindings func(context.Context) error

	err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
		// Get the latest version of the pod
		current, err := s.getSandboxPod(ctx, sandboxID)
		if err != nil {
			return err
		}

		teamID := ""
		if current.Annotations != nil {
			teamID = current.Annotations[controller.AnnotationTeamID]
		}
		templateSpec, templateBindings := s.templateNetworkDefaults(current)
		requestBindings := append([]v1alpha1.CredentialBinding(nil), policy.CredentialBindings...)
		if policy.CredentialBindings == nil {
			requestBindings, err = s.loadCredentialBindings(ctx, current)
			if err != nil {
				return fmt.Errorf("load credential bindings: %w", err)
			}
		}

		networkState = s.NetworkPolicyService.BuildNetworkPolicyState(&BuildNetworkPolicyRequest{
			SandboxID:        current.Name,
			TeamID:           teamID,
			TemplateSpec:     templateSpec,
			RequestSpec:      policy,
			TemplateBindings: templateBindings,
			RequestBindings:  requestBindings,
		})
		rollbackBindings, err = s.syncCredentialBindings(ctx, current, teamID, networkState)
		if err != nil {
			return fmt.Errorf("stage credential bindings: %w", err)
		}

		updatedPod = current.DeepCopy()
		if updatedPod.Annotations == nil {
			updatedPod.Annotations = make(map[string]string)
		}
		if _, err := s.setNetworkPolicyAnnotations(updatedPod, policySpecFromState(networkState)); err != nil {
			return err
		}

		if configJSON := updatedPod.Annotations[controller.AnnotationConfig]; configJSON != "" {
			var storedConfig SandboxConfig
			if err := json.Unmarshal([]byte(configJSON), &storedConfig); err != nil {
				s.logger.Warn("Failed to parse sandbox config annotation",
					zap.String("sandboxID", sandboxID),
					zap.Error(err),
				)
			} else {
				storedConfig.Network = sanitizedNetworkPolicyForPersistence(policy)
				updatedConfigJSON, err := json.Marshal(storedConfig)
				if err != nil {
					return fmt.Errorf("marshal sandbox config: %w", err)
				}
				updatedPod.Annotations[controller.AnnotationConfig] = string(updatedConfigJSON)
			}
		} else {
			storedConfig := SandboxConfig{Network: sanitizedNetworkPolicyForPersistence(policy)}
			updatedConfigJSON, err := json.Marshal(storedConfig)
			if err != nil {
				return fmt.Errorf("marshal sandbox config: %w", err)
			}
			updatedPod.Annotations[controller.AnnotationConfig] = string(updatedConfigJSON)
		}

		updatedPod, err = s.k8sClient.CoreV1().Pods(pod.Namespace).Update(ctx, updatedPod, metav1.UpdateOptions{})
		if err != nil && rollbackBindings != nil {
			if rollbackErr := rollbackBindings(ctx); rollbackErr != nil {
				s.logger.Warn("Failed to roll back credential bindings after network policy update failure",
					zap.String("sandboxID", sandboxID),
					zap.Error(rollbackErr),
				)
			}
		}
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("update pod annotations: %w", err)
	}

	teamID := ""
	if updatedPod.Annotations != nil {
		teamID = updatedPod.Annotations[controller.AnnotationTeamID]
	}
	if err := s.applyNetworkProvider(ctx, updatedPod, teamID, policySpecFromState(networkState)); err != nil {
		return nil, fmt.Errorf("apply network policy: %w", err)
	}

	return sandboxNetworkPolicyFromState(networkState), nil
}

func (s *SandboxService) templateNetworkDefaults(pod *corev1.Pod) (*v1alpha1.SandboxNetworkPolicy, []v1alpha1.CredentialBinding) {
	template := s.templateForPod(pod)
	if template == nil {
		return nil, nil
	}
	return template.Spec.Network, templateCredentialBindings(template.Spec.Network)
}

func (s *SandboxService) templateForPod(pod *corev1.Pod) *v1alpha1.SandboxTemplate {
	if pod == nil || s.templateLister == nil {
		return nil
	}
	templateID := pod.Labels[controller.LabelTemplateID]
	if templateID == "" {
		return nil
	}
	teamID := pod.Annotations[controller.AnnotationTeamID]
	if teamID != "" {
		namespace, err := naming.TemplateNamespaceForTeam(teamID)
		if err == nil {
			template, getErr := s.templateLister.Get(namespace, templateID)
			if getErr == nil {
				return template
			}
		}
	}

	namespace, err := naming.TemplateNamespaceForBuiltin(templateID)
	if err != nil {
		s.logger.Warn("Failed to resolve template namespace",
			zap.String("templateID", templateID),
			zap.Error(err),
		)
		return nil
	}
	template, err := s.templateLister.Get(namespace, templateID)
	if err != nil {
		s.logger.Warn("Failed to get template for network policy",
			zap.String("templateID", templateID),
			zap.String("namespace", namespace),
			zap.Error(err),
		)
		return nil
	}
	return template
}

func networkPolicyFromSpec(spec *v1alpha1.NetworkPolicySpec) *v1alpha1.SandboxNetworkPolicy {
	if spec == nil {
		return &v1alpha1.SandboxNetworkPolicy{Mode: v1alpha1.NetworkModeAllowAll}
	}

	var (
		egressAllowedCIDRs    []string
		egressDeniedCIDRs     []string
		egressAllowedDomains  []string
		egressDeniedDomains   []string
		egressAllowedPorts    []v1alpha1.PortSpec
		egressDeniedPorts     []v1alpha1.PortSpec
		egressTrafficRules    []v1alpha1.TrafficRule
		egressCredentialRules []v1alpha1.EgressCredentialRule
	)
	if spec.Egress != nil {
		egressAllowedCIDRs = append(egressAllowedCIDRs, spec.Egress.AllowedCIDRs...)
		egressDeniedCIDRs = append(egressDeniedCIDRs, spec.Egress.DeniedCIDRs...)
		egressAllowedDomains = append(egressAllowedDomains, spec.Egress.AllowedDomains...)
		egressDeniedDomains = append(egressDeniedDomains, spec.Egress.DeniedDomains...)
		egressAllowedPorts = append(egressAllowedPorts, spec.Egress.AllowedPorts...)
		egressDeniedPorts = append(egressDeniedPorts, spec.Egress.DeniedPorts...)
		egressTrafficRules = append(egressTrafficRules, spec.Egress.TrafficRules...)
		egressCredentialRules = append(egressCredentialRules, spec.Egress.CredentialRules...)
	}

	mode := v1alpha1.NetworkModeAllowAll
	if spec.Mode != "" {
		mode = spec.Mode
	}

	policy := &v1alpha1.SandboxNetworkPolicy{
		Mode: mode,
	}
	if len(egressAllowedCIDRs)+len(egressDeniedCIDRs)+len(egressAllowedDomains)+len(egressDeniedDomains)+len(egressAllowedPorts)+len(egressDeniedPorts)+len(egressTrafficRules)+len(egressCredentialRules) > 0 {
		policy.Egress = &v1alpha1.NetworkEgressPolicy{
			AllowedCIDRs:    egressAllowedCIDRs,
			DeniedCIDRs:     egressDeniedCIDRs,
			AllowedDomains:  egressAllowedDomains,
			DeniedDomains:   egressDeniedDomains,
			AllowedPorts:    egressAllowedPorts,
			DeniedPorts:     egressDeniedPorts,
			TrafficRules:    egressTrafficRules,
			CredentialRules: egressCredentialRules,
		}
	}

	return policy
}

func sandboxNetworkPolicyWithBindings(policy *v1alpha1.SandboxNetworkPolicy, bindings []v1alpha1.CredentialBinding) *v1alpha1.SandboxNetworkPolicy {
	result := &v1alpha1.SandboxNetworkPolicy{Mode: v1alpha1.NetworkModeAllowAll}
	if policy != nil {
		result.Mode = policy.Mode
		result.Egress = policy.Egress.DeepCopy()
	}
	if len(bindings) > 0 {
		result.CredentialBindings = append(result.CredentialBindings, bindings...)
	}
	return result
}

func sandboxNetworkPolicyFromParts(spec *v1alpha1.NetworkPolicySpec, bindings []v1alpha1.CredentialBinding) *v1alpha1.SandboxNetworkPolicy {
	return sandboxNetworkPolicyWithBindings(networkPolicyFromSpec(spec), bindings)
}

func sandboxNetworkPolicyFromState(state *BuildNetworkPolicyResult) *v1alpha1.SandboxNetworkPolicy {
	if state == nil {
		return &v1alpha1.SandboxNetworkPolicy{Mode: v1alpha1.NetworkModeAllowAll}
	}
	return sandboxNetworkPolicyFromParts(state.PolicySpec, state.CredentialBindings)
}
