package namespacepolicy

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"strings"

	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"
)

const (
	defaultProcdPort       = 49983
	serviceAccountNSPath   = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"
	policyDenyIngressName  = "sandbox0-baseline-deny-sandbox-ingress"
	policyAllowSystemName  = "sandbox0-baseline-allow-system-to-sandbox"
	managedByLabelKey      = "app.kubernetes.io/managed-by"
	managedByLabelValue    = "sandbox0-manager"
	componentLabelKey      = "app.kubernetes.io/component"
	componentLabelValue    = "template-namespace-baseline"
	appNameLabelKey        = "app.kubernetes.io/name"
	metadataNamespaceLabel = "kubernetes.io/metadata.name"
	sandboxIDLabelKey      = "sandbox0.ai/sandbox-id"
)

// TemplateNamespaceReconciler ensures namespace-scoped ingress baseline policy.
type TemplateNamespaceReconciler interface {
	EnsureBaseline(ctx context.Context, namespace string) error
}

// Config configures template namespace baseline reconciliation.
type Config struct {
	SystemNamespace string
	ProcdPort       int
}

// Reconciler manages template namespace ingress baseline policies.
type Reconciler struct {
	client          kubernetes.Interface
	logger          *zap.Logger
	systemNamespace string
	procdPort       int
}

type systemIngressRule struct {
	component string
	procdOnly bool
}

// NewReconciler creates a manager-owned template namespace baseline reconciler.
func NewReconciler(client kubernetes.Interface, cfg Config, logger *zap.Logger) (*Reconciler, error) {
	if client == nil {
		return nil, fmt.Errorf("k8s client is required")
	}
	if logger == nil {
		logger = zap.NewNop()
	}

	systemNamespace, err := resolveSystemNamespace(cfg.SystemNamespace)
	if err != nil {
		return nil, err
	}

	procdPort := cfg.ProcdPort
	if procdPort <= 0 {
		procdPort = defaultProcdPort
	}

	return &Reconciler{
		client:          client,
		logger:          logger,
		systemNamespace: systemNamespace,
		procdPort:       procdPort,
	}, nil
}

// EnsureBaseline creates or updates the required ingress baseline for a template namespace.
func (r *Reconciler) EnsureBaseline(ctx context.Context, namespace string) error {
	if strings.TrimSpace(namespace) == "" {
		return fmt.Errorf("template namespace is required")
	}

	for _, desired := range r.desiredPolicies(namespace) {
		if err := r.ensurePolicy(ctx, desired); err != nil {
			return err
		}
	}

	return nil
}

func (r *Reconciler) ensurePolicy(ctx context.Context, desired *networkingv1.NetworkPolicy) error {
	existing, err := r.client.NetworkingV1().NetworkPolicies(desired.Namespace).Get(ctx, desired.Name, metav1.GetOptions{})
	if err != nil {
		if !apierrors.IsNotFound(err) {
			return fmt.Errorf("get networkpolicy %s/%s: %w", desired.Namespace, desired.Name, err)
		}
		if _, err := r.client.NetworkingV1().NetworkPolicies(desired.Namespace).Create(ctx, desired, metav1.CreateOptions{}); err != nil {
			return fmt.Errorf("create networkpolicy %s/%s: %w", desired.Namespace, desired.Name, err)
		}
		return nil
	}

	updated := existing.DeepCopy()
	updated.Labels = desired.Labels
	updated.Spec = desired.Spec
	if reflect.DeepEqual(existing.Labels, updated.Labels) && reflect.DeepEqual(existing.Spec, updated.Spec) {
		return nil
	}

	if _, err := r.client.NetworkingV1().NetworkPolicies(desired.Namespace).Update(ctx, updated, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("update networkpolicy %s/%s: %w", desired.Namespace, desired.Name, err)
	}
	return nil
}

func (r *Reconciler) desiredPolicies(namespace string) []*networkingv1.NetworkPolicy {
	policyTypeIngress := networkingv1.PolicyTypeIngress

	baseLabels := map[string]string{
		managedByLabelKey: managedByLabelValue,
		componentLabelKey: componentLabelValue,
	}

	return []*networkingv1.NetworkPolicy{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      policyDenyIngressName,
				Namespace: namespace,
				Labels:    baseLabels,
			},
			Spec: networkingv1.NetworkPolicySpec{
				PodSelector: sandboxPodSelector(),
				PolicyTypes: []networkingv1.PolicyType{policyTypeIngress},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      policyAllowSystemName,
				Namespace: namespace,
				Labels:    baseLabels,
			},
			Spec: networkingv1.NetworkPolicySpec{
				PodSelector: sandboxPodSelector(),
				PolicyTypes: []networkingv1.PolicyType{policyTypeIngress},
				Ingress:     r.desiredSystemIngressRules(),
			},
		},
	}
}

func (r *Reconciler) desiredSystemIngressRules() []networkingv1.NetworkPolicyIngressRule {
	tcp := corev1.ProtocolTCP
	procdPort := intstr.FromInt(r.procdPort)
	specs := []systemIngressRule{
		{component: internalauth.ServiceManager, procdOnly: true},
		{component: internalauth.ServiceSSHGateway, procdOnly: true},
		{component: internalauth.ServiceClusterGateway},
	}

	rules := make([]networkingv1.NetworkPolicyIngressRule, 0, len(specs))
	for _, spec := range specs {
		rule := networkingv1.NetworkPolicyIngressRule{
			From: []networkingv1.NetworkPolicyPeer{r.systemPeer(spec.component)},
		}
		if spec.procdOnly {
			rule.Ports = []networkingv1.NetworkPolicyPort{{
				Protocol: &tcp,
				Port:     &procdPort,
			}}
		}
		rules = append(rules, rule)
	}

	return rules
}

func (r *Reconciler) systemPeer(component string) networkingv1.NetworkPolicyPeer {
	return networkingv1.NetworkPolicyPeer{
		NamespaceSelector: &metav1.LabelSelector{
			MatchLabels: map[string]string{metadataNamespaceLabel: r.systemNamespace},
		},
		PodSelector: &metav1.LabelSelector{
			MatchLabels: map[string]string{appNameLabelKey: component},
		},
	}
}

func sandboxPodSelector() metav1.LabelSelector {
	return metav1.LabelSelector{
		MatchExpressions: []metav1.LabelSelectorRequirement{{
			Key:      sandboxIDLabelKey,
			Operator: metav1.LabelSelectorOpExists,
		}},
	}
}

func resolveSystemNamespace(explicit string) (string, error) {
	if namespace := strings.TrimSpace(explicit); namespace != "" {
		return namespace, nil
	}
	if namespace := strings.TrimSpace(os.Getenv("POD_NAMESPACE")); namespace != "" {
		return namespace, nil
	}

	data, err := os.ReadFile(serviceAccountNSPath)
	if err == nil {
		if namespace := strings.TrimSpace(string(data)); namespace != "" {
			return namespace, nil
		}
	}
	if err != nil && !os.IsNotExist(err) {
		return "", fmt.Errorf("read service account namespace: %w", err)
	}

	return "", fmt.Errorf("resolve template namespace baseline system namespace")
}
