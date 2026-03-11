package framework

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	infrav1alpha1 "github.com/sandbox0-ai/sandbox0/infra-operator/api/v1alpha1"
	yamlv3 "gopkg.in/yaml.v3"
	"sigs.k8s.io/yaml"
)

// Scenario describes a test environment driven by a sample manifest.
type Scenario struct {
	Name           string
	ManifestPath   string
	InfraName      string
	InfraNamespace string
	Secrets        []SecretSpec
	Rollouts       []RolloutTarget
	ReadyTimeout   string
}

// RolloutTarget represents a workload to wait on.
type RolloutTarget struct {
	Kind      string
	Name      string
	Namespace string
	Timeout   string
}

// ResourceID returns the kubernetes resource reference for rollout checks.
func (r RolloutTarget) ResourceID() (string, error) {
	if r.Kind == "" || r.Name == "" {
		return "", fmt.Errorf("rollout kind and name are required")
	}
	return fmt.Sprintf("%s/%s", r.Kind, r.Name), nil
}

// ScenarioEnv binds runtime config to a scenario.
type ScenarioEnv struct {
	Config   Config
	Scenario Scenario
	TestCtx  *TestContext
	Infra    InfraConfig
}

// DiscoverSampleManifests returns sorted sample manifest paths.
func DiscoverSampleManifests(cfg Config, clusterType string) ([]string, error) {
	if clusterType == "" {
		return nil, fmt.Errorf("cluster type is required")
	}
	if cfg.OperatorChartPath == "" {
		return nil, fmt.Errorf("operator chart path is required")
	}

	root := filepath.Join(cfg.OperatorChartPath, "samples", clusterType)
	entries, err := os.ReadDir(root)
	if err != nil {
		return nil, fmt.Errorf("read samples dir %q: %w", root, err)
	}

	var manifests []string
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		ext := strings.ToLower(filepath.Ext(name))
		if ext != ".yaml" && ext != ".yml" {
			continue
		}
		manifests = append(manifests, filepath.Join(root, name))
	}

	sort.Strings(manifests)
	if len(manifests) == 0 {
		return nil, fmt.Errorf("no sample manifests found in %q", root)
	}
	return manifests, nil
}

// BuildScenarioFromManifest builds a Scenario from a Sandbox0Infra manifest.
func BuildScenarioFromManifest(cfg Config, manifestPath string) (Scenario, error) {
	if manifestPath == "" {
		return Scenario{}, fmt.Errorf("manifest path is required")
	}
	content, err := os.ReadFile(manifestPath)
	if err != nil {
		return Scenario{}, fmt.Errorf("read manifest %q: %w", manifestPath, err)
	}

	infra, err := extractSandbox0Infra(content)
	if err != nil {
		return Scenario{}, err
	}

	infraName := strings.TrimSpace(infra.Name)
	if infraName == "" {
		return Scenario{}, fmt.Errorf("manifest %q missing Sandbox0Infra metadata.name", manifestPath)
	}
	infraNamespace := strings.TrimSpace(infra.Namespace)
	if infraNamespace == "" {
		infraNamespace = cfg.InfraNamespace
	}

	scenario := Scenario{
		Name:           scenarioNameFromPath(manifestPath),
		ManifestPath:   manifestPath,
		InfraName:      infraName,
		InfraNamespace: infraNamespace,
		Secrets:        buildScenarioSecrets(infra, infraNamespace),
		Rollouts:       buildScenarioRollouts(infra, infraName, infraNamespace),
	}
	return scenario, nil
}

func extractSandbox0Infra(content []byte) (*infrav1alpha1.Sandbox0Infra, error) {
	decoder := yamlv3.NewDecoder(bytes.NewReader(content))
	for {
		var doc map[string]any
		if err := decoder.Decode(&doc); err != nil {
			if err == io.EOF {
				break
			}
			return nil, fmt.Errorf("decode manifest: %w", err)
		}
		if len(doc) == 0 {
			continue
		}
		kind, _ := doc["kind"].(string)
		if !strings.EqualFold(strings.TrimSpace(kind), "Sandbox0Infra") {
			continue
		}
		raw, err := yaml.Marshal(doc)
		if err != nil {
			return nil, fmt.Errorf("marshal Sandbox0Infra: %w", err)
		}
		var infra infrav1alpha1.Sandbox0Infra
		if err := yaml.Unmarshal(raw, &infra); err != nil {
			return nil, fmt.Errorf("unmarshal Sandbox0Infra: %w", err)
		}
		return &infra, nil
	}
	return nil, fmt.Errorf("Sandbox0Infra not found in manifest")
}

func scenarioNameFromPath(path string) string {
	base := strings.TrimSuffix(filepath.Base(path), filepath.Ext(path))
	parent := filepath.Base(filepath.Dir(path))
	if parent == "" || parent == "." || parent == "/" {
		return base
	}
	return filepath.Join(parent, base)
}

func buildScenarioRollouts(infra *infrav1alpha1.Sandbox0Infra, infraName, namespace string) []RolloutTarget {
	var rollouts []RolloutTarget
	if infrav1alpha1.IsGlobalDirectoryEnabled(infra) {
		rollouts = append(rollouts, RolloutTarget{Kind: "deployment", Name: infraName + "-global-directory", Namespace: namespace, Timeout: "5m"})
	}
	if infrav1alpha1.IsEdgeGatewayEnabled(infra) {
		rollouts = append(rollouts, RolloutTarget{Kind: "deployment", Name: infraName + "-edge-gateway", Namespace: namespace, Timeout: "5m"})
	}
	if infrav1alpha1.IsSchedulerEnabled(infra) {
		rollouts = append(rollouts, RolloutTarget{Kind: "deployment", Name: infraName + "-scheduler", Namespace: namespace, Timeout: "5m"})
	}
	if infrav1alpha1.IsInternalGatewayEnabled(infra) {
		rollouts = append(rollouts, RolloutTarget{Kind: "deployment", Name: infraName + "-internal-gateway", Namespace: namespace, Timeout: "5m"})
	}
	if infrav1alpha1.IsManagerEnabled(infra) {
		rollouts = append(rollouts, RolloutTarget{Kind: "deployment", Name: infraName + "-manager", Namespace: namespace, Timeout: "5m"})
	}
	if infrav1alpha1.IsStorageProxyEnabled(infra) {
		rollouts = append(rollouts, RolloutTarget{Kind: "deployment", Name: infraName + "-storage-proxy", Namespace: namespace, Timeout: "5m"})
	}
	return rollouts
}

func buildScenarioSecrets(infra *infrav1alpha1.Sandbox0Infra, namespace string) []SecretSpec {
	var secrets []SecretSpec
	addSecret := func(spec SecretSpec) {
		if spec.Name == "" || spec.Namespace == "" || len(spec.StringData) == 0 {
			return
		}
		secrets = append(secrets, spec)
	}

	if infra == nil {
		return secrets
	}
	if infra.Spec.InitUser != nil {
		addSecret(secretFromKeyRef(namespace, infra.Spec.InitUser.PasswordSecret, "password"))
	}
	if infra.Spec.Database != nil && infra.Spec.Database.Type == infrav1alpha1.DatabaseTypeExternal && infra.Spec.Database.External != nil {
		addSecret(secretFromKeyRef(namespace, infra.Spec.Database.External.PasswordSecret, "password"))
	}
	if infra.Spec.Storage != nil {
		switch infra.Spec.Storage.Type {
		case infrav1alpha1.StorageTypeS3:
			if infra.Spec.Storage.S3 != nil {
				addSecret(credentialsSecretFromS3(namespace, infra.Spec.Storage.S3.CredentialsSecret))
			}
		case infrav1alpha1.StorageTypeOSS:
			if infra.Spec.Storage.OSS != nil {
				addSecret(credentialsSecretFromOSS(namespace, infra.Spec.Storage.OSS.CredentialsSecret))
			}
		}
	}
	if infra.Spec.ControlPlane != nil {
		addSecret(secretFromKeyRef(namespace, infra.Spec.ControlPlane.InternalAuthPublicKeySecret, "public.key"))
	}
	if infra.Spec.InternalAuth != nil {
		if infra.Spec.InternalAuth.ControlPlane != nil && infra.Spec.InternalAuth.ControlPlane.SecretRef != nil {
			addSecret(keyPairSecretFromRef(namespace, infra.Spec.InternalAuth.ControlPlane.SecretRef))
		}
		if infra.Spec.InternalAuth.DataPlane != nil && infra.Spec.InternalAuth.DataPlane.SecretRef != nil {
			addSecret(keyPairSecretFromRef(namespace, infra.Spec.InternalAuth.DataPlane.SecretRef))
		}
	}

	return secrets
}

func secretFromKeyRef(namespace string, ref infrav1alpha1.SecretKeyRef, defaultKey string) SecretSpec {
	key := strings.TrimSpace(ref.Key)
	if key == "" {
		key = defaultKey
	}
	if ref.Name == "" || key == "" {
		return SecretSpec{}
	}
	return SecretSpec{
		Name:      ref.Name,
		Namespace: namespace,
		StringData: map[string]string{
			key: randomHex(16),
		},
	}
}

func credentialsSecretFromS3(namespace string, ref infrav1alpha1.S3CredentialsSecret) SecretSpec {
	accessKey := strings.TrimSpace(ref.AccessKeyKey)
	if accessKey == "" {
		accessKey = "accessKeyId"
	}
	secretKey := strings.TrimSpace(ref.SecretKeyKey)
	if secretKey == "" {
		secretKey = "secretAccessKey"
	}
	if ref.Name == "" || accessKey == "" || secretKey == "" {
		return SecretSpec{}
	}
	return SecretSpec{
		Name:      ref.Name,
		Namespace: namespace,
		StringData: map[string]string{
			accessKey: randomHex(12),
			secretKey: randomHex(24),
		},
	}
}

func credentialsSecretFromOSS(namespace string, ref infrav1alpha1.OSSCredentialsSecret) SecretSpec {
	accessKey := strings.TrimSpace(ref.AccessKeyKey)
	if accessKey == "" {
		accessKey = "accessKeyId"
	}
	secretKey := strings.TrimSpace(ref.SecretKeyKey)
	if secretKey == "" {
		secretKey = "accessKeySecret"
	}
	if ref.Name == "" || accessKey == "" || secretKey == "" {
		return SecretSpec{}
	}
	return SecretSpec{
		Name:      ref.Name,
		Namespace: namespace,
		StringData: map[string]string{
			accessKey: randomHex(12),
			secretKey: randomHex(24),
		},
	}
}

func keyPairSecretFromRef(namespace string, ref *infrav1alpha1.KeyPairSecretRef) SecretSpec {
	if ref == nil || ref.Name == "" {
		return SecretSpec{}
	}
	privateKey := strings.TrimSpace(ref.PrivateKeyKey)
	if privateKey == "" {
		privateKey = "private.key"
	}
	publicKey := strings.TrimSpace(ref.PublicKeyKey)
	if publicKey == "" {
		publicKey = "public.key"
	}
	return SecretSpec{
		Name:      ref.Name,
		Namespace: namespace,
		StringData: map[string]string{
			privateKey: randomHex(32),
			publicKey:  randomHex(32),
		},
	}
}
