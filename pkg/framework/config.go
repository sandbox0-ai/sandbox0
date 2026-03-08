package framework

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// Config defines E2E runtime configuration.
type Config struct {
	ClusterName           string
	KindConfigPath        string
	Kubeconfig            string
	UseExistingCluster    bool
	SkipClusterDelete     bool
	SkipOperatorInstall   bool
	SkipOperatorUninstall bool

	OperatorChartPath       string
	OperatorNamespace       string
	OperatorReleaseName     string
	OperatorDeploymentName  string
	OperatorValuesPath      string
	OperatorImageRepo       string
	OperatorImageTag        string
	OperatorImagePullPolicy string

	InfraNamespace string
}

// LoadConfig reads E2E configuration from environment variables.
func LoadConfig() (Config, error) {
	infraRoot, err := FindInfraRoot()
	if err != nil {
		return Config{}, err
	}

	defaultKindConfig := filepath.Join(infraRoot, "tests", "e2e", "kind-config.yaml")
	defaultOperatorChart := filepath.Join(infraRoot, "infra-operator", "chart")
	defaultOperatorValues := filepath.Join(defaultOperatorChart, "values.yaml")
	cfg := Config{
		ClusterName:           envString("E2E_CLUSTER_NAME", "sandbox0-e2e"),
		KindConfigPath:        envString("E2E_KIND_CONFIG", defaultKindConfig),
		Kubeconfig:            envString("E2E_KUBECONFIG", ""),
		UseExistingCluster:    envBool("E2E_USE_EXISTING_CLUSTER", false),
		SkipClusterDelete:     envBool("E2E_SKIP_CLUSTER_DELETE", false),
		SkipOperatorInstall:   envBool("E2E_SKIP_OPERATOR_INSTALL", false),
		SkipOperatorUninstall: envBool("E2E_SKIP_OPERATOR_UNINSTALL", false),

		OperatorChartPath:       envString("E2E_OPERATOR_CHART", defaultOperatorChart),
		OperatorNamespace:       envString("E2E_OPERATOR_NAMESPACE", "infra-operator"),
		OperatorReleaseName:     envString("E2E_OPERATOR_RELEASE", "infra-operator"),
		OperatorDeploymentName:  envString("E2E_OPERATOR_DEPLOYMENT", "infra-operator"),
		OperatorValuesPath:      envString("E2E_OPERATOR_VALUES", defaultOperatorValues),
		OperatorImageRepo:       envString("E2E_OPERATOR_IMAGE_REPO", ""),
		OperatorImageTag:        envString("E2E_OPERATOR_IMAGE_TAG", "latest"),
		OperatorImagePullPolicy: envString("E2E_OPERATOR_IMAGE_PULL_POLICY", "IfNotPresent"),

		InfraNamespace: envString("E2E_INFRA_NAMESPACE", "sandbox0-system"),
	}

	if cfg.UseExistingCluster {
		cfg.SkipClusterDelete = true
	}

	if cfg.KindConfigPath == "" {
		return Config{}, fmt.Errorf("kind config path is required")
	}
	if cfg.OperatorChartPath == "" && !cfg.SkipOperatorInstall {
		return Config{}, fmt.Errorf("operator chart path is required")
	}
	return cfg, nil
}

func envString(key, fallback string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	return value
}

func envBool(key string, fallback bool) bool {
	raw := strings.TrimSpace(strings.ToLower(os.Getenv(key)))
	if raw == "" {
		return fallback
	}
	switch raw {
	case "1", "true", "yes", "y":
		return true
	case "0", "false", "no", "n":
		return false
	default:
		return fallback
	}
}
