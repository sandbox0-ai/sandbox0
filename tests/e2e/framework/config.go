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
	TestMode              string

	OperatorChartPath      string
	OperatorNamespace      string
	OperatorReleaseName    string
	OperatorDeploymentName string
	OperatorValuesPath     string
	OperatorImageRepo      string
	OperatorImageTag       string

	InfraNamespace                string
	InfraControlPlaneManifestPath string
	InfraControlPlaneName         string
	InfraDataPlaneManifestPath    string
	InfraDataPlaneName            string
	InfraAllManifestPath          string
	InfraAllName                  string
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
	defaultInfraControlPlane := filepath.Join(defaultOperatorChart, "samples", "infra_v1alpha1_sandbox0infra_controlplane.yaml")
	defaultInfraDataPlane := filepath.Join(defaultOperatorChart, "samples", "infra_v1alpha1_sandbox0infra_dataplane.yaml")
	defaultInfraAll := filepath.Join(defaultOperatorChart, "samples", "infra_v1alpha1_sandbox0infra.yaml")

	cfg := Config{
		ClusterName:           envString("E2E_CLUSTER_NAME", "sandbox0-e2e"),
		KindConfigPath:        envString("E2E_KIND_CONFIG", defaultKindConfig),
		Kubeconfig:            envString("E2E_KUBECONFIG", ""),
		UseExistingCluster:    envBool("E2E_USE_EXISTING_CLUSTER", false),
		SkipClusterDelete:     envBool("E2E_SKIP_CLUSTER_DELETE", false),
		SkipOperatorInstall:   envBool("E2E_SKIP_OPERATOR_INSTALL", false),
		SkipOperatorUninstall: envBool("E2E_SKIP_OPERATOR_UNINSTALL", false),
		TestMode:              envString("E2E_TEST_MODE", "all"),

		OperatorChartPath:      envString("E2E_OPERATOR_CHART", defaultOperatorChart),
		OperatorNamespace:      envString("E2E_OPERATOR_NAMESPACE", "infra-operator"),
		OperatorReleaseName:    envString("E2E_OPERATOR_RELEASE", "infra-operator"),
		OperatorDeploymentName: envString("E2E_OPERATOR_DEPLOYMENT", "infra-operator-manager"),
		OperatorValuesPath:     envString("E2E_OPERATOR_VALUES", defaultOperatorValues),
		OperatorImageRepo:      envString("E2E_OPERATOR_IMAGE_REPO", ""),
		OperatorImageTag:       envString("E2E_OPERATOR_IMAGE_TAG", ""),

		InfraNamespace:                envString("E2E_INFRA_NAMESPACE", "sandbox0-system"),
		InfraControlPlaneManifestPath: envString("E2E_INFRA_CONTROL_PLANE_MANIFEST", defaultInfraControlPlane),
		InfraControlPlaneName:         envString("E2E_INFRA_CONTROL_PLANE_NAME", "sandbox0-control-plane"),
		InfraDataPlaneManifestPath:    envString("E2E_INFRA_DATA_PLANE_MANIFEST", defaultInfraDataPlane),
		InfraDataPlaneName:            envString("E2E_INFRA_DATA_PLANE_NAME", "sandbox0-data-plane"),
		InfraAllManifestPath:          envString("E2E_INFRA_ALL_MANIFEST", defaultInfraAll),
		InfraAllName:                  envString("E2E_INFRA_ALL_NAME", "sandbox0-dev"),
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
	if cfg.InfraControlPlaneManifestPath == "" || cfg.InfraDataPlaneManifestPath == "" || cfg.InfraAllManifestPath == "" {
		return Config{}, fmt.Errorf("infra manifest paths are required")
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
