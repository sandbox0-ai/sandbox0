package main

import (
	"fmt"
	"os"

	apiconfig "github.com/sandbox0-ai/infra/infra-operator/api/config"
	"gopkg.in/yaml.v3"
)

type configEntry struct {
	name string
	cfg  any
}

func main() {
	entries := []configEntry{
		{name: "edge-gateway", cfg: apiconfig.LoadEdgeGatewayConfig()},
		{name: "internal-gateway", cfg: apiconfig.LoadInternalGatewayConfig()},
		{name: "manager", cfg: apiconfig.LoadManagerConfig()},
		{name: "scheduler", cfg: apiconfig.LoadSchedulerConfig()},
		{name: "storage-proxy", cfg: apiconfig.LoadStorageProxyConfig()},
		{name: "netd", cfg: apiconfig.LoadNetdConfig()},
	}

	for _, entry := range entries {
		if err := updateValuesConfig(entry.name, entry.cfg); err != nil {
			fmt.Fprintf(os.Stderr, "Failed to update %s values.yaml: %v\n", entry.name, err)
			os.Exit(1)
		}
		fmt.Printf("Updated %s/chart/values.yaml\n", entry.name)
	}
}

func updateValuesConfig(service string, cfg any) error {
	cfgMap, err := toMap(cfg)
	if err != nil {
		return err
	}

	cfgYaml, err := yaml.Marshal(cfgMap)
	if err != nil {
		return err
	}

	return os.WriteFile("./"+service+".yaml", cfgYaml, 0o644)
}

func toMap(cfg any) (map[string]any, error) {
	data, err := yaml.Marshal(cfg)
	if err != nil {
		return nil, err
	}

	var cfgMap map[string]any
	if err := yaml.Unmarshal(data, &cfgMap); err != nil {
		return nil, err
	}

	return cfgMap, nil
}
