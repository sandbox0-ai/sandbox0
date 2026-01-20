package main

import (
	"fmt"
	"os"
	"path/filepath"

	edgeconfig "github.com/sandbox0-ai/infra/edge-gateway/pkg/config"
	internalconfig "github.com/sandbox0-ai/infra/internal-gateway/pkg/config"
	managerconfig "github.com/sandbox0-ai/infra/manager/pkg/config"
	netdconfig "github.com/sandbox0-ai/infra/netd/pkg/config"
	schedulerconfig "github.com/sandbox0-ai/infra/scheduler/pkg/config"
	storageconfig "github.com/sandbox0-ai/infra/storage-proxy/pkg/config"
	"gopkg.in/yaml.v3"
)

type configEntry struct {
	name string
	cfg  any
}

func main() {
	entries := []configEntry{
		{name: "edge-gateway", cfg: edgeconfig.LoadConfig()},
		{name: "internal-gateway", cfg: internalconfig.LoadConfig()},
		{name: "manager", cfg: managerconfig.LoadConfig()},
		{name: "scheduler", cfg: schedulerconfig.LoadConfig()},
		{name: "storage-proxy", cfg: storageconfig.LoadConfig()},
		{name: "netd", cfg: netdconfig.LoadConfig()},
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
	valuesPath := filepath.Join(service, "chart", "values.yaml")
	valuesData, err := os.ReadFile(valuesPath)
	if err != nil {
		return err
	}

	var values map[string]any
	if err := yaml.Unmarshal(valuesData, &values); err != nil {
		return err
	}

	cfgMap, err := toMap(cfg)
	if err != nil {
		return err
	}

	values["config"] = cfgMap
	updated, err := yaml.Marshal(values)
	if err != nil {
		return err
	}

	return os.WriteFile(valuesPath, updated, 0o644)
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
