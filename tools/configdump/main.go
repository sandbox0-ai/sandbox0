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
	outDir := filepath.Join("helm", "configs")
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create output directory: %v\n", err)
		os.Exit(1)
	}

	entries := []configEntry{
		{name: "edge-gateway", cfg: edgeconfig.LoadConfig()},
		{name: "internal-gateway", cfg: internalconfig.LoadConfig()},
		{name: "manager", cfg: managerconfig.LoadConfig()},
		{name: "scheduler", cfg: schedulerconfig.LoadConfig()},
		{name: "storage-proxy", cfg: storageconfig.LoadConfig()},
		{name: "netd", cfg: netdconfig.LoadConfig()},
	}

	for _, entry := range entries {
		if err := writeConfig(outDir, entry.name, entry.cfg); err != nil {
			fmt.Fprintf(os.Stderr, "Failed to write %s config: %v\n", entry.name, err)
			os.Exit(1)
		}
		fmt.Printf("Wrote %s/%s.yaml\n", outDir, entry.name)
	}
}

func writeConfig(outDir, name string, cfg any) error {
	data, err := yaml.Marshal(cfg)
	if err != nil {
		return err
	}

	outPath := filepath.Join(outDir, fmt.Sprintf("%s.yaml", name))
	return os.WriteFile(outPath, data, 0o644)
}
