package main

import (
	"fmt"
	"os"

	"github.com/sandbox0-ai/sandbox0/pkg/framework"
)

func main() {
	cfg, err := framework.LoadConfig()
	if err != nil {
		exitf("load e2e config: %v", err)
	}
	cfg.PreserveScenario = true

	manifestPath, err := framework.ResolveSamplePath(cfg, "single-cluster/fullmode.yaml")
	if err != nil {
		exitf("resolve fullmode scenario: %v", err)
	}
	scenario, err := framework.BuildScenarioFromManifest(cfg, manifestPath)
	if err != nil {
		exitf("build fullmode scenario: %v", err)
	}

	env, cleanup, err := framework.SetupScenario(cfg, scenario)
	if err != nil {
		exitf("setup fullmode scenario: %v", err)
	}
	if err := framework.WaitForSandbox0InfraReady(env.TestCtx.Context, env.Config.Kubeconfig, env.Infra, "10m"); err != nil {
		cleanup()
		exitf("wait for fullmode scenario: %v", err)
	}
	cleanup()
	fmt.Printf("S0FS POSIX fullmode scenario %q is ready in namespace %q.\n", scenario.InfraName, scenario.InfraNamespace)
}

func exitf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
