// Copyright 2026 Sandbox0 Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/sandbox0-ai/sandbox0/nomad-driver-sandbox0/internal/driver"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
)

func main() {
	socket := flag.String("socket", "/run/sandbox0/rootfs-sessiond.sock", "root-owned session daemon socket")
	stageFile := flag.String("stage-file", "", "durable StageRequest JSON file")
	operationID := flag.String("operation-id", "", "stable regional fork operation ID")
	sourceSandboxID := flag.String("source-sandbox-id", "", "active source sandbox ID")
	targetSandboxID := flag.String("target-sandbox-id", "", "pre-created paused target sandbox ID")
	targetGenerationID := flag.String("target-generation-id", "", "immutable target generation ID")
	timeout := flag.Duration("timeout", 5*time.Minute, "capture and regional publication timeout")
	flag.Parse()

	stage, err := readStage(*stageFile)
	if err != nil {
		fatal("read stage: %v", err)
	}
	fork := rootfshandoff.RunningForkCheckpointRequest{
		OperationID: strings.TrimSpace(*operationID), SourceSandboxID: strings.TrimSpace(*sourceSandboxID),
		TargetSandboxID: strings.TrimSpace(*targetSandboxID), TargetGenerationID: strings.TrimSpace(*targetGenerationID),
	}
	if err := fork.Validate(); err != nil {
		fatal("validate fork: %v", err)
	}
	if *timeout <= 0 {
		fatal("timeout must be positive")
	}
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	result, err := driver.RequestRunningRootFSFork(ctx, *socket, stage, fork)
	if err != nil {
		fatal("publish running fork: %v", err)
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(result); err != nil {
		fatal("encode result: %v", err)
	}
}

func readStage(path string) (rootfshandoff.StageRequest, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return rootfshandoff.StageRequest{}, fmt.Errorf("stage-file is required")
	}
	payload, err := os.ReadFile(path)
	if err != nil {
		return rootfshandoff.StageRequest{}, err
	}
	var envelope struct {
		Stage rootfshandoff.StageRequest `json:"stage"`
	}
	if err := json.Unmarshal(payload, &envelope); err != nil {
		return rootfshandoff.StageRequest{}, err
	}
	stage := envelope.Stage
	if stage.Parent == "" {
		if err := json.Unmarshal(payload, &stage); err != nil {
			return rootfshandoff.StageRequest{}, err
		}
	}
	stage = stage.WithoutWriterGrantToken()
	if err := stage.ValidateDurableBinding(); err != nil {
		return rootfshandoff.StageRequest{}, err
	}
	return stage, nil
}

func fatal(format string, args ...any) {
	_, _ = fmt.Fprintf(os.Stderr, "nomad-rootfs-sessionctl: "+format+"\n", args...)
	os.Exit(1)
}
