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
	"fmt"
	"strings"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/runtimeslotnode"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/runtimeslotnomad"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/runtimeslotreconciler"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/runtimeslotwriter"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
)

type runtimeSlotTerminalConfig struct {
	Enabled            bool
	NomadEndpointsFile string
	Interval           time.Duration
	PassTimeout        time.Duration
	ScanLimit          int
}

func newRuntimeSlotTerminalWorker(
	store *sandboxstore.PGSandboxStore,
	hub *runtimeslotnode.ChannelHub,
	config runtimeSlotTerminalConfig,
) (*runtimeslotreconciler.Worker, error) {
	if !config.Enabled {
		if strings.TrimSpace(config.NomadEndpointsFile) != "" {
			return nil, fmt.Errorf("runtime slot terminal reconciler must be enabled when a Nomad endpoint catalog is configured")
		}
		return nil, nil
	}
	if store == nil || hub == nil {
		return nil, fmt.Errorf("runtime slot terminal store and node channel are required")
	}
	resolver, err := runtimeslotnomad.LoadStaticEndpointResolver(config.NomadEndpointsFile)
	if err != nil {
		return nil, fmt.Errorf("load runtime slot Nomad endpoints: %w", err)
	}
	nomadAPI, err := runtimeslotnomad.NewHTTPAPI(resolver)
	if err != nil {
		return nil, fmt.Errorf("create runtime slot Nomad API: %w", err)
	}
	allocation, err := runtimeslotnomad.New(nomadAPI)
	if err != nil {
		return nil, fmt.Errorf("create runtime slot Nomad controller: %w", err)
	}
	node, err := runtimeslotnode.New(hub)
	if err != nil {
		return nil, fmt.Errorf("create runtime slot node controller: %w", err)
	}
	writer, err := runtimeslotwriter.New(store)
	if err != nil {
		return nil, fmt.Errorf("create runtime slot writer controller: %w", err)
	}
	reconciler, err := runtimeslotreconciler.New(runtimeslotreconciler.Config{
		Store: store, Allocation: allocation, Node: node, Writer: writer, Limit: config.ScanLimit,
	})
	if err != nil {
		return nil, fmt.Errorf("create runtime slot terminal reconciler: %w", err)
	}
	worker, err := runtimeslotreconciler.NewWorker(runtimeslotreconciler.WorkerConfig{
		Runner: reconciler, Interval: config.Interval, PassTimeout: config.PassTimeout,
	})
	if err != nil {
		return nil, fmt.Errorf("create runtime slot terminal worker: %w", err)
	}
	return worker, nil
}
