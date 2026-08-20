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

import "testing"

func TestParseAllowedClientsSupportsNodeChannelRoute(t *testing.T) {
	identities, err := parseAllowedClients(
		"legacy:node-uid-1:pod-1,channel:node-uid-2:pod-2:cluster-2:node-2",
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(identities) != 2 {
		t.Fatalf("identity count = %d", len(identities))
	}
	if identities[0].ClusterID != "" || identities[0].NodeID != "" {
		t.Fatalf("legacy identity unexpectedly acquired a route: %+v", identities[0])
	}
	if identities[1].ClusterID != "cluster-2" || identities[1].NodeID != "node-2" {
		t.Fatalf("node channel route = %+v", identities[1])
	}
}

func TestParseAllowedClientsRejectsPartialNodeChannelRoute(t *testing.T) {
	for _, value := range []string{
		"channel:node-uid:pod:cluster:",
		"channel:node-uid:pod::node",
		"channel:node-uid:pod:cluster",
	} {
		if _, err := parseAllowedClients(value); err == nil {
			t.Fatalf("parseAllowedClients(%q) unexpectedly succeeded", value)
		}
	}
}

func TestRuntimeSlotTerminalWorkerRequiresExplicitEnablement(t *testing.T) {
	worker, err := newRuntimeSlotTerminalWorker(nil, nil, runtimeSlotTerminalConfig{})
	if err != nil || worker != nil {
		t.Fatalf("disabled worker = %v, %v", worker, err)
	}
	_, err = newRuntimeSlotTerminalWorker(nil, nil, runtimeSlotTerminalConfig{
		NomadEndpointsFile: "/etc/sandbox0/nomad-endpoints.json",
	})
	if err == nil {
		t.Fatal("disabled worker accepted a silently ignored endpoint catalog")
	}
	_, err = newRuntimeSlotTerminalWorker(nil, nil, runtimeSlotTerminalConfig{Enabled: true})
	if err == nil {
		t.Fatal("enabled worker accepted missing dependencies")
	}
}
