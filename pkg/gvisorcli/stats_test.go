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

package gvisorcli

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

const stockStatsFixture = `{
  "type": "stats",
  "id": "container-a",
  "data": {
    "cpu": {"usage": {"kernel": 11, "user": 22, "total": 33, "percpu": [16, 17]}},
    "memory": {
      "cache": 44,
      "usage": {"limit": 1000, "usage": 45, "max": 46, "failcnt": 1},
      "swap": {"limit": 2000, "usage": 47, "max": 48, "failcnt": 2},
      "kernel": {"limit": 3000, "usage": 49, "max": 50, "failcnt": 3},
      "kernelTCP": {"limit": 4000, "usage": 51, "max": 52, "failcnt": 4},
      "raw": {"rss": 53}
    },
    "pids": {"current": 5, "limit": 128},
    "network_interfaces": [{
      "Name": "eth0",
      "RxBytes": 54,
      "RxPackets": 55,
      "RxErrors": 56,
      "RxDropped": 57,
      "TxBytes": 58,
      "TxPackets": 59,
      "TxErrors": 60,
      "TxDropped": 61
    }]
  }
}`

func TestCommandStatsDecodesStockEventAndUsesExpectedArguments(t *testing.T) {
	tempDir := t.TempDir()
	fixturePath := filepath.Join(tempDir, "stats.json")
	argsPath := filepath.Join(tempDir, "args")
	if err := os.WriteFile(fixturePath, []byte(stockStatsFixture), 0o600); err != nil {
		t.Fatalf("write stats fixture: %v", err)
	}
	t.Setenv("RUNSC_FIXTURE", fixturePath)
	t.Setenv("RUNSC_ARGS", argsPath)
	runner := newOutputRunsc(t, `
printf '%s\n' "$@" >"$RUNSC_ARGS"
cat "$RUNSC_FIXTURE"
`)

	stats, err := runner.Stats(context.Background(), "container-a")
	if err != nil {
		t.Fatalf("Stats() error = %v", err)
	}
	if stats.Data.CPU.Usage.Total != 33 || stats.Data.Memory.KernelTCP.Usage != 51 || stats.Data.Pids.Current != 5 {
		t.Fatalf("Stats() did not preserve stock fields: %+v", stats)
	}
	if len(stats.Data.NetworkInterfaces) != 1 || stats.Data.NetworkInterfaces[0].Name != "eth0" ||
		stats.Data.NetworkInterfaces[0].TxDropped != 61 {
		t.Fatalf("Stats() network interfaces = %+v", stats.Data.NetworkInterfaces)
	}
	arguments, err := os.ReadFile(argsPath)
	if err != nil {
		t.Fatalf("read runsc arguments: %v", err)
	}
	want := []string{
		"--root=" + runner.config.Root,
		"--platform=systrap",
		"--overlay2=none",
		"--file-access=shared",
		"--directfs=true",
		"events",
		"--stats",
		"container-a",
	}
	if got := strings.Split(strings.TrimSpace(string(arguments)), "\n"); !reflect.DeepEqual(got, want) {
		t.Fatalf("runsc arguments = %#v, want %#v", got, want)
	}
}

func TestCommandStatsRejectsInvalidOutput(t *testing.T) {
	tests := []struct {
		name       string
		fixture    string
		container  string
		wantSubstr string
	}{
		{
			name:       "unknown field",
			fixture:    strings.Replace(stockStatsFixture, `"type": "stats"`, `"type": "stats", "future": true`, 1),
			container:  "container-a",
			wantSubstr: "unknown field",
		},
		{
			name:       "wrong event type",
			fixture:    strings.Replace(stockStatsFixture, `"type": "stats"`, `"type": "oom"`, 1),
			container:  "container-a",
			wantSubstr: "is not stats",
		},
		{
			name:       "wrong container identity",
			fixture:    stockStatsFixture,
			container:  "container-b",
			wantSubstr: "does not match",
		},
		{
			name:       "trailing JSON value",
			fixture:    stockStatsFixture + "\n{}",
			container:  "container-a",
			wantSubstr: "exactly one JSON value",
		},
		{
			name: "duplicate interface",
			fixture: strings.Replace(stockStatsFixture, `"network_interfaces": [{`,
				`"network_interfaces": [{"Name":"eth0"}, {`, 1),
			container:  "container-a",
			wantSubstr: "is duplicated",
		},
		{
			name: "null interface",
			fixture: strings.Replace(stockStatsFixture, `"network_interfaces": [{`,
				`"network_interfaces": [null, {`, 1),
			container:  "container-a",
			wantSubstr: "must not be null",
		},
		{
			name:       "non-canonical expected identity",
			fixture:    stockStatsFixture,
			container:  " container-a",
			wantSubstr: "must be canonical",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tempDir := t.TempDir()
			fixturePath := filepath.Join(tempDir, "stats.json")
			if err := os.WriteFile(fixturePath, []byte(test.fixture), 0o600); err != nil {
				t.Fatalf("write stats fixture: %v", err)
			}
			t.Setenv("RUNSC_FIXTURE", fixturePath)
			runner := newOutputRunsc(t, `cat "$RUNSC_FIXTURE"`)

			_, err := runner.Stats(context.Background(), test.container)
			if err == nil || !strings.Contains(err.Error(), test.wantSubstr) {
				t.Fatalf("Stats() error = %v, want substring %q", err, test.wantSubstr)
			}
		})
	}
}

func TestCommandStatsBoundsOutputAndStderr(t *testing.T) {
	t.Run("stdout", func(t *testing.T) {
		runner := newOutputRunsc(t, `dd if=/dev/zero bs=1048576 count=3 2>/dev/null`)
		_, err := runner.Stats(context.Background(), "container-a")
		if err == nil || !strings.Contains(err.Error(), "stdout exceeds 2097152 bytes") {
			t.Fatalf("Stats() error = %v", err)
		}
	})

	t.Run("stderr", func(t *testing.T) {
		runner := newOutputRunsc(t, `
dd if=/dev/zero bs=65537 count=1 1>&2 2>/dev/null
exit 1
`)
		_, err := runner.Stats(context.Background(), "container-a")
		if err == nil || !strings.Contains(err.Error(), "stderr exceeds 65536 bytes") {
			t.Fatalf("Stats() error = %v", err)
		}
	})
}

func TestBoundedBufferDiscardsBytesBeyondLimit(t *testing.T) {
	buffer := boundedBuffer{limit: 4}
	payload := make([]byte, 1<<20)
	written, err := buffer.Write(payload)
	if err != nil || written != len(payload) {
		t.Fatalf("Write() = (%d, %v), want (%d, nil)", written, err, len(payload))
	}
	if !buffer.exceeded || buffer.Len() != 4 {
		t.Fatalf("bounded buffer = {len:%d exceeded:%t}", buffer.Len(), buffer.exceeded)
	}
}

func newOutputRunsc(t *testing.T, body string) *Command {
	t.Helper()
	tempDir := t.TempDir()
	runscPath := filepath.Join(tempDir, "runsc")
	if err := os.WriteFile(runscPath, []byte("#!/bin/sh\nset -eu\n"+body+"\n"), 0o755); err != nil {
		t.Fatalf("write fake runsc: %v", err)
	}
	return &Command{config: Config{
		Path:       runscPath,
		Root:       filepath.Join(tempDir, "root"),
		Platform:   "systrap",
		Overlay2:   "none",
		FileAccess: "shared",
		DirectFS:   true,
	}}
}
