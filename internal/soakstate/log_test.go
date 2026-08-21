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

package soakstate

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type testCheckpoint struct {
	Next int `json:"next"`
}

func TestLogResumesHashChainedCheckpointAndTruncatesPartialTail(t *testing.T) {
	root := t.TempDir()
	bootIDPath := filepath.Join(root, "boot-id")
	require.NoError(t, os.WriteFile(bootIDPath, []byte("boot-a\n"), 0o600))
	path := filepath.Join(root, "evidence.jsonl")
	config := struct {
		Duration string `json:"duration"`
	}{Duration: "24h"}
	log, err := Open(OpenOptions{
		Path: path, Mode: ModeCreate, Config: config,
		Initial: testCheckpoint{}, BootIDPath: bootIDPath,
	})
	require.NoError(t, err)
	runID := log.RunID()
	require.NoError(t, log.Commit("sample", 5*time.Second, map[string]int{"value": 1}, testCheckpoint{Next: 7}))
	require.NoError(t, log.Close())

	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	require.NoError(t, err)
	_, err = file.WriteString(`{"partial":`)
	require.NoError(t, err)
	require.NoError(t, file.Sync())
	require.NoError(t, file.Close())
	require.NoError(t, os.WriteFile(bootIDPath, []byte("boot-b\n"), 0o600))

	resumed, err := Open(OpenOptions{
		Path: path, Mode: ModeResume, Config: config,
		Initial: testCheckpoint{}, BootIDPath: bootIDPath,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, resumed.Close()) })
	require.Equal(t, runID, resumed.RunID())
	require.Equal(t, 5*time.Second, resumed.ActiveElapsed())
	info := resumed.ResumeInfo()
	require.True(t, info.Resumed)
	require.Equal(t, "boot-a", info.PreviousBootID)
	require.Equal(t, "boot-b", info.CurrentBootID)
	require.True(t, info.BootChanged)
	require.False(t, info.OpenedAt.IsZero())
	require.Positive(t, info.TruncatedPartialBytes)
	var checkpoint testCheckpoint
	require.NoError(t, resumed.DecodeCheckpoint(&checkpoint))
	require.Equal(t, 7, checkpoint.Next)
	require.NoError(t, resumed.Commit("resumed", 5*time.Second, info, checkpoint))
}

func TestLogRejectsConfigurationChangeAndConcurrentWriter(t *testing.T) {
	root := t.TempDir()
	bootIDPath := filepath.Join(root, "boot-id")
	require.NoError(t, os.WriteFile(bootIDPath, []byte("boot-a\n"), 0o600))
	path := filepath.Join(root, "evidence.jsonl")
	log, err := Open(OpenOptions{
		Path: path, Mode: ModeCreate, Config: map[string]int{"proofs": 10},
		Initial: testCheckpoint{}, BootIDPath: bootIDPath,
	})
	require.NoError(t, err)
	_, err = Open(OpenOptions{
		Path: path, Mode: ModeResume, Config: map[string]int{"proofs": 10},
		Initial: testCheckpoint{}, BootIDPath: bootIDPath,
	})
	require.ErrorContains(t, err, "lock soak evidence")
	require.NoError(t, log.Close())
	_, err = Open(OpenOptions{
		Path: path, Mode: ModeResume, Config: map[string]int{"proofs": 11},
		Initial: testCheckpoint{}, BootIDPath: bootIDPath,
	})
	require.ErrorContains(t, err, "configuration digest changed")
}

func TestLogRejectsTamperedCompleteEvent(t *testing.T) {
	root := t.TempDir()
	bootIDPath := filepath.Join(root, "boot-id")
	require.NoError(t, os.WriteFile(bootIDPath, []byte("boot-a\n"), 0o600))
	path := filepath.Join(root, "evidence.jsonl")
	log, err := Open(OpenOptions{
		Path: path, Mode: ModeCreate, Config: map[string]int{"proofs": 10},
		Initial: testCheckpoint{}, BootIDPath: bootIDPath,
	})
	require.NoError(t, err)
	require.NoError(t, log.Close())
	payload, err := os.ReadFile(path)
	require.NoError(t, err)
	var event map[string]any
	require.NoError(t, json.Unmarshal(payload[:len(payload)-1], &event))
	event["data"] = map[string]any{"proofs": float64(11)}
	payload, err = json.Marshal(event)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, append(payload, '\n'), 0o600))
	_, err = Open(OpenOptions{
		Path: path, Mode: ModeResume, Config: map[string]int{"proofs": 10},
		Initial: testCheckpoint{}, BootIDPath: bootIDPath,
	})
	require.ErrorContains(t, err, "event digest mismatch")
}
