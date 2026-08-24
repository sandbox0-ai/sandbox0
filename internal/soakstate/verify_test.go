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
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestVerifyFileReturnsIndependentHashChainedSummary(t *testing.T) {
	root := t.TempDir()
	bootIDPath := filepath.Join(root, "boot-id")
	require.NoError(t, os.WriteFile(bootIDPath, []byte("boot-a\n"), 0o600))
	path := filepath.Join(root, "evidence.jsonl")
	config := map[string]any{"duration": "24h0m0s", "proofs": 10_000}
	checkpoint := map[string]any{"phase": "active", "next": 0}
	log, err := Open(OpenOptions{
		Path: path, Mode: ModeCreate, Config: config, Initial: checkpoint, BootIDPath: bootIDPath,
	})
	require.NoError(t, err)
	executableDigest := log.ExecutableSHA256()
	configDigest := log.ConfigSHA256()
	checkpoint = map[string]any{"phase": "complete", "next": 10_000}
	require.NoError(t, log.Commit("final", 24*time.Hour, map[string]any{"passed": true}, checkpoint))
	require.NoError(t, log.Close())

	verified, err := VerifyFile(VerifyOptions{
		Path: path, ExpectedConfigSHA256: configDigest,
		ExpectedExecutableSHA256: executableDigest, RequireFinal: true,
	})
	require.NoError(t, err)
	require.Equal(t, uint64(2), verified.Records)
	require.Equal(t, executableDigest, verified.ExecutableSHA256)
	require.Equal(t, configDigest, verified.ConfigSHA256)
	require.Equal(t, 24*time.Hour, verified.ActiveElapsed)
	require.Equal(t, "final", verified.LastType)
	require.JSONEq(t, `{"duration":"24h0m0s","proofs":10000}`, string(verified.Config))
	require.JSONEq(t, `{"passed":true}`, string(verified.LastData))
	require.JSONEq(t, `{"next":10000,"phase":"complete"}`, string(verified.LastCheckpoint))
}

func TestVerifyFileRejectsActiveWriterAndIncompleteTail(t *testing.T) {
	root := t.TempDir()
	bootIDPath := filepath.Join(root, "boot-id")
	require.NoError(t, os.WriteFile(bootIDPath, []byte("boot-a\n"), 0o600))
	path := filepath.Join(root, "evidence.jsonl")
	log, err := Open(OpenOptions{
		Path: path, Mode: ModeCreate, Config: map[string]int{"proofs": 10},
		Initial: testCheckpoint{}, BootIDPath: bootIDPath,
	})
	require.NoError(t, err)
	_, err = VerifyFile(VerifyOptions{Path: path})
	require.ErrorContains(t, err, "lock soak evidence for verification")
	require.NoError(t, log.Close())

	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	require.NoError(t, err)
	_, err = file.WriteString(`{"partial":`)
	require.NoError(t, err)
	require.NoError(t, file.Sync())
	require.NoError(t, file.Close())
	_, err = VerifyFile(VerifyOptions{Path: path})
	require.ErrorContains(t, err, "incomplete trailing record")
}

func TestVerifyFileRejectsUnexpectedDigestAndMissingFinal(t *testing.T) {
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

	_, err = VerifyFile(VerifyOptions{Path: path, RequireFinal: true})
	require.ErrorContains(t, err, "does not end with a final event")
	_, err = VerifyFile(VerifyOptions{Path: path, ExpectedExecutableSHA256: strings.Repeat("0", 64)})
	require.ErrorContains(t, err, "executable digest does not match")
	_, err = VerifyFile(VerifyOptions{Path: path, ExpectedConfigSHA256: "not-a-digest"})
	require.ErrorContains(t, err, "expected configuration SHA-256")
}
