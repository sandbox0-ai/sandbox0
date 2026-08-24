package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/internal/soakstate"
	"github.com/stretchr/testify/require"
)

func TestValidateMaterializerRequiresExactProductionContract(t *testing.T) {
	verified := soakstate.Verification{
		ActiveElapsed: 24 * time.Hour,
		Config:        mustJSON(t, validMaterializerConfig()),
	}
	final := validMaterializerFinal()
	verified.LastData = mustJSON(t, final)
	verified.LastCheckpoint = mustJSON(t, validMaterializerCheckpoint(verified.ActiveElapsed))

	_, err := validateMaterializer(verified, 24*time.Hour)
	require.NoError(t, err)
	final.Counters.ExpectedWorkerErrors = 1
	verified.LastData = mustJSON(t, final)
	_, err = validateMaterializer(verified, 24*time.Hour)
	require.ErrorContains(t, err, "final data")

	final = validMaterializerFinal()
	final.Objects.Objects = final.Bounds.MaxObjects + 1
	final.Database.CatalogObjects = final.Objects.Objects - 1
	verified.LastData = mustJSON(t, final)
	_, err = validateMaterializer(verified, 24*time.Hour)
	require.ErrorContains(t, err, "final data")
}

func TestValidateMaterializerAcceptsExplicitAcceleratedProfile(t *testing.T) {
	config := validMaterializerConfig()
	config.Duration = time.Minute.String()
	config.WorkerInterval = (10 * time.Millisecond).String()
	config.SampleInterval = time.Second.String()
	config.MaxDelay = (2 * time.Second).String()
	final := validMaterializerFinal()
	final.Counters.RetainedBatches = 12
	final.Database.CatalogObjects = 25
	final.Objects.Objects = 26
	final.Bounds.MaxBatches = 33
	final.Bounds.MaxObjects = 68
	verified := soakstate.Verification{
		ActiveElapsed:  111 * time.Second,
		Config:         mustJSON(t, config),
		LastData:       mustJSON(t, final),
		LastCheckpoint: mustJSON(t, validMaterializerCheckpoint(111*time.Second)),
	}

	_, err := validateMaterializer(verified, time.Minute)
	require.NoError(t, err)
	config.WorkerInterval = time.Millisecond.String()
	verified.Config = mustJSON(t, config)
	_, err = validateMaterializer(verified, time.Minute)
	require.ErrorContains(t, err, "accelerated profile")
}

func TestVerifyAuditsCompletedHashChainedEvidence(t *testing.T) {
	directory := t.TempDir()
	bootIDPath := directory + "/boot-id"
	require.NoError(t, os.WriteFile(bootIDPath, []byte("test-boot\n"), 0o600))
	evidencePath := directory + "/materializer.jsonl"
	evidence, err := soakstate.Open(soakstate.OpenOptions{
		Path: evidencePath, Mode: soakstate.ModeCreate, Config: validMaterializerConfig(),
		Initial: validMaterializerCheckpoint(0), BootIDPath: bootIDPath,
	})
	require.NoError(t, err)
	executableSHA256 := evidence.ExecutableSHA256()
	configSHA256 := evidence.ConfigSHA256()
	require.NoError(t, evidence.Commit("final", 24*time.Hour,
		validMaterializerFinal(), validMaterializerCheckpoint(24*time.Hour)))
	require.NoError(t, evidence.Close())

	result, err := verify(options{
		path: evidencePath, kind: materializerKind,
		expectedConfigSHA256: configSHA256, expectedExecutableSHA256: executableSHA256,
		minimumActiveDuration: 24 * time.Hour,
	})
	require.NoError(t, err)
	require.True(t, result.Passed)
	require.Equal(t, "final", result.Evidence.LastType)
	require.Equal(t, uint64(2), result.Evidence.Records)
}

func TestValidateBoltRequiresRestartAndBoundedGrowth(t *testing.T) {
	passed := true
	verified := soakstate.Verification{
		ActiveElapsed: 24 * time.Hour,
		Config: mustJSON(t, boltConfig{
			Duration: "24h0m0s", Proofs: 10_000, Bursts: 20,
			TerminalTTL: "1h0m0s", StateDir: "/var/lib/soak/bolt", JournalFormat: 1,
		}),
		LastData: mustJSON(t, boltFinal{
			Passed: &passed, Inserted: 10_000, Deleted: 10_000,
			WarmFileBytes: 1 << 20, FinalFileBytes: (1 << 20) + int64(os.Getpagesize()),
			GrowthBytes: int64(os.Getpagesize()),
		}),
		LastCheckpoint: mustJSON(t, boltCheckpoint{
			Version: 1, Phase: "complete", ActiveElapsedNS: (24 * time.Hour).Nanoseconds(),
			Next: 10_000, Deleted: 10_000, Restarted: true, WarmSize: 1 << 20,
		}),
	}
	_, err := validateBolt(verified, 24*time.Hour)
	require.NoError(t, err)

	var final boltFinal
	require.NoError(t, json.Unmarshal(verified.LastData, &final))
	final.GrowthBytes++
	verified.LastData = mustJSON(t, final)
	_, err = validateBolt(verified, 24*time.Hour)
	require.ErrorContains(t, err, "final data")

	final.GrowthBytes--
	verified.LastData = mustJSON(t, final)
	var checkpoint boltCheckpoint
	require.NoError(t, json.Unmarshal(verified.LastCheckpoint, &checkpoint))
	checkpoint.WarmSize++
	verified.LastCheckpoint = mustJSON(t, checkpoint)
	_, err = validateBolt(verified, 24*time.Hour)
	require.ErrorContains(t, err, "checkpoint")
}

func TestVerifyRejectsIncompleteOptionsBeforeOpeningEvidence(t *testing.T) {
	_, err := verify(options{})
	require.ErrorContains(t, err, "path and expected executable")
	_, err = verify(options{path: "/missing", expectedExecutableSHA256: "digest", kind: "other", minimumActiveDuration: 24 * time.Hour})
	require.ErrorContains(t, err, "kind")
	_, err = verify(options{
		path: "/evidence", output: "/evidence", kind: materializerKind,
		expectedExecutableSHA256: strings.Repeat("0", 64), minimumActiveDuration: 24 * time.Hour,
	})
	require.ErrorContains(t, err, "must not replace")
}

func TestPhysicalRustFSConfigurationMustBeCanonical(t *testing.T) {
	require.True(t, canonicalRustFSEndpoint("http://172.16.100.2:19200"))
	require.True(t, canonicalRustFSEndpoint("https://rustfs.example.test"))
	require.False(t, canonicalRustFSEndpoint("http://172.16.100.2:19200/"))
	require.False(t, canonicalRustFSEndpoint("http://user@172.16.100.2:19200"))
	require.False(t, canonicalRustFSEndpoint("ftp://172.16.100.2:19200"))
	require.False(t, canonicalRustFSEndpoint("http://172.16.100.2:0"))
	require.False(t, canonicalRustFSEndpoint("http://172.16.100.2:invalid"))
	require.True(t, canonicalListenAddress("172.16.100.2:19201"))
	require.True(t, canonicalListenAddress("[::1]:19201"))
	require.False(t, canonicalListenAddress(":19201"))
	require.False(t, canonicalListenAddress("172.16.100.2:0"))
	require.False(t, canonicalBucket("not canonical"))
}

func TestWriteReportFilePublishesMode0600RegularFile(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "verification.json")
	require.NoError(t, writeReportFile(path, []byte("first\n")))
	require.NoError(t, writeReportFile(path, []byte("second\n")))
	payload, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, "second\n", string(payload))
	info, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), info.Mode().Perm())

	target := filepath.Join(directory, "target")
	require.NoError(t, os.WriteFile(target, nil, 0o600))
	symlink := filepath.Join(directory, "symlink")
	require.NoError(t, os.Symlink(target, symlink))
	require.ErrorContains(t, writeReportFile(symlink, []byte("unsafe\n")), "regular file")
}

func validMaterializerConfig() materializerConfig {
	return materializerConfig{
		Duration: "24h0m0s", Generations: 10_000, BurstCount: 20,
		WorkerInterval: "1s", SampleInterval: "1m0s", MinPackBytes: 32 << 20,
		MaxDelay: "5m0s", PhysicalByteLimit: 512 << 20, PhysicalFileLimit: 4096,
		DatabaseGrowthLimit: 512 << 20, TerminalRetention: "24h0m0s",
		UploadingStale: "1h0m0s", GarbageInterval: "1m0s",
		RustFSEndpoint: "http://172.16.100.2:19200", RustFSBucket: "soak",
		RustFSDataDir: "/var/lib/soak/rustfs", ProxyListen: "172.16.100.2:19201",
	}
}

func validMaterializerFinal() materializerFinal {
	passed := true
	final := materializerFinal{Passed: &passed}
	final.Counters.Generated = 10_000
	final.Counters.Materialized = 10_000
	final.Counters.RetainedBatches = 254
	final.Counters.ExpectedWorkerErrors = 2
	final.Database.MaterializedGenerations = 10_001
	final.Database.CatalogObjects = 515
	final.Objects.Objects = 516
	final.Bounds.MaxBatches = 291
	final.Bounds.MaxObjects = 584
	final.DatabaseGrowthBytes = 1
	final.PhysicalGrowthFiles = 1
	final.PhysicalGrowthBytes = 1
	return final
}

func validMaterializerCheckpoint(elapsed time.Duration) materializerCheckpoint {
	return materializerCheckpoint{
		Version: 1, Phase: "passed", ActiveElapsedNS: elapsed.Nanoseconds(),
		NextGeneration: 10_000, FaultPhase: "recovered", ExpectedWorkerErrors: 2,
	}
}

func mustJSON(t *testing.T, value any) json.RawMessage {
	t.Helper()
	payload, err := json.Marshal(value)
	require.NoError(t, err)
	return payload
}
