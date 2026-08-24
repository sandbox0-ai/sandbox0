package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBuildGenerationScheduleMixesLowRateAndExactBursts(t *testing.T) {
	schedule := buildGenerationSchedule(10_000, 24*time.Hour, 5*time.Minute, 20)
	require.Len(t, schedule, 10_000)
	seen := make(map[int]struct{}, len(schedule))
	duplicates := 0
	for index, item := range schedule {
		require.GreaterOrEqual(t, item.offset, time.Duration(0))
		require.Less(t, item.offset, 24*time.Hour-5*time.Minute)
		if index > 0 {
			require.LessOrEqual(t, schedule[index-1].offset, item.offset)
			if schedule[index-1].offset == item.offset {
				duplicates++
			}
		}
		_, found := seen[item.index]
		require.False(t, found)
		seen[item.index] = struct{}{}
	}
	require.Greater(t, duplicates, 4_000, "half the lifecycle must arrive in deterministic bursts")
}

func TestOutageProxyPassesOnePutThenFailsAndRecovers(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		response.WriteHeader(http.StatusNoContent)
	}))
	defer upstream.Close()
	proxy, err := startOutageProxy("127.0.0.1:0", upstream.URL)
	require.NoError(t, err)
	defer proxy.Close(t.Context())
	endpoint := "http://" + proxy.listener.Addr().String()

	proxy.ArmAfterNextPut()
	request, err := http.NewRequestWithContext(t.Context(), http.MethodPut, endpoint, nil)
	require.NoError(t, err)
	response, err := http.DefaultClient.Do(request)
	require.NoError(t, err)
	require.NoError(t, response.Body.Close())
	require.Equal(t, http.StatusNoContent, response.StatusCode)
	require.True(t, proxy.Tripped())
	response, err = http.Get(endpoint)
	require.NoError(t, err)
	require.NoError(t, response.Body.Close())
	require.Equal(t, http.StatusServiceUnavailable, response.StatusCode)

	proxy.Recover()
	response, err = http.Get(endpoint)
	require.NoError(t, err)
	require.NoError(t, response.Body.Close())
	require.Equal(t, http.StatusNoContent, response.StatusCode)
	require.Equal(t, int64(1), proxy.Snapshot().ForwardedPUTs)
}

func TestOutageProxyCanRestoreFailAllAfterProcessRestart(t *testing.T) {
	var upstreamRequests atomic.Int64
	upstream := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		upstreamRequests.Add(1)
		response.WriteHeader(http.StatusNoContent)
	}))
	defer upstream.Close()
	proxy, err := startOutageProxy("127.0.0.1:0", upstream.URL)
	require.NoError(t, err)
	defer proxy.Close(t.Context())
	endpoint := "http://" + proxy.listener.Addr().String()

	proxy.FailAll()
	response, err := http.Get(endpoint)
	require.NoError(t, err)
	require.NoError(t, response.Body.Close())
	require.Equal(t, http.StatusServiceUnavailable, response.StatusCode)
	require.Zero(t, upstreamRequests.Load())
	proxy.Recover()
	response, err = http.Get(endpoint)
	require.NoError(t, err)
	require.NoError(t, response.Body.Close())
	require.Equal(t, http.StatusNoContent, response.StatusCode)
	require.Equal(t, int64(1), upstreamRequests.Load())
}

func TestMaterializerSoakConfigurationBindsGateWithoutSecrets(t *testing.T) {
	opts := options{
		databaseURL: "postgres://user:secret@database/soak", rustFSEndpoint: "http://rustfs",
		rustFSBucket: "bucket", accessKey: "access-secret", secretKey: "secret-secret",
		proxyListen: "172.16.100.2:19001", rustFSDataDir: "/var/lib/rustfs",
		duration: 24 * time.Hour, generations: 10_000, burstCount: 20,
		workerInterval: time.Second, sampleInterval: time.Minute,
		minPackBytes: 32 << 20, maxDelay: 5 * time.Minute,
		physicalByteLimit: 512 << 20, physicalFileLimit: 4096, databaseGrowthLimit: 512 << 20,
	}
	payload, err := json.Marshal(materializerSoakConfiguration(opts))
	require.NoError(t, err)
	require.NotContains(t, string(payload), "secret")
	require.NotContains(t, string(payload), "postgres://")
	require.Contains(t, string(payload), `"generations":10000`)
	require.Contains(t, string(payload), `"physical_file_limit":4096`)
}

func TestValidateMaterializerSoakCheckpointRequiresExactFaultProgress(t *testing.T) {
	opts := options{duration: 24 * time.Hour, generations: 10_000}
	state := soakCheckpoint{
		Version: materializerSoakStateVersion, Phase: materializerSoakPhaseActive,
		DatabaseBaseline: databaseSnapshot{DatabaseBytes: 1}, Fixture: &fixtureCheckpoint{},
		FaultPhase: materializerFaultPending,
	}
	require.NoError(t, validateMaterializerSoakCheckpoint(state, opts))
	state.FaultPhase = materializerFaultTripped
	require.ErrorContains(t, validateMaterializerSoakCheckpoint(state, opts), "fault checkpoint")
	state.ExpectedWorkerErrors = 1
	require.ErrorContains(t, validateMaterializerSoakCheckpoint(state, opts), "fault batch identity")
	state.FaultBatchID = "batch-a"
	require.NoError(t, validateMaterializerSoakCheckpoint(state, opts))
	state.Phase = materializerSoakPhasePassed
	require.ErrorContains(t, validateMaterializerSoakCheckpoint(state, opts), "passed")
}

func TestEvaluateFinalBoundsAcceptsBoundedRun(t *testing.T) {
	opts := options{
		duration: 10 * time.Second, generations: 10, maxDelay: time.Second,
		physicalByteLimit: 1_000, physicalFileLimit: 100, databaseGrowthLimit: 1_000,
	}
	baselineDB := databaseSnapshot{DatabaseBytes: 100}
	finalDB := databaseSnapshot{
		MaterializedGenerations: 11, CatalogObjects: 5, DatabaseBytes: 200,
	}
	violations := evaluateFinalBounds(opts, time.Minute,
		counters{Generated: 10, Materialized: 10, RetainedBatches: 2, ExpectedWorkerErrors: 2}, true,
		baselineDB, directorySnapshot{Files: 1, Bytes: 1}, finalDB,
		objectSnapshot{Objects: 6}, directorySnapshot{Files: 5, Bytes: 500})
	require.Empty(t, violations)
}

func TestEvaluateFinalBoundsUsesForcedFlushBoundAfterTerminalBatchPurge(t *testing.T) {
	opts := options{
		duration: 24 * time.Hour, generations: 10_000, maxDelay: 5 * time.Minute,
		physicalByteLimit: 512 << 20, physicalFileLimit: 4_096, databaseGrowthLimit: 512 << 20,
	}
	baselineDB := databaseSnapshot{DatabaseBytes: 10_255_719}
	finalDB := databaseSnapshot{
		MaterializedGenerations: 10_001, CatalogObjects: 515, DatabaseBytes: 93_461_863,
	}
	state := counters{
		Generated: 10_000, Materialized: 10_000, RetainedBatches: 254, ExpectedWorkerErrors: 2,
	}
	violations := evaluateFinalBounds(opts, 24*time.Hour, state, true,
		baselineDB, directorySnapshot{Files: 14, Bytes: 13_837}, finalDB,
		objectSnapshot{Objects: 516}, directorySnapshot{Files: 550, Bytes: 42_503_320})
	require.Empty(t, violations)

	tooManyObjects := objectSnapshot{Objects: materializerAcceptanceBounds(opts).MaxObjects + 1}
	finalDB.CatalogObjects = tooManyObjects.Objects - 1
	violations = evaluateFinalBounds(opts, 24*time.Hour, state, true,
		baselineDB, directorySnapshot{Files: 14, Bytes: 13_837}, finalDB,
		tooManyObjects, directorySnapshot{Files: 550, Bytes: 42_503_320})
	require.Contains(t, violations, "RustFS object count=585 exceeds batch bound=584")
}
