package main

import (
	"net/http"
	"net/http/httptest"
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

func TestEvaluateFinalBoundsAcceptsBoundedRun(t *testing.T) {
	opts := options{
		duration: 10 * time.Second, generations: 10, maxDelay: time.Second,
		physicalByteLimit: 1_000, physicalFileLimit: 100, databaseGrowthLimit: 1_000,
	}
	baselineDB := databaseSnapshot{DatabaseBytes: 100}
	finalDB := databaseSnapshot{
		MaterializedGenerations: 11, CatalogObjects: 4, DatabaseBytes: 200,
	}
	violations := evaluateFinalBounds(opts, time.Now().Add(-time.Minute),
		counters{Generated: 10, Materialized: 10, Batches: 2, ExpectedWorkerErrors: 2}, true,
		baselineDB, directorySnapshot{Files: 1, Bytes: 1}, finalDB,
		objectSnapshot{Objects: 5}, directorySnapshot{Files: 5, Bytes: 500})
	require.Empty(t, violations)
}
