package clickhouse

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/sandboxobservability"
)

func TestNormalizeRuntimeSeriesQueryDefaultsAndUpsizesStep(t *testing.T) {
	now := time.Date(2026, 7, 1, 12, 0, 0, 0, time.FixedZone("offset", 8*60*60))
	query, selected, err := normalizeRuntimeSeriesQuery(sandboxobservability.RuntimeSeriesQuery{
		TeamID:    " team-1 ",
		SandboxID: " sb-1 ",
	}, now)
	if err != nil {
		t.Fatalf("normalizeRuntimeSeriesQuery() error = %v", err)
	}
	if query.TeamID != "team-1" || query.SandboxID != "sb-1" {
		t.Fatalf("normalized identity = %q/%q", query.TeamID, query.SandboxID)
	}
	if !query.EndTime.Equal(now.UTC()) || !query.StartTime.Equal(now.UTC().Add(-time.Hour)) {
		t.Fatalf("normalized window = %s..%s", query.StartTime, query.EndTime)
	}
	if query.Step != 15*time.Second || query.MaxPoints != 240 || query.Statistic != sandboxobservability.RuntimeMetricStatisticAuto {
		t.Fatalf("normalized query = %+v", query)
	}
	wantMetrics := []sandboxobservability.RuntimeMetricName{
		sandboxobservability.RuntimeMetricCPUUtilization,
		sandboxobservability.RuntimeMetricMemoryUtilization,
		sandboxobservability.RuntimeMetricNetworkIO,
	}
	if len(selected) != len(wantMetrics) {
		t.Fatalf("selected count = %d, want %d", len(selected), len(wantMetrics))
	}
	for i, want := range wantMetrics {
		if selected[i].Name != want {
			t.Fatalf("selected[%d] = %q, want %q", i, selected[i].Name, want)
		}
	}

	query, _, err = normalizeRuntimeSeriesQuery(sandboxobservability.RuntimeSeriesQuery{
		TeamID:    "team-1",
		SandboxID: "sb-1",
		StartTime: now.Add(-time.Hour),
		EndTime:   now,
		Step:      time.Second,
		MaxPoints: 10,
	}, now)
	if err != nil {
		t.Fatalf("normalizeRuntimeSeriesQuery() with max points error = %v", err)
	}
	if query.Step != 10*time.Minute {
		t.Fatalf("effective step = %s, want 10m", query.Step)
	}
}

func TestNormalizeRuntimeSeriesQueryRejectsInvalidRangeAndRateGauge(t *testing.T) {
	now := time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)
	_, _, err := normalizeRuntimeSeriesQuery(sandboxobservability.RuntimeSeriesQuery{
		TeamID:    "team-1",
		SandboxID: "sb-1",
		StartTime: now.Add(-31 * 24 * time.Hour),
		EndTime:   now,
	}, now)
	if !errors.Is(err, sandboxobservability.ErrInvalidQuery) {
		t.Fatalf("range error = %v, want ErrInvalidQuery", err)
	}

	_, _, err = normalizeRuntimeSeriesQuery(sandboxobservability.RuntimeSeriesQuery{
		TeamID:    "team-1",
		SandboxID: "sb-1",
		Metrics:   []sandboxobservability.RuntimeMetricName{sandboxobservability.RuntimeMetricCPUUtilization},
		Statistic: sandboxobservability.RuntimeMetricStatisticRate,
	}, now)
	if !errors.Is(err, sandboxobservability.ErrInvalidQuery) {
		t.Fatalf("rate gauge error = %v, want ErrInvalidQuery", err)
	}
}

func TestNormalizeConfigDefaultsAndValidatesRuntimeQueryBounds(t *testing.T) {
	cfg, err := normalizeConfig(Config{})
	if err != nil {
		t.Fatalf("normalizeConfig() error = %v", err)
	}
	if cfg.RuntimeQueryConcurrency != DefaultRuntimeQueryConcurrency || cfg.RuntimeQueryTimeout != DefaultRuntimeQueryTimeout {
		t.Fatalf("runtime query defaults = %d/%s, want %d/%s", cfg.RuntimeQueryConcurrency, cfg.RuntimeQueryTimeout, DefaultRuntimeQueryConcurrency, DefaultRuntimeQueryTimeout)
	}
	if _, err := normalizeConfig(Config{RuntimeQueryConcurrency: -1}); err == nil {
		t.Fatal("negative runtime query concurrency was accepted")
	}
	if _, err := normalizeConfig(Config{RuntimeQueryConcurrency: MaxRuntimeQueryConcurrency + 1}); err == nil {
		t.Fatal("excessive runtime query concurrency was accepted")
	}
	if _, err := normalizeConfig(Config{RuntimeQueryTimeout: -time.Second}); err == nil {
		t.Fatal("negative runtime query timeout was accepted")
	}
}

func TestBuildRuntimeSeriesResultCalculatesCounterRate(t *testing.T) {
	start := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	query, selected := runtimeTestQuery(t, start, start.Add(30*time.Second), sandboxobservability.RuntimeMetricNetworkIO)
	samples := []sandboxobservability.RuntimeSample{
		runtimeNetworkSample(start, 1, "epoch-a", 100, 40),
		runtimeNetworkSample(start.Add(15*time.Second), 1, "epoch-a", 250, 100),
		runtimeNetworkSample(start.Add(30*time.Second), 1, "epoch-a", 400, 160),
	}

	result := buildRuntimeSeriesResult(query, selected, samples, false)
	if len(result.Series) != 2 {
		t.Fatalf("series count = %d, want receive and transmit", len(result.Series))
	}
	for _, series := range result.Series {
		if series.Statistic != sandboxobservability.RuntimeMetricStatisticRate || series.Unit != sandboxobservability.RuntimeMetricUnitBytesPerSecond {
			t.Fatalf("series metadata = %+v", series)
		}
		if len(series.Segments) != 1 || len(series.Segments[0].Points) != 1 {
			t.Fatalf("segments = %#v, want one endpoint-folded point", series.Segments)
		}
		want := 10.0
		if series.Dimensions["direction"] == string(sandboxobservability.RuntimeMetricDirectionTransmit) {
			want = 4
		}
		for _, point := range series.Segments[0].Points {
			if math.Abs(point.Value-want) > 1e-9 {
				t.Fatalf("%s rate = %f, want %f", series.Dimensions["direction"], point.Value, want)
			}
		}
	}
	if result.Freshness.Status != sandboxobservability.RuntimeSeriesFreshnessFresh || result.Partial {
		t.Fatalf("freshness/partial = %+v/%v", result.Freshness, result.Partial)
	}
}

func TestBuildRuntimeSeriesResultDoesNotRateAcrossReset(t *testing.T) {
	start := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	query, selected := runtimeTestQuery(t, start, start.Add(45*time.Second), sandboxobservability.RuntimeMetricCPUTime)
	samples := []sandboxobservability.RuntimeSample{
		runtimeCPUTimeSample(start, 1, "epoch-a", 100),
		runtimeCPUTimeSample(start.Add(15*time.Second), 1, "epoch-a", 250),
		runtimeCPUTimeSample(start.Add(30*time.Second), 1, "epoch-a", 10),
		runtimeCPUTimeSample(start.Add(45*time.Second), 1, "epoch-a", 160),
	}

	result := buildRuntimeSeriesResult(query, selected, samples, false)
	series := result.Series[0]
	if series.Unit != sandboxobservability.RuntimeMetricUnitCores || len(series.Segments) != 2 {
		t.Fatalf("series = %+v, want cores in two reset-separated segments", series)
	}
	if len(series.Segments[0].Points) != 1 || len(series.Segments[1].Points) != 1 {
		t.Fatalf("segments = %#v, want one valid rate on each side of reset", series.Segments)
	}
	for _, segment := range series.Segments {
		if got := segment.Points[0].Value; got != 10 {
			t.Fatalf("rate = %f, want 10", got)
		}
	}
	if !hasRuntimeGap(result.Gaps, sandboxobservability.RuntimeMetricCPUTime, sandboxobservability.RuntimeSeriesGapSeriesReset) {
		t.Fatalf("gaps = %#v, want series_reset", result.Gaps)
	}
}

func TestBuildRuntimeSeriesResultRejectsLongCounterDelta(t *testing.T) {
	start := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	query, selected := runtimeTestQuery(t, start, start.Add(time.Minute), sandboxobservability.RuntimeMetricCPUTime)
	samples := []sandboxobservability.RuntimeSample{
		runtimeCPUTimeSample(start, 1, "epoch-a", 0),
		runtimeCPUTimeSample(start.Add(time.Minute), 1, "epoch-a", 60),
	}

	result := buildRuntimeSeriesResult(query, selected, samples, false)
	if len(result.Series[0].Segments) != 0 {
		t.Fatalf("segments = %#v, want no rate for interval over 45s", result.Series[0].Segments)
	}
	if !hasRuntimeGap(result.Gaps, sandboxobservability.RuntimeMetricCPUTime, sandboxobservability.RuntimeSeriesGapNoData) {
		t.Fatalf("gaps = %#v, want no_data", result.Gaps)
	}
}

func TestBuildRuntimeSeriesResultKeepsGaugeResetOutOfSameBucket(t *testing.T) {
	start := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	query, selected := runtimeTestQuery(t, start, start.Add(30*time.Second), sandboxobservability.RuntimeMetricCPUUtilization)
	query.Step = 30 * time.Second
	samples := []sandboxobservability.RuntimeSample{
		runtimeCPUUtilizationSample(start, 1, "epoch-a", 0.2),
		runtimeCPUUtilizationSample(start.Add(10*time.Second), 2, "epoch-b", 0.8),
	}

	result := buildRuntimeSeriesResult(query, selected, samples, false)
	segments := result.Series[0].Segments
	if len(segments) != 2 || len(segments[0].Points) != 1 || len(segments[1].Points) != 1 {
		t.Fatalf("segments = %#v, want identities kept separate", segments)
	}
	if segments[0].Points[0].Value != 0.2 || segments[1].Points[0].Value != 0.8 {
		t.Fatalf("values = %#v, want no cross-generation average", segments)
	}
	if !hasRuntimeGap(result.Gaps, sandboxobservability.RuntimeMetricCPUUtilization, sandboxobservability.RuntimeSeriesGapSeriesReset) {
		t.Fatalf("gaps = %#v, want series_reset", result.Gaps)
	}
}

func TestBuildRuntimeSeriesResultReportsMissingWithoutZeroFill(t *testing.T) {
	start := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	end := start.Add(30 * time.Second)
	query, selected := runtimeTestQuery(t, start, end, sandboxobservability.RuntimeMetricRootFSWritableUsage)
	samples := []sandboxobservability.RuntimeSample{}
	// Query loaders scan newest-first to retain the latest rows under the raw
	// safety cap; missing ranges must merge correctly in that order.
	for _, observedAt := range []time.Time{end, start.Add(15 * time.Second)} {
		samples = append(samples, sandboxobservability.RuntimeSample{
			ObservedAt:        observedAt,
			RuntimeGeneration: 1,
			SeriesEpoch:       "epoch-a",
			Missing: []sandboxobservability.RuntimeMetricMissing{{
				Metric: sandboxobservability.RuntimeMetricRootFSWritableUsage,
				Reason: sandboxobservability.RuntimeMetricMissingUnsupported,
			}},
		})
	}

	result := buildRuntimeSeriesResult(query, selected, samples, false)
	if len(result.Series[0].Segments) != 0 {
		t.Fatalf("segments = %#v, want no synthesized zero", result.Series[0].Segments)
	}
	unsupported := runtimeGaps(result.Gaps, sandboxobservability.RuntimeMetricRootFSWritableUsage, sandboxobservability.RuntimeSeriesGapUnsupported)
	if len(unsupported) != 1 || !unsupported[0].StartTime.Equal(start) || !unsupported[0].EndTime.Equal(end) {
		t.Fatalf("gaps = %#v, want unsupported", result.Gaps)
	}
	if !result.Partial {
		t.Fatal("partial = false, want true for unavailable series")
	}
}

func TestBuildRuntimeSeriesResultMarksRawTruncation(t *testing.T) {
	start := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	end := start.Add(time.Minute)
	query, selected := runtimeTestQuery(t, start, end, sandboxobservability.RuntimeMetricCPUUtilization)
	samples := []sandboxobservability.RuntimeSample{
		runtimeCPUUtilizationSample(start.Add(30*time.Second), 1, "epoch-a", 0.3),
		runtimeCPUUtilizationSample(start.Add(45*time.Second), 1, "epoch-a", 0.4),
		runtimeCPUUtilizationSample(end, 1, "epoch-a", 0.5),
	}

	result := buildRuntimeSeriesResult(query, selected, samples, true)
	if !result.Partial {
		t.Fatal("partial = false, want true for raw truncation")
	}
	noData := runtimeGaps(result.Gaps, sandboxobservability.RuntimeMetricCPUUtilization, sandboxobservability.RuntimeSeriesGapNoData)
	if len(noData) == 0 || !noData[0].StartTime.Equal(start) || noData[0].EndTime.Before(start.Add(30*time.Second)) {
		t.Fatalf("gaps = %#v, want truncated leading range", result.Gaps)
	}
}

func TestBuildRuntimeSeriesResultHonorsMaxPointsForInclusiveWindow(t *testing.T) {
	start := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	end := start.Add(time.Hour)
	query, selected := runtimeTestQuery(t, start, end, sandboxobservability.RuntimeMetricCPUUtilization)
	query.MaxPoints = 240
	samples := make([]sandboxobservability.RuntimeSample, 0, 241)
	for observedAt := start; !observedAt.After(end); observedAt = observedAt.Add(15 * time.Second) {
		samples = append(samples, runtimeCPUUtilizationSample(observedAt, 1, "epoch-a", 0.5))
	}

	result := buildRuntimeSeriesResult(query, selected, samples, false)
	if count := runtimePointCount(result.Series[0]); count != 240 {
		t.Fatalf("point count = %d, want 240 for inclusive one-hour window", count)
	}
	if result.Partial {
		t.Fatalf("partial = true for fully represented window: gaps=%#v", result.Gaps)
	}
}

func TestBuildRuntimeSeriesResultCapsResetDuplicatesAndKeepsRatePredecessor(t *testing.T) {
	start := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	end := start.Add(15 * time.Second)
	query, selected := runtimeTestQuery(t, start, end, sandboxobservability.RuntimeMetricCPUUtilization)
	query.MaxPoints = 1
	samples := []sandboxobservability.RuntimeSample{
		runtimeCPUUtilizationSample(start, 1, "epoch-a", 0.2),
		runtimeCPUUtilizationSample(start.Add(5*time.Second), 2, "epoch-b", 0.4),
		runtimeCPUUtilizationSample(end, 3, "epoch-c", 0.6),
	}
	result := buildRuntimeSeriesResult(query, selected, samples, false)
	if count := runtimePointCount(result.Series[0]); count != 1 {
		t.Fatalf("point count = %d, want max_points=1", count)
	}
	if !result.Partial || !hasRuntimeGap(result.Gaps, sandboxobservability.RuntimeMetricCPUUtilization, sandboxobservability.RuntimeSeriesGapNoData) {
		t.Fatalf("partial/gaps = %v/%#v, want explicit point-limit gap", result.Partial, result.Gaps)
	}

	rateStart := start.Add(15 * time.Second)
	rateEnd := rateStart.Add(15 * time.Second)
	rateQuery, rateSelected := runtimeTestQuery(t, rateStart, rateEnd, sandboxobservability.RuntimeMetricCPUTime)
	rateQuery.MaxPoints = 1
	rateSamples := []sandboxobservability.RuntimeSample{
		runtimeCPUTimeSample(start, 1, "epoch-a", 0),
		runtimeCPUTimeSample(rateStart, 1, "epoch-a", 150),
		runtimeCPUTimeSample(rateEnd, 1, "epoch-a", 300),
	}
	rateResult := buildRuntimeSeriesResult(rateQuery, rateSelected, rateSamples, false)
	if count := runtimePointCount(rateResult.Series[0]); count != 1 {
		t.Fatalf("rate point count = %d, want 1", count)
	}
	if got := rateResult.Series[0].Segments[0].Points[0].Value; got != 10 {
		t.Fatalf("rate = %f, want 10 using predecessor before start_time", got)
	}
}

func TestRuntimeFreshnessUsesRequestedHistoricalEnd(t *testing.T) {
	end := time.Date(2020, 1, 1, 1, 0, 0, 0, time.UTC)
	newest := end.Add(-10 * time.Second)
	freshness := runtimeFreshness(&newest, end, 15*time.Second)
	if freshness.Status != sandboxobservability.RuntimeSeriesFreshnessFresh || freshness.AgeSeconds == nil || *freshness.AgeSeconds != 10 {
		t.Fatalf("freshness = %+v, want fresh age 10", freshness)
	}
}

func TestNormalizeRuntimeSampleForInsertValidatesLeafValuesAndDimensions(t *testing.T) {
	now := time.Date(2026, 7, 1, 0, 0, 15, 0, time.UTC)
	base := sandboxobservability.RuntimeSample{
		TeamID:            " team-1 ",
		SandboxID:         " sb-1 ",
		RuntimeGeneration: 1,
		SeriesEpoch:       " epoch-a ",
		ObservedAt:        now.Add(-time.Second),
		SampleID:          " sample-1 ",
	}
	base.IngestedAt = now.Add(24 * time.Hour)

	empty := base
	empty.CPU = &sandboxobservability.RuntimeCPUValues{}
	if _, err := normalizeRuntimeSampleForInsert(empty, now); err == nil {
		t.Fatal("empty value group was accepted")
	}

	invalidDimension := base
	invalidDimension.Missing = []sandboxobservability.RuntimeMetricMissing{{
		Metric:     sandboxobservability.RuntimeMetricCPUUtilization,
		Reason:     sandboxobservability.RuntimeMetricMissingUnavailable,
		Dimensions: map[string]string{"direction": "receive"},
	}}
	if _, err := normalizeRuntimeSampleForInsert(invalidDimension, now); err == nil || !strings.Contains(err.Error(), "unknown dimension") {
		t.Fatalf("invalid dimension error = %v", err)
	}

	tooLongEpoch := base
	tooLongEpoch.SeriesEpoch = strings.Repeat("e", maxRuntimeSeriesEpoch+1)
	if _, err := normalizeRuntimeSampleForInsert(tooLongEpoch, now); err == nil || !strings.Contains(err.Error(), "series_epoch cannot exceed") {
		t.Fatalf("long epoch error = %v", err)
	}
	tooLongSampleID := base
	tooLongSampleID.SampleID = strings.Repeat("s", maxRuntimeSampleID+1)
	if _, err := normalizeRuntimeSampleForInsert(tooLongSampleID, now); err == nil || !strings.Contains(err.Error(), "sample_id cannot exceed") {
		t.Fatalf("long sample ID error = %v", err)
	}

	accepted := base
	accepted.Missing = []sandboxobservability.RuntimeMetricMissing{{
		Metric: sandboxobservability.RuntimeMetricNetworkIO,
		Reason: sandboxobservability.RuntimeMetricMissingUnsupported,
	}}
	normalized, err := normalizeRuntimeSampleForInsert(accepted, now)
	if err != nil {
		t.Fatalf("normalizeRuntimeSampleForInsert() error = %v", err)
	}
	if normalized.TeamID != "team-1" || normalized.SandboxID != "sb-1" || normalized.SeriesEpoch != "epoch-a" || normalized.SampleID != "sample-1" {
		t.Fatalf("normalized sample = %+v", normalized)
	}
	if !normalized.IngestedAt.Equal(now) {
		t.Fatalf("ingested_at = %s, want storage time %s", normalized.IngestedAt, now)
	}
}

func TestBuildRuntimeMetricQueryPrunesColumnsAndKeepsCounterPredecessor(t *testing.T) {
	repo, _ := mustRepository(t)
	start := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	query := sandboxobservability.RuntimeSeriesQuery{
		TeamID:    "team-1",
		SandboxID: "sb-1",
		StartTime: start,
		EndTime:   start.Add(time.Hour),
		Step:      15 * time.Second,
	}
	descriptor, _ := sandboxobservability.RuntimeMetricDescriptorFor(sandboxobservability.RuntimeMetricCPUTime)
	sqlQuery, args := repo.buildRuntimeMetricQuery(query, descriptor, 200001)
	for _, want := range []string{
		"SELECT runtime_generation, series_epoch, observed_at, cpu_time_seconds, missing",
		"FROM `sandbox0_observability`.`sandbox_runtime_samples` FINAL",
		"ORDER BY observed_at DESC, runtime_generation DESC, series_epoch DESC, sample_id DESC",
		"LIMIT 200001",
	} {
		if !strings.Contains(sqlQuery, want) {
			t.Fatalf("query missing %q:\n%s", want, sqlQuery)
		}
	}
	if got, ok := args[2].(time.Time); !ok || !got.Equal(start.Add(-maximumCounterDelta)) {
		t.Fatalf("query start arg = %#v, want preceding sample time", args[2])
	}
	selectClause := strings.SplitN(sqlQuery, " FROM ", 2)[0]
	for _, excluded := range []string{"cpu_utilization", "cpu_usage", "memory_", "network_", "process_count", "rootfs_"} {
		if strings.Contains(selectClause, excluded) {
			t.Fatalf("query selected unrelated column %q:\n%s", excluded, sqlQuery)
		}
	}
	if runtimeRawSampleLimit() != maxRuntimeRawSamples {
		t.Fatalf("raw limit = %d, want %d", runtimeRawSampleLimit(), maxRuntimeRawSamples)
	}
	if capacity := runtimeRawPointCapacity(query, maxRuntimeRawSamples); capacity != 244 {
		t.Fatalf("one-hour raw point capacity = %d, want 244", capacity)
	}

	networkDescriptor, _ := sandboxobservability.RuntimeMetricDescriptorFor(sandboxobservability.RuntimeMetricNetworkIO)
	networkQuery, _ := repo.buildRuntimeMetricQuery(query, networkDescriptor, 200001)
	networkSelect := strings.SplitN(networkQuery, " FROM ", 2)[0]
	for _, included := range []string{"network_receive_bytes", "network_transmit_bytes"} {
		if !strings.Contains(networkSelect, included) {
			t.Fatalf("network query missing %q:\n%s", included, networkQuery)
		}
	}
	for _, excluded := range []string{"network_receive_errors", "cpu_", "memory_"} {
		if strings.Contains(networkSelect, excluded) {
			t.Fatalf("network query selected unrelated column %q:\n%s", excluded, networkQuery)
		}
	}
}

func TestRuntimeMetricColumnsCoverCatalog(t *testing.T) {
	for _, descriptor := range sandboxobservability.RuntimeMetricCatalogSnapshot().Metrics {
		columns := runtimeMetricColumns(descriptor.Name)
		if len(columns) == 0 {
			t.Fatalf("metric %q has no query projection", descriptor.Name)
		}
		wantColumns := 1
		if descriptor.Name == sandboxobservability.RuntimeMetricNetworkIO || descriptor.Name == sandboxobservability.RuntimeMetricNetworkErrors {
			wantColumns = 2
		}
		if len(columns) != wantColumns {
			t.Fatalf("metric %q column count = %d, want %d", descriptor.Name, len(columns), wantColumns)
		}
		inputs := initializeRuntimeSeriesInputs([]sandboxobservability.RuntimeMetricDescriptor{descriptor})
		for _, column := range columns {
			if inputs[column.Key] == nil {
				t.Fatalf("metric %q column %q has no matching series input", descriptor.Name, column.Name)
			}
		}
	}
}

func TestRuntimeMetricQueryKeepsThirtyDayRawLimitForSmallMaxPoints(t *testing.T) {
	repo, _ := mustRepository(t)
	end := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	start := end.Add(-30 * 24 * time.Hour)
	descriptor, _ := sandboxobservability.RuntimeMetricDescriptorFor(sandboxobservability.RuntimeMetricCPUUtilization)
	for _, maxPoints := range []int{1, 10} {
		t.Run(fmt.Sprintf("max_points_%d", maxPoints), func(t *testing.T) {
			normalized, _, err := normalizeRuntimeSeriesQuery(sandboxobservability.RuntimeSeriesQuery{
				TeamID:    "team-1",
				SandboxID: "sb-1",
				StartTime: start,
				EndTime:   end,
				Metrics:   []sandboxobservability.RuntimeMetricName{descriptor.Name},
				MaxPoints: maxPoints,
			}, end)
			if err != nil {
				t.Fatalf("normalizeRuntimeSeriesQuery() error = %v", err)
			}
			rawLimit := runtimeRawSampleLimit()
			sqlQuery, args := repo.buildRuntimeMetricQuery(normalized, descriptor, rawLimit+1)
			if !strings.Contains(sqlQuery, "LIMIT 200001") {
				t.Fatalf("query limit depends on max_points=%d:\n%s", maxPoints, sqlQuery)
			}
			if got, ok := args[2].(time.Time); !ok || !got.Equal(start.Add(-maximumCounterDelta)) {
				t.Fatalf("query start arg = %#v, want 30-day start plus predecessor", args[2])
			}
			if capacity := runtimeRawPointCapacity(normalized, rawLimit); capacity != 172804 {
				t.Fatalf("raw capacity = %d, want 172804", capacity)
			}
		})
	}
}

func TestListRuntimeSeriesBoundsQueryConcurrencyAndCancelsWaiter(t *testing.T) {
	repo, err := NewRepository(&captureDB{}, Config{
		RuntimeQueryConcurrency: 1,
		RuntimeQueryTimeout:     time.Second,
	})
	if err != nil {
		t.Fatalf("NewRepository() error = %v", err)
	}
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	var calls atomic.Int32
	var active atomic.Int32
	var maximum atomic.Int32
	repo.loadRuntimeMetric = func(ctx context.Context, _ sandboxobservability.RuntimeSeriesQuery, descriptor sandboxobservability.RuntimeMetricDescriptor, _ int) (runtimeMetricLoad, error) {
		calls.Add(1)
		current := active.Add(1)
		defer active.Add(-1)
		for {
			observed := maximum.Load()
			if current <= observed || maximum.CompareAndSwap(observed, current) {
				break
			}
		}
		entered <- struct{}{}
		select {
		case <-release:
			return runtimeMetricLoad{Inputs: initializeRuntimeSeriesInputs([]sandboxobservability.RuntimeMetricDescriptor{descriptor})}, nil
		case <-ctx.Done():
			return runtimeMetricLoad{}, ctx.Err()
		}
	}

	query := sandboxobservability.RuntimeSeriesQuery{
		TeamID:    "team-1",
		SandboxID: "sb-1",
		Metrics:   []sandboxobservability.RuntimeMetricName{sandboxobservability.RuntimeMetricCPUUtilization},
	}
	firstResult := make(chan error, 1)
	go func() {
		_, listErr := repo.ListRuntimeSeries(context.Background(), query)
		firstResult <- listErr
	}()
	<-entered

	waitContext, cancel := context.WithCancel(context.Background())
	secondResult := make(chan error, 1)
	go func() {
		_, listErr := repo.ListRuntimeSeries(waitContext, query)
		secondResult <- listErr
	}()
	cancel()
	if err := <-secondResult; !errors.Is(err, context.Canceled) || !errors.Is(err, sandboxobservability.ErrBackendUnavailable) {
		t.Fatalf("waiting query error = %v, want context.Canceled and ErrBackendUnavailable", err)
	}
	if calls.Load() != 1 || maximum.Load() != 1 {
		t.Fatalf("loader calls/max active = %d/%d, want 1/1", calls.Load(), maximum.Load())
	}
	close(release)
	if err := <-firstResult; err != nil {
		t.Fatalf("first ListRuntimeSeries() error = %v", err)
	}
}

func TestListRuntimeSeriesQueryTimeoutCancelsLoader(t *testing.T) {
	repo, err := NewRepository(&captureDB{}, Config{
		RuntimeQueryConcurrency: 1,
		RuntimeQueryTimeout:     20 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewRepository() error = %v", err)
	}
	repo.loadRuntimeMetric = func(ctx context.Context, _ sandboxobservability.RuntimeSeriesQuery, _ sandboxobservability.RuntimeMetricDescriptor, _ int) (runtimeMetricLoad, error) {
		<-ctx.Done()
		return runtimeMetricLoad{}, ctx.Err()
	}
	_, err = repo.ListRuntimeSeries(context.Background(), sandboxobservability.RuntimeSeriesQuery{
		TeamID:    "team-1",
		SandboxID: "sb-1",
		Metrics:   []sandboxobservability.RuntimeMetricName{sandboxobservability.RuntimeMetricCPUUtilization},
	})
	if !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, sandboxobservability.ErrBackendUnavailable) {
		t.Fatalf("ListRuntimeSeries() error = %v, want context.DeadlineExceeded and ErrBackendUnavailable", err)
	}
}

func TestAggregateRuntimePointsBoundsThirtyDaySeries(t *testing.T) {
	const rowCount = 172800
	if rowCount >= runtimeRawSampleLimit() {
		t.Fatalf("30-day row count %d is not covered by raw limit %d", rowCount, runtimeRawSampleLimit())
	}
	start := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	query, selected := runtimeTestQuery(t, start, start.Add(30*24*time.Hour), sandboxobservability.RuntimeMetricCPUUtilization)
	query.Step = 2 * time.Hour
	query.MaxPoints = 360
	if capacity := runtimeRawPointCapacity(query, maxRuntimeRawSamples); capacity != rowCount+4 {
		t.Fatalf("30-day raw point capacity = %d, want %d", capacity, rowCount+4)
	}
	raw := makeRuntimeGaugePoints(start, rowCount)

	points, gaps := aggregateRuntimePoints(raw, selected[0], sandboxobservability.RuntimeMetricStatisticAverage, nil, query)
	if len(points) != 360 {
		t.Fatalf("aggregated point count = %d, want 360", len(points))
	}
	if len(gaps) != 0 {
		t.Fatalf("gaps = %#v, want none", gaps)
	}
}

func BenchmarkAggregateRuntimePoints172800Rows(b *testing.B) {
	const rowCount = 172800
	start := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	descriptor, _ := sandboxobservability.RuntimeMetricDescriptorFor(sandboxobservability.RuntimeMetricCPUUtilization)
	query := sandboxobservability.RuntimeSeriesQuery{
		StartTime: start,
		EndTime:   start.Add(30 * 24 * time.Hour),
		Step:      2 * time.Hour,
		MaxPoints: 360,
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		raw := makeRuntimeGaugePoints(start, rowCount)
		points, _ := aggregateRuntimePoints(raw, descriptor, sandboxobservability.RuntimeMetricStatisticAverage, nil, query)
		if len(points) != 360 {
			b.Fatalf("aggregated point count = %d, want 360", len(points))
		}
	}
}

func makeRuntimeGaugePoints(start time.Time, count int) []rawRuntimePoint {
	points := make([]rawRuntimePoint, count)
	for i := range points {
		points[i] = rawRuntimePoint{
			Time:              start.Add(time.Duration(i) * 15 * time.Second),
			Value:             0.5,
			RuntimeGeneration: 1,
			SeriesEpoch:       "epoch-a",
		}
	}
	return points
}

func runtimeTestQuery(t *testing.T, start, end time.Time, metric sandboxobservability.RuntimeMetricName) (sandboxobservability.RuntimeSeriesQuery, []sandboxobservability.RuntimeMetricDescriptor) {
	t.Helper()
	descriptor, ok := sandboxobservability.RuntimeMetricDescriptorFor(metric)
	if !ok {
		t.Fatalf("unknown test metric %q", metric)
	}
	return sandboxobservability.RuntimeSeriesQuery{
		TeamID:    "team-1",
		SandboxID: "sb-1",
		StartTime: start,
		EndTime:   end,
		Metrics:   []sandboxobservability.RuntimeMetricName{metric},
		Step:      15 * time.Second,
		Statistic: sandboxobservability.RuntimeMetricStatisticAuto,
		MaxPoints: 240,
	}, []sandboxobservability.RuntimeMetricDescriptor{descriptor}
}

func runtimeNetworkSample(observedAt time.Time, generation int64, epoch string, receive, transmit uint64) sandboxobservability.RuntimeSample {
	return sandboxobservability.RuntimeSample{
		ObservedAt:        observedAt,
		RuntimeGeneration: generation,
		SeriesEpoch:       epoch,
		Network: &sandboxobservability.RuntimeNetworkValues{
			ReceiveBytes:  &receive,
			TransmitBytes: &transmit,
		},
	}
}

func runtimeCPUTimeSample(observedAt time.Time, generation int64, epoch string, value float64) sandboxobservability.RuntimeSample {
	return sandboxobservability.RuntimeSample{
		ObservedAt:        observedAt,
		RuntimeGeneration: generation,
		SeriesEpoch:       epoch,
		CPU:               &sandboxobservability.RuntimeCPUValues{TimeSeconds: &value},
	}
}

func runtimeCPUUtilizationSample(observedAt time.Time, generation int64, epoch string, value float64) sandboxobservability.RuntimeSample {
	return sandboxobservability.RuntimeSample{
		ObservedAt:        observedAt,
		RuntimeGeneration: generation,
		SeriesEpoch:       epoch,
		CPU:               &sandboxobservability.RuntimeCPUValues{Utilization: &value},
	}
}

func hasRuntimeGap(gaps []sandboxobservability.RuntimeSeriesGap, metric sandboxobservability.RuntimeMetricName, reason sandboxobservability.RuntimeSeriesGapReason) bool {
	return len(runtimeGaps(gaps, metric, reason)) > 0
}

func runtimeGaps(gaps []sandboxobservability.RuntimeSeriesGap, metric sandboxobservability.RuntimeMetricName, reason sandboxobservability.RuntimeSeriesGapReason) []sandboxobservability.RuntimeSeriesGap {
	result := []sandboxobservability.RuntimeSeriesGap{}
	for _, gap := range gaps {
		if gap.Metric == metric && gap.Reason == reason {
			result = append(result, gap)
		}
	}
	return result
}

func runtimePointCount(series sandboxobservability.RuntimeSeries) int {
	count := 0
	for _, segment := range series.Segments {
		count += len(segment.Points)
	}
	return count
}
