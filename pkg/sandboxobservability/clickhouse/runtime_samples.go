package clickhouse

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/sandboxobservability"
)

const (
	defaultRuntimeMaxPoints = 240
	maxRuntimeMaxPoints     = 1000
	defaultRuntimeWindow    = time.Hour
	minimumRuntimeStep      = sandboxobservability.DefaultRuntimeSampleInterval
	maximumCounterDelta     = 3 * sandboxobservability.DefaultRuntimeSampleInterval
	maxRuntimeQueryRange    = 30 * 24 * time.Hour
	maxRuntimeRawSamples    = 200000
	maxRuntimeSeriesEpoch   = 128
	maxRuntimeSampleID      = 256
)

type rawRuntimePoint struct {
	Time              time.Time
	Value             float64
	RuntimeGeneration int64
	SeriesEpoch       string
	ResetSequence     int64
}

type runtimeSeriesKey struct {
	Metric    sandboxobservability.RuntimeMetricName
	Direction sandboxobservability.RuntimeMetricDirection
}

type runtimeSeriesInput struct {
	Descriptor sandboxobservability.RuntimeMetricDescriptor
	Dimensions map[string]string
	Points     []rawRuntimePoint
}

type runtimeBucket struct {
	Time              time.Time
	Sum               float64
	Minimum           float64
	Maximum           float64
	Last              float64
	Count             int
	RuntimeGeneration int64
	SeriesEpoch       string
	ResetSequence     int64
}

type runtimeBucketKey struct {
	Index             int64
	RuntimeGeneration int64
	SeriesEpoch       string
	ResetSequence     int64
}

type aggregatedRuntimePoint struct {
	Time              time.Time
	Value             float64
	RuntimeGeneration int64
	SeriesEpoch       string
	ResetSequence     int64
}

type runtimeMetricColumn struct {
	Name     string
	Key      runtimeSeriesKey
	Unsigned bool
}

type runtimeMetricLoad struct {
	Inputs    map[runtimeSeriesKey]*runtimeSeriesInput
	Gaps      []sandboxobservability.RuntimeSeriesGap
	Newest    *time.Time
	Oldest    *time.Time
	Truncated bool
}

type runtimeMetricLoader func(context.Context, sandboxobservability.RuntimeSeriesQuery, sandboxobservability.RuntimeMetricDescriptor, int) (runtimeMetricLoad, error)

type runtimeMetricSQLValue struct {
	Float    sql.NullFloat64
	Unsigned sql.Null[uint64]
}

func (v *runtimeMetricSQLValue) destination(unsigned bool) any {
	if unsigned {
		return &v.Unsigned
	}
	return &v.Float
}

func (v *runtimeMetricSQLValue) value(unsigned bool) (float64, bool) {
	if unsigned {
		return float64(v.Unsigned.V), v.Unsigned.Valid
	}
	return v.Float.Float64, v.Float.Valid
}

func (r *Repository) InsertRuntimeSamples(ctx context.Context, samples []sandboxobservability.RuntimeSample) error {
	if len(samples) == 0 {
		return nil
	}

	normalized := make([]sandboxobservability.RuntimeSample, 0, len(samples))
	now := r.now()
	for i, sample := range samples {
		normalizedSample, err := normalizeRuntimeSampleForInsert(sample, now)
		if err != nil {
			return fmt.Errorf("runtime sample %d: %w", i, err)
		}
		normalized = append(normalized, normalizedSample)
	}

	for len(normalized) > 0 {
		chunkSize := len(normalized)
		if chunkSize > maxInsertBatchSize {
			chunkSize = maxInsertBatchSize
		}
		query, args, err := r.buildRuntimeSampleInsertSQL(normalized[:chunkSize])
		if err != nil {
			return err
		}
		if _, err := r.db.ExecContext(ctx, query, args...); err != nil {
			return fmt.Errorf("%w: insert runtime samples: %v", sandboxobservability.ErrBackendUnavailable, err)
		}
		normalized = normalized[chunkSize:]
	}
	return nil
}

func (r *Repository) ListRuntimeSeries(ctx context.Context, query sandboxobservability.RuntimeSeriesQuery) (*sandboxobservability.RuntimeSeriesResult, error) {
	normalized, selected, err := normalizeRuntimeSeriesQuery(query, r.now())
	if err != nil {
		return nil, err
	}

	queryContext, release, err := r.acquireRuntimeQuery(ctx)
	if err != nil {
		return nil, runtimeQueryContextError(err)
	}
	defer release()

	rawLimit := runtimeRawSampleLimit()
	state := newRuntimeResultState(normalized)
	for _, descriptor := range selected {
		loaded, loadErr := r.loadRuntimeMetric(queryContext, normalized, descriptor, rawLimit)
		if loadErr != nil {
			if contextErr := queryContext.Err(); contextErr != nil {
				return nil, runtimeQueryContextError(contextErr)
			}
			return nil, fmt.Errorf("%w: query runtime metric %s: %v", sandboxobservability.ErrBackendUnavailable, descriptor.Name, loadErr)
		}
		state.addLoad(descriptor, loaded)
		loaded = runtimeMetricLoad{}
	}
	return state.result(), nil
}

func runtimeQueryContextError(err error) error {
	return fmt.Errorf("%w: query runtime samples: %w", sandboxobservability.ErrBackendUnavailable, err)
}

func (r *Repository) acquireRuntimeQuery(ctx context.Context) (context.Context, func(), error) {
	queryContext, cancel := context.WithTimeout(ctx, r.cfg.RuntimeQueryTimeout)
	select {
	case r.runtimeQuerySlots <- struct{}{}:
		return queryContext, func() {
			<-r.runtimeQuerySlots
			cancel()
		}, nil
	case <-queryContext.Done():
		cancel()
		return nil, nil, queryContext.Err()
	}
}

func (r *Repository) buildRuntimeSampleInsertSQL(samples []sandboxobservability.RuntimeSample) (string, []any, error) {
	const columns = " (team_id, sandbox_id, region_id, cluster_id, runtime_generation, series_epoch, observed_at, ingested_at, sample_id, cpu_utilization, cpu_usage, cpu_time_seconds, cpu_limit_cores, memory_usage_bytes, memory_working_set_bytes, memory_available_bytes, memory_limit_bytes, memory_utilization, network_receive_bytes, network_transmit_bytes, network_receive_errors, network_transmit_errors, process_count, rootfs_writable_usage_bytes, rootfs_writable_inodes, missing) VALUES "

	var builder strings.Builder
	builder.WriteString("INSERT INTO ")
	builder.WriteString(r.runtimeSamplesTable)
	builder.WriteString(columns)

	args := make([]any, 0, len(samples)*26)
	for i, sample := range samples {
		if i > 0 {
			builder.WriteString(", ")
		}
		builder.WriteString("(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
		missing, err := json.Marshal(sample.Missing)
		if err != nil {
			return "", nil, fmt.Errorf("encode missing metrics: %w", err)
		}

		var cpu sandboxobservability.RuntimeCPUValues
		if sample.CPU != nil {
			cpu = *sample.CPU
		}
		var memory sandboxobservability.RuntimeMemoryValues
		if sample.Memory != nil {
			memory = *sample.Memory
		}
		var network sandboxobservability.RuntimeNetworkValues
		if sample.Network != nil {
			network = *sample.Network
		}
		var process sandboxobservability.RuntimeProcessValues
		if sample.Process != nil {
			process = *sample.Process
		}
		var rootfs sandboxobservability.RuntimeRootFSWritableValues
		if sample.RootFSWritable != nil {
			rootfs = *sample.RootFSWritable
		}

		args = append(args,
			sample.TeamID,
			sample.SandboxID,
			sample.RegionID,
			sample.ClusterID,
			sample.RuntimeGeneration,
			sample.SeriesEpoch,
			sample.ObservedAt.UTC(),
			sample.IngestedAt.UTC(),
			sample.SampleID,
			nullableFloat64(cpu.Utilization),
			nullableFloat64(cpu.Usage),
			nullableFloat64(cpu.TimeSeconds),
			nullableFloat64(cpu.LimitCores),
			nullableUint64(memory.UsageBytes),
			nullableUint64(memory.WorkingSetBytes),
			nullableUint64(memory.AvailableBytes),
			nullableUint64(memory.LimitBytes),
			nullableFloat64(memory.Utilization),
			nullableUint64(network.ReceiveBytes),
			nullableUint64(network.TransmitBytes),
			nullableUint64(network.ReceiveErrors),
			nullableUint64(network.TransmitErrors),
			nullableUint64(process.Count),
			nullableUint64(rootfs.UsageBytes),
			nullableUint64(rootfs.Inodes),
			string(missing),
		)
	}
	return builder.String(), args, nil
}

func (r *Repository) buildRuntimeMetricQuery(query sandboxobservability.RuntimeSeriesQuery, descriptor sandboxobservability.RuntimeMetricDescriptor, limit int) (string, []any) {
	columns := runtimeMetricColumns(descriptor.Name)
	// One collection tolerance before the requested range preserves the valid
	// counter predecessor without expanding a coarse query by an entire step.
	start := query.StartTime.Add(-maximumCounterDelta)
	var builder strings.Builder
	builder.WriteString("SELECT runtime_generation, series_epoch, observed_at")
	for _, column := range columns {
		builder.WriteString(", ")
		builder.WriteString(column.Name)
	}
	builder.WriteString(", missing")
	builder.WriteString(" FROM ")
	builder.WriteString(r.runtimeSamplesTable)
	builder.WriteString(" FINAL WHERE team_id = ? AND sandbox_id = ? AND observed_at >= ? AND observed_at <= ? ORDER BY observed_at DESC, runtime_generation DESC, series_epoch DESC, sample_id DESC")
	builder.WriteString(fmt.Sprintf(" LIMIT %d", limit))
	return builder.String(), []any{query.TeamID, query.SandboxID, start.UTC(), query.EndTime.UTC()}
}

func normalizeRuntimeSampleForInsert(sample sandboxobservability.RuntimeSample, now time.Time) (sandboxobservability.RuntimeSample, error) {
	sample.TeamID = strings.TrimSpace(sample.TeamID)
	sample.SandboxID = strings.TrimSpace(sample.SandboxID)
	sample.RegionID = strings.TrimSpace(sample.RegionID)
	sample.ClusterID = strings.TrimSpace(sample.ClusterID)
	sample.SeriesEpoch = strings.TrimSpace(sample.SeriesEpoch)
	sample.SampleID = strings.TrimSpace(sample.SampleID)
	if sample.TeamID == "" {
		return sandboxobservability.RuntimeSample{}, fmt.Errorf("team_id is required")
	}
	if sample.SandboxID == "" {
		return sandboxobservability.RuntimeSample{}, fmt.Errorf("sandbox_id is required")
	}
	if sample.RuntimeGeneration < 0 {
		return sandboxobservability.RuntimeSample{}, fmt.Errorf("runtime_generation must be non-negative")
	}
	if sample.SeriesEpoch == "" {
		return sandboxobservability.RuntimeSample{}, fmt.Errorf("series_epoch is required")
	}
	if len(sample.SeriesEpoch) > maxRuntimeSeriesEpoch {
		return sandboxobservability.RuntimeSample{}, fmt.Errorf("series_epoch cannot exceed %d bytes", maxRuntimeSeriesEpoch)
	}
	if sample.ObservedAt.IsZero() {
		return sandboxobservability.RuntimeSample{}, fmt.Errorf("observed_at is required")
	}
	if sample.SampleID == "" {
		return sandboxobservability.RuntimeSample{}, fmt.Errorf("sample_id is required")
	}
	if len(sample.SampleID) > maxRuntimeSampleID {
		return sandboxobservability.RuntimeSample{}, fmt.Errorf("sample_id cannot exceed %d bytes", maxRuntimeSampleID)
	}
	if !runtimeSampleHasValues(sample) && len(sample.Missing) == 0 {
		return sandboxobservability.RuntimeSample{}, fmt.Errorf("at least one runtime value or missing metric is required")
	}
	if err := validateRuntimeValues(sample); err != nil {
		return sandboxobservability.RuntimeSample{}, err
	}
	for i := range sample.Missing {
		missing := &sample.Missing[i]
		missing.Detail = strings.TrimSpace(missing.Detail)
		descriptor, ok := sandboxobservability.RuntimeMetricDescriptorFor(missing.Metric)
		if !ok {
			return sandboxobservability.RuntimeSample{}, fmt.Errorf("missing metric %d has unknown name", i)
		}
		if !sandboxobservability.ValidRuntimeMetricMissingReason(missing.Reason) {
			return sandboxobservability.RuntimeSample{}, fmt.Errorf("missing metric %d has invalid reason", i)
		}
		for dimension := range missing.Dimensions {
			if !containsString(descriptor.Dimensions, dimension) {
				return sandboxobservability.RuntimeSample{}, fmt.Errorf("missing metric %d has unknown dimension %q", i, dimension)
			}
		}
		if direction := strings.TrimSpace(missing.Dimensions["direction"]); direction != "" &&
			direction != string(sandboxobservability.RuntimeMetricDirectionReceive) &&
			direction != string(sandboxobservability.RuntimeMetricDirectionTransmit) {
			return sandboxobservability.RuntimeSample{}, fmt.Errorf("missing metric %d has invalid direction", i)
		}
	}
	sample.ObservedAt = sample.ObservedAt.UTC()
	// Ingestion time is a storage trust-boundary value. Never accept a
	// producer-supplied timestamp because it determines replacement ordering.
	sample.IngestedAt = now.UTC()
	return sample, nil
}

func validateRuntimeValues(sample sandboxobservability.RuntimeSample) error {
	floatValues := []*float64{}
	if sample.CPU != nil {
		floatValues = append(floatValues, sample.CPU.Utilization, sample.CPU.Usage, sample.CPU.TimeSeconds, sample.CPU.LimitCores)
	}
	if sample.Memory != nil {
		floatValues = append(floatValues, sample.Memory.Utilization)
	}
	for _, value := range floatValues {
		if value == nil {
			continue
		}
		if math.IsNaN(*value) || math.IsInf(*value, 0) || *value < 0 {
			return fmt.Errorf("runtime values must be finite and non-negative")
		}
	}
	return nil
}

func runtimeSampleHasValues(sample sandboxobservability.RuntimeSample) bool {
	return sample.CPU != nil && (sample.CPU.Utilization != nil || sample.CPU.Usage != nil || sample.CPU.TimeSeconds != nil || sample.CPU.LimitCores != nil) ||
		sample.Memory != nil && (sample.Memory.UsageBytes != nil || sample.Memory.WorkingSetBytes != nil || sample.Memory.AvailableBytes != nil || sample.Memory.LimitBytes != nil || sample.Memory.Utilization != nil) ||
		sample.Network != nil && (sample.Network.ReceiveBytes != nil || sample.Network.TransmitBytes != nil || sample.Network.ReceiveErrors != nil || sample.Network.TransmitErrors != nil) ||
		sample.Process != nil && sample.Process.Count != nil ||
		sample.RootFSWritable != nil && (sample.RootFSWritable.UsageBytes != nil || sample.RootFSWritable.Inodes != nil)
}

func containsString(values []string, candidate string) bool {
	for _, value := range values {
		if value == candidate {
			return true
		}
	}
	return false
}

func nullableFloat64(value *float64) any {
	if value == nil {
		return nil
	}
	return *value
}

func nullableUint64(value *uint64) any {
	if value == nil {
		return nil
	}
	return *value
}

func (r *Repository) queryRuntimeMetric(ctx context.Context, query sandboxobservability.RuntimeSeriesQuery, descriptor sandboxobservability.RuntimeMetricDescriptor, rawLimit int) (runtimeMetricLoad, error) {
	columns := runtimeMetricColumns(descriptor.Name)
	if len(columns) == 0 {
		return runtimeMetricLoad{}, fmt.Errorf("metric %q has no storage projection", descriptor.Name)
	}

	sqlQuery, args := r.buildRuntimeMetricQuery(query, descriptor, rawLimit+1)
	rows, err := r.db.QueryContext(ctx, sqlQuery, args...)
	if err != nil {
		return runtimeMetricLoad{}, err
	}
	defer rows.Close()

	load := runtimeMetricLoad{Inputs: initializeRuntimeSeriesInputs([]sandboxobservability.RuntimeMetricDescriptor{descriptor})}
	pointCapacity := runtimeRawPointCapacity(query, rawLimit)
	for _, input := range load.Inputs {
		input.Points = make([]rawRuntimePoint, 0, pointCapacity)
	}
	missingGapRanges := map[string][]sandboxobservability.RuntimeSeriesGap{}
	selected := map[sandboxobservability.RuntimeMetricName]struct{}{descriptor.Name: {}}
	var runtimeGeneration int64
	var seriesEpoch string
	var observedAt time.Time
	var missingJSON string
	values := make([]runtimeMetricSQLValue, len(columns))
	destinations := make([]any, 0, 4+len(columns))
	destinations = append(destinations, &runtimeGeneration, &seriesEpoch, &observedAt)
	for i := range columns {
		destinations = append(destinations, values[i].destination(columns[i].Unsigned))
	}
	destinations = append(destinations, &missingJSON)
	seriesEpochs := map[string]string{}
	rowCount := 0
	for rows.Next() {
		if err := rows.Scan(destinations...); err != nil {
			return runtimeMetricLoad{}, err
		}

		rowCount++
		if rowCount > rawLimit {
			load.Truncated = true
			continue
		}
		observedAt = observedAt.UTC()
		if interned, ok := seriesEpochs[seriesEpoch]; ok {
			seriesEpoch = interned
		} else {
			seriesEpochs[seriesEpoch] = seriesEpoch
		}
		if load.Oldest == nil || observedAt.Before(*load.Oldest) {
			oldest := observedAt
			load.Oldest = &oldest
		}
		if !observedAt.Before(query.StartTime) && !observedAt.After(query.EndTime) {
			if load.Newest == nil || observedAt.After(*load.Newest) {
				newest := observedAt
				load.Newest = &newest
			}
			trimmedMissing := strings.TrimSpace(missingJSON)
			if trimmedMissing != "" && trimmedMissing != "[]" && trimmedMissing != "null" {
				var missing []sandboxobservability.RuntimeMetricMissing
				if err := json.Unmarshal([]byte(trimmedMissing), &missing); err != nil {
					return runtimeMetricLoad{}, fmt.Errorf("decode missing metrics: %w", err)
				}
				appendRuntimeMissingGaps(missingGapRanges, selected, sandboxobservability.RuntimeSample{
					ObservedAt: observedAt,
					Missing:    missing,
				}, query.StartTime, query.EndTime, query.Step)
			}
		}
		for i, column := range columns {
			value, valid := values[i].value(column.Unsigned)
			if !valid {
				continue
			}
			input := load.Inputs[column.Key]
			input.Points = append(input.Points, rawRuntimePoint{
				Time:              observedAt,
				Value:             value,
				RuntimeGeneration: runtimeGeneration,
				SeriesEpoch:       seriesEpoch,
			})
		}
	}
	if err := rows.Err(); err != nil {
		return runtimeMetricLoad{}, err
	}
	for _, input := range load.Inputs {
		for left, right := 0, len(input.Points)-1; left < right; left, right = left+1, right-1 {
			input.Points[left], input.Points[right] = input.Points[right], input.Points[left]
		}
	}
	for _, ranges := range missingGapRanges {
		load.Gaps = append(load.Gaps, ranges...)
	}
	return load, nil
}

func runtimeMetricColumns(metric sandboxobservability.RuntimeMetricName) []runtimeMetricColumn {
	scalar := func(name string, unsigned bool) []runtimeMetricColumn {
		return []runtimeMetricColumn{{Name: name, Key: runtimeSeriesKey{Metric: metric}, Unsigned: unsigned}}
	}
	switch metric {
	case sandboxobservability.RuntimeMetricCPUUtilization:
		return scalar("cpu_utilization", false)
	case sandboxobservability.RuntimeMetricCPUUsage:
		return scalar("cpu_usage", false)
	case sandboxobservability.RuntimeMetricCPUTime:
		return scalar("cpu_time_seconds", false)
	case sandboxobservability.RuntimeMetricCPULimit:
		return scalar("cpu_limit_cores", false)
	case sandboxobservability.RuntimeMetricMemoryUsage:
		return scalar("memory_usage_bytes", true)
	case sandboxobservability.RuntimeMetricMemoryWorkingSet:
		return scalar("memory_working_set_bytes", true)
	case sandboxobservability.RuntimeMetricMemoryAvailable:
		return scalar("memory_available_bytes", true)
	case sandboxobservability.RuntimeMetricMemoryLimit:
		return scalar("memory_limit_bytes", true)
	case sandboxobservability.RuntimeMetricMemoryUtilization:
		return scalar("memory_utilization", false)
	case sandboxobservability.RuntimeMetricNetworkIO:
		return []runtimeMetricColumn{
			{Name: "network_receive_bytes", Key: runtimeSeriesKey{Metric: metric, Direction: sandboxobservability.RuntimeMetricDirectionReceive}, Unsigned: true},
			{Name: "network_transmit_bytes", Key: runtimeSeriesKey{Metric: metric, Direction: sandboxobservability.RuntimeMetricDirectionTransmit}, Unsigned: true},
		}
	case sandboxobservability.RuntimeMetricNetworkErrors:
		return []runtimeMetricColumn{
			{Name: "network_receive_errors", Key: runtimeSeriesKey{Metric: metric, Direction: sandboxobservability.RuntimeMetricDirectionReceive}, Unsigned: true},
			{Name: "network_transmit_errors", Key: runtimeSeriesKey{Metric: metric, Direction: sandboxobservability.RuntimeMetricDirectionTransmit}, Unsigned: true},
		}
	case sandboxobservability.RuntimeMetricProcessCount:
		return scalar("process_count", true)
	case sandboxobservability.RuntimeMetricRootFSWritableUsage:
		return scalar("rootfs_writable_usage_bytes", true)
	case sandboxobservability.RuntimeMetricRootFSWritableInodes:
		return scalar("rootfs_writable_inodes", true)
	default:
		return nil
	}
}

func normalizeRuntimeSeriesQuery(query sandboxobservability.RuntimeSeriesQuery, now time.Time) (sandboxobservability.RuntimeSeriesQuery, []sandboxobservability.RuntimeMetricDescriptor, error) {
	query.TeamID = strings.TrimSpace(query.TeamID)
	query.SandboxID = strings.TrimSpace(query.SandboxID)
	if query.TeamID == "" {
		return query, nil, fmt.Errorf("%w: team_id is required", sandboxobservability.ErrInvalidQuery)
	}
	if query.SandboxID == "" {
		return query, nil, fmt.Errorf("%w: sandbox_id is required", sandboxobservability.ErrInvalidQuery)
	}
	if query.EndTime.IsZero() {
		query.EndTime = now
	}
	query.EndTime = query.EndTime.UTC()
	if query.StartTime.IsZero() {
		query.StartTime = query.EndTime.Add(-defaultRuntimeWindow)
	}
	query.StartTime = query.StartTime.UTC()
	if !query.EndTime.After(query.StartTime) {
		return query, nil, fmt.Errorf("%w: end_time must be greater than start_time", sandboxobservability.ErrInvalidQuery)
	}
	if query.EndTime.Sub(query.StartTime) > maxRuntimeQueryRange {
		return query, nil, fmt.Errorf("%w: time range cannot exceed 30 days", sandboxobservability.ErrInvalidQuery)
	}
	if query.MaxPoints <= 0 {
		query.MaxPoints = defaultRuntimeMaxPoints
	}
	if query.MaxPoints > maxRuntimeMaxPoints {
		query.MaxPoints = maxRuntimeMaxPoints
	}
	minimumStep := niceRuntimeStep(query.EndTime.Sub(query.StartTime), query.MaxPoints)
	if query.Step <= 0 || query.Step < minimumStep {
		query.Step = minimumStep
	}
	if query.Statistic == "" {
		query.Statistic = sandboxobservability.RuntimeMetricStatisticAuto
	}
	if !sandboxobservability.ValidRuntimeMetricStatistic(query.Statistic) {
		return query, nil, fmt.Errorf("%w: invalid statistic", sandboxobservability.ErrInvalidQuery)
	}

	seen := map[sandboxobservability.RuntimeMetricName]struct{}{}
	selected := []sandboxobservability.RuntimeMetricDescriptor{}
	if len(query.Metrics) == 0 {
		query.Metrics = []sandboxobservability.RuntimeMetricName{
			sandboxobservability.RuntimeMetricCPUUtilization,
			sandboxobservability.RuntimeMetricMemoryUtilization,
			sandboxobservability.RuntimeMetricNetworkIO,
		}
		for _, name := range query.Metrics {
			descriptor, _ := sandboxobservability.RuntimeMetricDescriptorFor(name)
			selected = append(selected, descriptor)
		}
	} else {
		metrics := make([]sandboxobservability.RuntimeMetricName, 0, len(query.Metrics))
		for _, name := range query.Metrics {
			name = sandboxobservability.RuntimeMetricName(strings.TrimSpace(string(name)))
			if _, duplicate := seen[name]; duplicate {
				continue
			}
			descriptor, ok := sandboxobservability.RuntimeMetricDescriptorFor(name)
			if !ok {
				return query, nil, fmt.Errorf("%w: unknown metric %q", sandboxobservability.ErrInvalidQuery, name)
			}
			seen[name] = struct{}{}
			metrics = append(metrics, name)
			selected = append(selected, descriptor)
		}
		query.Metrics = metrics
	}
	if query.Statistic == sandboxobservability.RuntimeMetricStatisticRate {
		for _, descriptor := range selected {
			if descriptor.Kind != sandboxobservability.RuntimeMetricKindCounter {
				return query, nil, fmt.Errorf("%w: rate is only valid for counter metrics", sandboxobservability.ErrInvalidQuery)
			}
		}
	}
	return query, selected, nil
}

func niceRuntimeStep(window time.Duration, maxPoints int) time.Duration {
	if maxPoints <= 0 {
		maxPoints = defaultRuntimeMaxPoints
	}
	desired := time.Duration(math.Ceil(float64(window) / float64(maxPoints)))
	steps := []time.Duration{
		minimumRuntimeStep,
		30 * time.Second,
		time.Minute,
		2 * time.Minute,
		5 * time.Minute,
		10 * time.Minute,
		15 * time.Minute,
		30 * time.Minute,
		time.Hour,
		2 * time.Hour,
		6 * time.Hour,
		12 * time.Hour,
		24 * time.Hour,
	}
	for _, step := range steps {
		if step >= desired {
			return step
		}
	}
	return time.Duration(math.Ceil(float64(desired)/float64(24*time.Hour))) * 24 * time.Hour
}

type runtimeResultState struct {
	query        sandboxobservability.RuntimeSeriesQuery
	series       []sandboxobservability.RuntimeSeries
	gaps         []sandboxobservability.RuntimeSeriesGap
	newest       *time.Time
	pointLimited bool
	rawTruncated bool
}

func newRuntimeResultState(query sandboxobservability.RuntimeSeriesQuery) *runtimeResultState {
	return &runtimeResultState{query: query}
}

func (s *runtimeResultState) addLoad(descriptor sandboxobservability.RuntimeMetricDescriptor, load runtimeMetricLoad) {
	s.gaps = append(s.gaps, load.Gaps...)
	if load.Newest != nil && (s.newest == nil || load.Newest.After(*s.newest)) {
		newest := load.Newest.UTC()
		s.newest = &newest
	}
	if load.Truncated {
		s.rawTruncated = true
	}
	for _, key := range orderedRuntimeSeriesKeys([]sandboxobservability.RuntimeMetricDescriptor{descriptor}) {
		s.addInput(load.Inputs[key], load.Truncated, load.Oldest)
	}
}

func (s *runtimeResultState) addInput(input *runtimeSeriesInput, truncated bool, oldest *time.Time) {
	if truncated && oldest != nil {
		s.gaps = appendBoundedRuntimeGap(s.gaps, input.Descriptor.Name, input.Dimensions, s.query.StartTime, *oldest, sandboxobservability.RuntimeSeriesGapNoData, s.query)
	}
	statistic := s.query.Statistic
	if statistic == sandboxobservability.RuntimeMetricStatisticAuto {
		if input.Descriptor.Kind == sandboxobservability.RuntimeMetricKindCounter {
			statistic = sandboxobservability.RuntimeMetricStatisticRate
		} else {
			statistic = sandboxobservability.RuntimeMetricStatisticAverage
		}
	}
	points, rateGaps := aggregateRuntimePoints(input.Points, input.Descriptor, statistic, input.Dimensions, s.query)
	s.gaps = append(s.gaps, rateGaps...)
	if s.query.MaxPoints > 0 && len(points) > s.query.MaxPoints {
		firstKept := len(points) - s.query.MaxPoints
		gapEnd := points[firstKept].Time
		if !gapEnd.After(s.query.StartTime) {
			gapEnd = s.query.StartTime.Add(s.query.Step)
			if gapEnd.After(s.query.EndTime) {
				gapEnd = s.query.EndTime
			}
		}
		s.gaps = appendBoundedRuntimeGap(s.gaps, input.Descriptor.Name, input.Dimensions, s.query.StartTime, gapEnd, sandboxobservability.RuntimeSeriesGapNoData, s.query)
		points = points[firstKept:]
		s.pointLimited = true
	}
	s.gaps = append(s.gaps, detectRuntimePointGaps(input.Descriptor.Name, input.Dimensions, points, s.query)...)
	s.series = append(s.series, sandboxobservability.RuntimeSeries{
		Metric:     input.Descriptor.Name,
		Kind:       input.Descriptor.Kind,
		Unit:       runtimeSeriesUnit(input.Descriptor, statistic),
		Statistic:  statistic,
		Dimensions: copyStringMap(input.Dimensions),
		Segments:   runtimeSeriesSegments(points, s.query.Step),
	})
}

func (s *runtimeResultState) result() *sandboxobservability.RuntimeSeriesResult {
	gaps := mergeRuntimeGaps(s.gaps, s.query.Step)
	freshness := runtimeFreshness(s.newest, s.query.EndTime, s.query.Step)
	return &sandboxobservability.RuntimeSeriesResult{
		StartTime:   s.query.StartTime,
		EndTime:     s.query.EndTime,
		StepSeconds: int64(s.query.Step / time.Second),
		Series:      s.series,
		Freshness:   freshness,
		Gaps:        gaps,
		Partial:     s.rawTruncated || s.pointLimited || len(gaps) > 0 || freshness.Status != sandboxobservability.RuntimeSeriesFreshnessFresh,
	}
}

func buildRuntimeSeriesResult(query sandboxobservability.RuntimeSeriesQuery, selected []sandboxobservability.RuntimeMetricDescriptor, samples []sandboxobservability.RuntimeSample, truncated bool) *sandboxobservability.RuntimeSeriesResult {
	inputs := initializeRuntimeSeriesInputs(selected)
	selectedNames := make(map[sandboxobservability.RuntimeMetricName]struct{}, len(selected))
	for _, descriptor := range selected {
		selectedNames[descriptor.Name] = struct{}{}
	}
	gaps := []sandboxobservability.RuntimeSeriesGap{}
	missingGapRanges := map[string][]sandboxobservability.RuntimeSeriesGap{}
	var newest *time.Time
	var oldest *time.Time
	for _, sample := range samples {
		appendRuntimeSampleValues(inputs, selectedNames, sample)
		observed := sample.ObservedAt.UTC()
		if oldest == nil || observed.Before(*oldest) {
			oldest = &observed
		}
		if !sample.ObservedAt.Before(query.StartTime) && !sample.ObservedAt.After(query.EndTime) {
			if newest == nil || observed.After(*newest) {
				newest = &observed
			}
			appendRuntimeMissingGaps(missingGapRanges, selectedNames, sample, query.StartTime, query.EndTime, query.Step)
		}
	}
	for _, ranges := range missingGapRanges {
		gaps = append(gaps, ranges...)
	}
	state := newRuntimeResultState(query)
	state.gaps = gaps
	state.newest = newest
	state.rawTruncated = truncated
	for _, descriptor := range selected {
		state.addLoad(descriptor, runtimeMetricLoad{
			Inputs:    inputs,
			Oldest:    oldest,
			Truncated: truncated,
		})
	}
	return state.result()
}

func initializeRuntimeSeriesInputs(selected []sandboxobservability.RuntimeMetricDescriptor) map[runtimeSeriesKey]*runtimeSeriesInput {
	inputs := map[runtimeSeriesKey]*runtimeSeriesInput{}
	for _, descriptor := range selected {
		if descriptor.Name == sandboxobservability.RuntimeMetricNetworkIO || descriptor.Name == sandboxobservability.RuntimeMetricNetworkErrors {
			for _, direction := range []sandboxobservability.RuntimeMetricDirection{sandboxobservability.RuntimeMetricDirectionReceive, sandboxobservability.RuntimeMetricDirectionTransmit} {
				key := runtimeSeriesKey{Metric: descriptor.Name, Direction: direction}
				inputs[key] = &runtimeSeriesInput{Descriptor: descriptor, Dimensions: map[string]string{"direction": string(direction)}, Points: []rawRuntimePoint{}}
			}
			continue
		}
		key := runtimeSeriesKey{Metric: descriptor.Name}
		inputs[key] = &runtimeSeriesInput{Descriptor: descriptor, Dimensions: map[string]string{}, Points: []rawRuntimePoint{}}
	}
	return inputs
}

func orderedRuntimeSeriesKeys(selected []sandboxobservability.RuntimeMetricDescriptor) []runtimeSeriesKey {
	keys := []runtimeSeriesKey{}
	for _, descriptor := range selected {
		if descriptor.Name == sandboxobservability.RuntimeMetricNetworkIO || descriptor.Name == sandboxobservability.RuntimeMetricNetworkErrors {
			keys = append(keys,
				runtimeSeriesKey{Metric: descriptor.Name, Direction: sandboxobservability.RuntimeMetricDirectionReceive},
				runtimeSeriesKey{Metric: descriptor.Name, Direction: sandboxobservability.RuntimeMetricDirectionTransmit},
			)
			continue
		}
		keys = append(keys, runtimeSeriesKey{Metric: descriptor.Name})
	}
	return keys
}

func appendRuntimeSampleValues(inputs map[runtimeSeriesKey]*runtimeSeriesInput, selected map[sandboxobservability.RuntimeMetricName]struct{}, sample sandboxobservability.RuntimeSample) {
	appendValue := func(metric sandboxobservability.RuntimeMetricName, direction sandboxobservability.RuntimeMetricDirection, value *float64) {
		if value == nil {
			return
		}
		if _, ok := selected[metric]; !ok {
			return
		}
		key := runtimeSeriesKey{Metric: metric, Direction: direction}
		if input := inputs[key]; input != nil {
			input.Points = append(input.Points, rawRuntimePoint{Time: sample.ObservedAt, Value: *value, RuntimeGeneration: sample.RuntimeGeneration, SeriesEpoch: sample.SeriesEpoch})
		}
	}
	appendUint := func(metric sandboxobservability.RuntimeMetricName, direction sandboxobservability.RuntimeMetricDirection, value *uint64) {
		if value == nil {
			return
		}
		converted := float64(*value)
		appendValue(metric, direction, &converted)
	}

	if sample.CPU != nil {
		appendValue(sandboxobservability.RuntimeMetricCPUUtilization, "", sample.CPU.Utilization)
		appendValue(sandboxobservability.RuntimeMetricCPUUsage, "", sample.CPU.Usage)
		appendValue(sandboxobservability.RuntimeMetricCPUTime, "", sample.CPU.TimeSeconds)
		appendValue(sandboxobservability.RuntimeMetricCPULimit, "", sample.CPU.LimitCores)
	}
	if sample.Memory != nil {
		appendUint(sandboxobservability.RuntimeMetricMemoryUsage, "", sample.Memory.UsageBytes)
		appendUint(sandboxobservability.RuntimeMetricMemoryWorkingSet, "", sample.Memory.WorkingSetBytes)
		appendUint(sandboxobservability.RuntimeMetricMemoryAvailable, "", sample.Memory.AvailableBytes)
		appendUint(sandboxobservability.RuntimeMetricMemoryLimit, "", sample.Memory.LimitBytes)
		appendValue(sandboxobservability.RuntimeMetricMemoryUtilization, "", sample.Memory.Utilization)
	}
	if sample.Network != nil {
		appendUint(sandboxobservability.RuntimeMetricNetworkIO, sandboxobservability.RuntimeMetricDirectionReceive, sample.Network.ReceiveBytes)
		appendUint(sandboxobservability.RuntimeMetricNetworkIO, sandboxobservability.RuntimeMetricDirectionTransmit, sample.Network.TransmitBytes)
		appendUint(sandboxobservability.RuntimeMetricNetworkErrors, sandboxobservability.RuntimeMetricDirectionReceive, sample.Network.ReceiveErrors)
		appendUint(sandboxobservability.RuntimeMetricNetworkErrors, sandboxobservability.RuntimeMetricDirectionTransmit, sample.Network.TransmitErrors)
	}
	if sample.Process != nil {
		appendUint(sandboxobservability.RuntimeMetricProcessCount, "", sample.Process.Count)
	}
	if sample.RootFSWritable != nil {
		appendUint(sandboxobservability.RuntimeMetricRootFSWritableUsage, "", sample.RootFSWritable.UsageBytes)
		appendUint(sandboxobservability.RuntimeMetricRootFSWritableInodes, "", sample.RootFSWritable.Inodes)
	}
}

func appendRuntimeMissingGaps(ranges map[string][]sandboxobservability.RuntimeSeriesGap, selected map[sandboxobservability.RuntimeMetricName]struct{}, sample sandboxobservability.RuntimeSample, start, end time.Time, step time.Duration) {
	for _, missing := range sample.Missing {
		if _, ok := selected[missing.Metric]; !ok {
			continue
		}
		gapEnd := sample.ObservedAt.UTC()
		gapStart := gapEnd.Add(-step)
		if gapStart.Before(start) {
			gapStart = start
			gapEnd = gapStart.Add(step)
		}
		if gapEnd.After(end) {
			gapEnd = end
		}
		directions := []string{strings.TrimSpace(missing.Dimensions["direction"])}
		if (missing.Metric == sandboxobservability.RuntimeMetricNetworkIO || missing.Metric == sandboxobservability.RuntimeMetricNetworkErrors) && directions[0] == "" {
			directions = []string{string(sandboxobservability.RuntimeMetricDirectionReceive), string(sandboxobservability.RuntimeMetricDirectionTransmit)}
		}
		for _, direction := range directions {
			var dimensions map[string]string
			if direction != "" {
				dimensions = map[string]string{"direction": direction}
			}
			gap := sandboxobservability.RuntimeSeriesGap{Metric: missing.Metric, Dimensions: dimensions, StartTime: gapStart, EndTime: gapEnd, Reason: sandboxobservability.RuntimeSeriesGapReason(missing.Reason)}
			key := runtimeGapKey(gap)
			existing := ranges[key]
			if len(existing) > 0 &&
				!gap.StartTime.After(existing[len(existing)-1].EndTime) &&
				!gap.EndTime.Before(existing[len(existing)-1].StartTime) {
				if gap.StartTime.Before(existing[len(existing)-1].StartTime) {
					existing[len(existing)-1].StartTime = gap.StartTime
				}
				if gap.EndTime.After(existing[len(existing)-1].EndTime) {
					existing[len(existing)-1].EndTime = gap.EndTime
				}
				ranges[key] = existing
				continue
			}
			ranges[key] = append(existing, gap)
		}
	}
}

func aggregateRuntimePoints(raw []rawRuntimePoint, descriptor sandboxobservability.RuntimeMetricDescriptor, statistic sandboxobservability.RuntimeMetricStatistic, dimensions map[string]string, query sandboxobservability.RuntimeSeriesQuery) ([]aggregatedRuntimePoint, []sandboxobservability.RuntimeSeriesGap) {
	sort.SliceStable(raw, func(i, j int) bool { return raw[i].Time.Before(raw[j].Time) })
	buckets := map[runtimeBucketKey]*runtimeBucket{}
	gaps := []sandboxobservability.RuntimeSeriesGap{}
	if statistic == sandboxobservability.RuntimeMetricStatisticRate {
		var resetSequence int64
		for i := 1; i < len(raw); i++ {
			previous, current := raw[i-1], raw[i]
			if current.Time.Before(query.StartTime) || current.Time.After(query.EndTime) {
				continue
			}
			if current.RuntimeGeneration != previous.RuntimeGeneration || current.SeriesEpoch != previous.SeriesEpoch || current.Value < previous.Value {
				gaps = appendBoundedRuntimeGap(gaps, descriptor.Name, dimensions, previous.Time, current.Time, sandboxobservability.RuntimeSeriesGapSeriesReset, query)
				resetSequence++
				continue
			}
			duration := current.Time.Sub(previous.Time).Seconds()
			if duration <= 0 {
				continue
			}
			if current.Time.Sub(previous.Time) > maximumCounterDelta {
				gaps = appendBoundedRuntimeGap(gaps, descriptor.Name, dimensions, previous.Time, current.Time, sandboxobservability.RuntimeSeriesGapNoData, query)
				resetSequence++
				continue
			}
			current.ResetSequence = resetSequence
			appendRuntimeBucket(buckets, current, (current.Value-previous.Value)/duration, query)
		}
	} else {
		var resetSequence int64
		for i, point := range raw {
			if i > 0 {
				previous := raw[i-1]
				reset := point.RuntimeGeneration != previous.RuntimeGeneration || point.SeriesEpoch != previous.SeriesEpoch ||
					descriptor.Kind == sandboxobservability.RuntimeMetricKindCounter && point.Value < previous.Value
				if reset {
					resetSequence++
					gaps = appendBoundedRuntimeGap(gaps, descriptor.Name, dimensions, previous.Time, point.Time, sandboxobservability.RuntimeSeriesGapSeriesReset, query)
				}
			}
			if point.Time.Before(query.StartTime) || point.Time.After(query.EndTime) {
				continue
			}
			point.ResetSequence = resetSequence
			appendRuntimeBucket(buckets, point, point.Value, query)
		}
	}

	indexes := make([]runtimeBucketKey, 0, len(buckets))
	for index := range buckets {
		indexes = append(indexes, index)
	}
	sort.Slice(indexes, func(i, j int) bool {
		if indexes[i].Index != indexes[j].Index {
			return indexes[i].Index < indexes[j].Index
		}
		if indexes[i].RuntimeGeneration != indexes[j].RuntimeGeneration {
			return indexes[i].RuntimeGeneration < indexes[j].RuntimeGeneration
		}
		if indexes[i].SeriesEpoch != indexes[j].SeriesEpoch {
			return indexes[i].SeriesEpoch < indexes[j].SeriesEpoch
		}
		return indexes[i].ResetSequence < indexes[j].ResetSequence
	})
	points := make([]aggregatedRuntimePoint, 0, len(indexes))
	for _, index := range indexes {
		bucket := buckets[index]
		value := bucket.Last
		switch statistic {
		case sandboxobservability.RuntimeMetricStatisticAverage, sandboxobservability.RuntimeMetricStatisticRate:
			value = bucket.Sum / float64(bucket.Count)
		case sandboxobservability.RuntimeMetricStatisticMinimum:
			value = bucket.Minimum
		case sandboxobservability.RuntimeMetricStatisticMaximum:
			value = bucket.Maximum
		case sandboxobservability.RuntimeMetricStatisticLast:
			value = bucket.Last
		}
		points = append(points, aggregatedRuntimePoint{Time: bucket.Time, Value: value, RuntimeGeneration: bucket.RuntimeGeneration, SeriesEpoch: bucket.SeriesEpoch, ResetSequence: bucket.ResetSequence})
	}
	return points, gaps
}

func appendBoundedRuntimeGap(gaps []sandboxobservability.RuntimeSeriesGap, metric sandboxobservability.RuntimeMetricName, dimensions map[string]string, start, end time.Time, reason sandboxobservability.RuntimeSeriesGapReason, query sandboxobservability.RuntimeSeriesQuery) []sandboxobservability.RuntimeSeriesGap {
	start = start.UTC()
	end = end.UTC()
	if start.Before(query.StartTime) {
		start = query.StartTime
	}
	if end.After(query.EndTime) {
		end = query.EndTime
	}
	if !end.After(start) {
		return gaps
	}
	return append(gaps, sandboxobservability.RuntimeSeriesGap{
		Metric:     metric,
		Dimensions: copyStringMap(dimensions),
		StartTime:  start,
		EndTime:    end,
		Reason:     reason,
	})
}

func appendRuntimeBucket(buckets map[runtimeBucketKey]*runtimeBucket, point rawRuntimePoint, value float64, query sandboxobservability.RuntimeSeriesQuery) {
	offset := point.Time.Sub(query.StartTime)
	// The range is externally inclusive, but buckets are half-open. Fold an
	// observation exactly at end_time into the final bucket so window/step
	// never produces an extra endpoint bucket.
	if point.Time.Equal(query.EndTime) && offset > 0 {
		offset--
	}
	index := int64(offset / query.Step)
	if index < 0 {
		return
	}
	key := runtimeBucketKey{Index: index, RuntimeGeneration: point.RuntimeGeneration, SeriesEpoch: point.SeriesEpoch, ResetSequence: point.ResetSequence}
	bucket := buckets[key]
	if bucket == nil {
		bucket = &runtimeBucket{Time: query.StartTime.Add(time.Duration(index) * query.Step), Minimum: value, Maximum: value}
		buckets[key] = bucket
	}
	bucket.Sum += value
	bucket.Count++
	if value < bucket.Minimum {
		bucket.Minimum = value
	}
	if value > bucket.Maximum {
		bucket.Maximum = value
	}
	bucket.Last = value
	bucket.RuntimeGeneration = point.RuntimeGeneration
	bucket.SeriesEpoch = point.SeriesEpoch
	bucket.ResetSequence = point.ResetSequence
}

func detectRuntimePointGaps(metric sandboxobservability.RuntimeMetricName, dimensions map[string]string, points []aggregatedRuntimePoint, query sandboxobservability.RuntimeSeriesQuery) []sandboxobservability.RuntimeSeriesGap {
	result := []sandboxobservability.RuntimeSeriesGap{}
	if len(points) == 0 {
		return []sandboxobservability.RuntimeSeriesGap{{Metric: metric, Dimensions: copyStringMap(dimensions), StartTime: query.StartTime, EndTime: query.EndTime, Reason: sandboxobservability.RuntimeSeriesGapNoData}}
	}
	if points[0].Time.Sub(query.StartTime) > 2*query.Step {
		result = append(result, sandboxobservability.RuntimeSeriesGap{Metric: metric, Dimensions: copyStringMap(dimensions), StartTime: query.StartTime, EndTime: points[0].Time, Reason: sandboxobservability.RuntimeSeriesGapNoData})
	}
	for i := 1; i < len(points); i++ {
		if points[i].Time.Sub(points[i-1].Time) > 2*query.Step {
			result = append(result, sandboxobservability.RuntimeSeriesGap{Metric: metric, Dimensions: copyStringMap(dimensions), StartTime: points[i-1].Time.Add(query.Step), EndTime: points[i].Time, Reason: sandboxobservability.RuntimeSeriesGapNoData})
		}
	}
	last := points[len(points)-1].Time
	if query.EndTime.Sub(last) > 2*query.Step {
		result = append(result, sandboxobservability.RuntimeSeriesGap{Metric: metric, Dimensions: copyStringMap(dimensions), StartTime: last.Add(query.Step), EndTime: query.EndTime, Reason: sandboxobservability.RuntimeSeriesGapNoData})
	}
	return result
}

func runtimeSeriesSegments(points []aggregatedRuntimePoint, step time.Duration) []sandboxobservability.RuntimeSeriesSegment {
	segments := []sandboxobservability.RuntimeSeriesSegment{}
	var generation int64
	var epoch string
	var resetSequence int64
	var previousTime time.Time
	for _, point := range points {
		if len(segments) == 0 || point.RuntimeGeneration != generation || point.SeriesEpoch != epoch || point.ResetSequence != resetSequence || (!previousTime.IsZero() && point.Time.Sub(previousTime) > 2*step) {
			segments = append(segments, sandboxobservability.RuntimeSeriesSegment{Points: []sandboxobservability.RuntimeSeriesPoint{}})
			generation = point.RuntimeGeneration
			epoch = point.SeriesEpoch
			resetSequence = point.ResetSequence
		}
		index := len(segments) - 1
		segments[index].Points = append(segments[index].Points, sandboxobservability.RuntimeSeriesPoint{Time: point.Time, Value: point.Value})
		previousTime = point.Time
	}
	return segments
}

func runtimeRawSampleLimit() int {
	// max_points bounds the returned aggregate, not the historical window.
	// Keep enough raw rows for a complete 30-day, 15-second series even when
	// the caller asks for only a handful of output points.
	return maxRuntimeRawSamples
}

func runtimeRawPointCapacity(query sandboxobservability.RuntimeSeriesQuery, rawLimit int) int {
	window := query.EndTime.Sub(query.StartTime) + maximumCounterDelta
	capacity := int((window+minimumRuntimeStep-1)/minimumRuntimeStep) + 1
	if capacity < 1 {
		capacity = 1
	}
	if capacity > rawLimit {
		capacity = rawLimit
	}
	return capacity
}

func mergeRuntimeGaps(gaps []sandboxobservability.RuntimeSeriesGap, _ time.Duration) []sandboxobservability.RuntimeSeriesGap {
	sort.Slice(gaps, func(i, j int) bool {
		left, right := runtimeGapKey(gaps[i]), runtimeGapKey(gaps[j])
		if left == right {
			return gaps[i].StartTime.Before(gaps[j].StartTime)
		}
		return left < right
	})
	merged := []sandboxobservability.RuntimeSeriesGap{}
	for _, gap := range gaps {
		if !gap.EndTime.After(gap.StartTime) {
			continue
		}
		if len(merged) == 0 {
			merged = append(merged, gap)
			continue
		}
		last := &merged[len(merged)-1]
		if runtimeGapKey(*last) == runtimeGapKey(gap) && !gap.StartTime.After(last.EndTime) {
			if gap.EndTime.After(last.EndTime) {
				last.EndTime = gap.EndTime
			}
			continue
		}
		merged = append(merged, gap)
	}
	return merged
}

func runtimeGapKey(gap sandboxobservability.RuntimeSeriesGap) string {
	return string(gap.Metric) + "\x00" + gap.Dimensions["direction"] + "\x00" + string(gap.Reason)
}

func runtimeFreshness(newest *time.Time, end time.Time, step time.Duration) sandboxobservability.RuntimeSeriesFreshness {
	if newest == nil {
		return sandboxobservability.RuntimeSeriesFreshness{Status: sandboxobservability.RuntimeSeriesFreshnessMissing}
	}
	age := end.Sub(*newest).Seconds()
	if age < 0 {
		age = 0
	}
	status := sandboxobservability.RuntimeSeriesFreshnessFresh
	if age > (2 * step).Seconds() {
		status = sandboxobservability.RuntimeSeriesFreshnessStale
	}
	return sandboxobservability.RuntimeSeriesFreshness{NewestObservedAt: newest, AgeSeconds: &age, Status: status}
}

func runtimeSeriesUnit(descriptor sandboxobservability.RuntimeMetricDescriptor, statistic sandboxobservability.RuntimeMetricStatistic) sandboxobservability.RuntimeMetricUnit {
	if statistic != sandboxobservability.RuntimeMetricStatisticRate {
		return descriptor.Unit
	}
	switch descriptor.Unit {
	case sandboxobservability.RuntimeMetricUnitBytes:
		return sandboxobservability.RuntimeMetricUnitBytesPerSecond
	case sandboxobservability.RuntimeMetricUnitCount:
		return sandboxobservability.RuntimeMetricUnitCountPerSecond
	case sandboxobservability.RuntimeMetricUnitSecond:
		return sandboxobservability.RuntimeMetricUnitCores
	default:
		return descriptor.Unit
	}
}

func copyStringMap(value map[string]string) map[string]string {
	if len(value) == 0 {
		return nil
	}
	result := make(map[string]string, len(value))
	for key, item := range value {
		result[key] = item
	}
	return result
}
