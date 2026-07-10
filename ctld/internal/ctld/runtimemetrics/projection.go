package runtimemetrics

import (
	"crypto/sha256"
	"encoding/hex"
	"strconv"
	"strings"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/sandboxobservability"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
)

const nanoScale = 1_000_000_000

func projectRuntimeSample(identity sandboxIdentity, stats *runtimeapi.PodSandboxStats, regionID, clusterID string, collectedAt time.Time, derivedCPUUsage *float64) (sandboxobservability.RuntimeSample, bool) {
	if stats == nil || stats.Attributes == nil {
		return sandboxobservability.RuntimeSample{}, false
	}
	seriesEpoch := runtimeSeriesEpoch(identity, stats.Attributes)
	if seriesEpoch == "" {
		return sandboxobservability.RuntimeSample{}, false
	}

	observedAt := collectedAt.UTC()
	if linux := stats.Linux; linux != nil {
		if timestamp := latestRuntimeTimestamp(linux); timestamp > 0 {
			observedAt = time.Unix(0, timestamp).UTC()
		}
	}
	if observedAt.IsZero() {
		return sandboxobservability.RuntimeSample{}, false
	}

	sample := sandboxobservability.RuntimeSample{
		TeamID:            identity.TeamID,
		SandboxID:         identity.SandboxID,
		RegionID:          strings.TrimSpace(regionID),
		ClusterID:         strings.TrimSpace(clusterID),
		RuntimeGeneration: identity.RuntimeGeneration,
		SeriesEpoch:       seriesEpoch,
		ObservedAt:        observedAt,
	}
	if stats.Linux == nil {
		appendAllRuntimeMissing(&sample, sandboxobservability.RuntimeMetricMissingUnsupported, "linux pod sandbox stats are unavailable")
	} else {
		projectCPU(&sample, stats.Linux.Cpu, identity.CPULimitCores, derivedCPUUsage)
		projectMemory(&sample, stats.Linux.Memory, identity.MemoryLimitBytes)
		projectNetwork(&sample, stats.Linux.Network)
		projectProcess(&sample, stats.Linux.Process)
		projectRootFSWritable(&sample, stats.Linux.Containers)
	}
	sample.SampleID = runtimeSampleID(sample)
	return sample, true
}

func projectCPU(sample *sandboxobservability.RuntimeSample, usage *runtimeapi.CpuUsage, limit *float64, derivedUsage *float64) {
	values := &sandboxobservability.RuntimeCPUValues{LimitCores: cloneFloat64(limit)}
	if limit == nil {
		appendMissing(sample, sandboxobservability.RuntimeMetricCPULimit, nil, sandboxobservability.RuntimeMetricMissingUnavailable, "cpu limit is not configured")
	}
	if usage == nil {
		appendMissing(sample, sandboxobservability.RuntimeMetricCPUUsage, nil, sandboxobservability.RuntimeMetricMissingUnavailable, "CRI cpu usage is unavailable")
		appendMissing(sample, sandboxobservability.RuntimeMetricCPUTime, nil, sandboxobservability.RuntimeMetricMissingUnavailable, "CRI cumulative cpu time is unavailable")
		appendMissing(sample, sandboxobservability.RuntimeMetricCPUUtilization, nil, sandboxobservability.RuntimeMetricMissingUnavailable, "cpu usage is unavailable")
		sample.CPU = values
		return
	}
	usageCores := cloneFloat64(derivedUsage)
	if usage.UsageNanoCores != nil {
		cores := float64(usage.UsageNanoCores.Value) / nanoScale
		usageCores = &cores
	}
	if usageCores != nil {
		values.Usage = usageCores
		if limit != nil && *limit > 0 {
			utilization := *usageCores / *limit
			values.Utilization = &utilization
		} else {
			appendMissing(sample, sandboxobservability.RuntimeMetricCPUUtilization, nil, sandboxobservability.RuntimeMetricMissingUnavailable, "cpu limit is unavailable")
		}
	} else {
		appendMissing(sample, sandboxobservability.RuntimeMetricCPUUsage, nil, sandboxobservability.RuntimeMetricMissingUnavailable, "CRI cpu usage cannot be derived yet")
		appendMissing(sample, sandboxobservability.RuntimeMetricCPUUtilization, nil, sandboxobservability.RuntimeMetricMissingUnavailable, "cpu usage is unavailable")
	}
	if usage.UsageCoreNanoSeconds != nil {
		seconds := float64(usage.UsageCoreNanoSeconds.Value) / nanoScale
		values.TimeSeconds = &seconds
	} else {
		appendMissing(sample, sandboxobservability.RuntimeMetricCPUTime, nil, sandboxobservability.RuntimeMetricMissingUnavailable, "CRI usage_core_nano_seconds is unavailable")
	}
	sample.CPU = values
}

func projectMemory(sample *sandboxobservability.RuntimeSample, usage *runtimeapi.MemoryUsage, limit *uint64) {
	values := &sandboxobservability.RuntimeMemoryValues{LimitBytes: cloneUint64(limit)}
	if limit == nil {
		appendMissing(sample, sandboxobservability.RuntimeMetricMemoryLimit, nil, sandboxobservability.RuntimeMetricMissingUnavailable, "memory limit is not configured")
	}
	if usage == nil {
		appendMissing(sample, sandboxobservability.RuntimeMetricMemoryUsage, nil, sandboxobservability.RuntimeMetricMissingUnavailable, "CRI memory usage is unavailable")
		appendMissing(sample, sandboxobservability.RuntimeMetricMemoryWorkingSet, nil, sandboxobservability.RuntimeMetricMissingUnavailable, "CRI memory working set is unavailable")
		appendMissing(sample, sandboxobservability.RuntimeMetricMemoryAvailable, nil, sandboxobservability.RuntimeMetricMissingUnavailable, "CRI available memory is unavailable")
		appendMissing(sample, sandboxobservability.RuntimeMetricMemoryUtilization, nil, sandboxobservability.RuntimeMetricMissingUnavailable, "memory working set is unavailable")
		sample.Memory = values
		return
	}
	values.UsageBytes = uint64Value(usage.UsageBytes)
	if values.UsageBytes == nil {
		appendMissing(sample, sandboxobservability.RuntimeMetricMemoryUsage, nil, sandboxobservability.RuntimeMetricMissingUnavailable, "CRI usage_bytes is unavailable")
	}
	values.WorkingSetBytes = uint64Value(usage.WorkingSetBytes)
	if values.WorkingSetBytes == nil {
		appendMissing(sample, sandboxobservability.RuntimeMetricMemoryWorkingSet, nil, sandboxobservability.RuntimeMetricMissingUnavailable, "CRI working_set_bytes is unavailable")
		appendMissing(sample, sandboxobservability.RuntimeMetricMemoryUtilization, nil, sandboxobservability.RuntimeMetricMissingUnavailable, "memory working set is unavailable")
	} else if limit != nil && *limit > 0 {
		utilization := float64(*values.WorkingSetBytes) / float64(*limit)
		values.Utilization = &utilization
	} else {
		appendMissing(sample, sandboxobservability.RuntimeMetricMemoryUtilization, nil, sandboxobservability.RuntimeMetricMissingUnavailable, "memory limit is unavailable")
	}
	values.AvailableBytes = uint64Value(usage.AvailableBytes)
	if values.AvailableBytes == nil {
		appendMissing(sample, sandboxobservability.RuntimeMetricMemoryAvailable, nil, sandboxobservability.RuntimeMetricMissingUnavailable, "CRI available_bytes is unavailable")
	}
	sample.Memory = values
}

func projectNetwork(sample *sandboxobservability.RuntimeSample, usage *runtimeapi.NetworkUsage) {
	values := &sandboxobservability.RuntimeNetworkValues{}
	interfaces := make([]*runtimeapi.NetworkInterfaceUsage, 0, 1)
	if usage != nil {
		if usage.DefaultInterface != nil {
			interfaces = append(interfaces, usage.DefaultInterface)
		}
		interfaces = append(interfaces, usage.Interfaces...)
	}
	values.ReceiveBytes = sumInterfaceValue(interfaces, func(item *runtimeapi.NetworkInterfaceUsage) *runtimeapi.UInt64Value { return item.RxBytes })
	values.TransmitBytes = sumInterfaceValue(interfaces, func(item *runtimeapi.NetworkInterfaceUsage) *runtimeapi.UInt64Value { return item.TxBytes })
	values.ReceiveErrors = sumInterfaceValue(interfaces, func(item *runtimeapi.NetworkInterfaceUsage) *runtimeapi.UInt64Value { return item.RxErrors })
	values.TransmitErrors = sumInterfaceValue(interfaces, func(item *runtimeapi.NetworkInterfaceUsage) *runtimeapi.UInt64Value { return item.TxErrors })
	if values.ReceiveBytes == nil {
		appendMissing(sample, sandboxobservability.RuntimeMetricNetworkIO, directionDimensions(sandboxobservability.RuntimeMetricDirectionReceive), sandboxobservability.RuntimeMetricMissingUnavailable, "CRI receive bytes are unavailable")
	}
	if values.TransmitBytes == nil {
		appendMissing(sample, sandboxobservability.RuntimeMetricNetworkIO, directionDimensions(sandboxobservability.RuntimeMetricDirectionTransmit), sandboxobservability.RuntimeMetricMissingUnavailable, "CRI transmit bytes are unavailable")
	}
	if values.ReceiveErrors == nil {
		appendMissing(sample, sandboxobservability.RuntimeMetricNetworkErrors, directionDimensions(sandboxobservability.RuntimeMetricDirectionReceive), sandboxobservability.RuntimeMetricMissingUnavailable, "CRI receive errors are unavailable")
	}
	if values.TransmitErrors == nil {
		appendMissing(sample, sandboxobservability.RuntimeMetricNetworkErrors, directionDimensions(sandboxobservability.RuntimeMetricDirectionTransmit), sandboxobservability.RuntimeMetricMissingUnavailable, "CRI transmit errors are unavailable")
	}
	sample.Network = values
}

func projectProcess(sample *sandboxobservability.RuntimeSample, usage *runtimeapi.ProcessUsage) {
	values := &sandboxobservability.RuntimeProcessValues{}
	if usage != nil {
		values.Count = uint64Value(usage.ProcessCount)
	}
	if values.Count == nil {
		appendMissing(sample, sandboxobservability.RuntimeMetricProcessCount, nil, sandboxobservability.RuntimeMetricMissingUnavailable, "CRI process count is unavailable")
	}
	sample.Process = values
}

func projectRootFSWritable(sample *sandboxobservability.RuntimeSample, containers []*runtimeapi.ContainerStats) {
	values := &sandboxobservability.RuntimeRootFSWritableValues{}
	var writable *runtimeapi.FilesystemUsage
	for _, container := range containers {
		if container == nil || container.Attributes == nil || container.Attributes.Metadata == nil {
			continue
		}
		if container.Attributes.Metadata.Name == sandboxContainerName {
			writable = container.WritableLayer
			break
		}
	}
	if writable != nil && !isUnavailableWritableLayer(writable) {
		values.UsageBytes = uint64Value(writable.UsedBytes)
		values.Inodes = uint64Value(writable.InodesUsed)
	}
	if values.UsageBytes == nil {
		appendMissing(sample, sandboxobservability.RuntimeMetricRootFSWritableUsage, nil, sandboxobservability.RuntimeMetricMissingUnavailable, "sandbox writable layer usage is unavailable")
	}
	if values.Inodes == nil {
		appendMissing(sample, sandboxobservability.RuntimeMetricRootFSWritableInodes, nil, sandboxobservability.RuntimeMetricMissingUnavailable, "sandbox writable layer inode usage is unavailable")
	}
	sample.RootFSWritable = values
}

func isUnavailableWritableLayer(usage *runtimeapi.FilesystemUsage) bool {
	if usage == nil || usage.Timestamp != 0 {
		return false
	}
	return (usage.UsedBytes == nil || usage.UsedBytes.Value == 0) &&
		(usage.InodesUsed == nil || usage.InodesUsed.Value == 0)
}

func runtimeSeriesEpoch(identity sandboxIdentity, attributes *runtimeapi.PodSandboxAttributes) string {
	if attributes != nil {
		if epoch := strings.TrimSpace(attributes.Id); epoch != "" {
			return epoch
		}
	}
	return strings.TrimSpace(identity.PodUID)
}

func latestRuntimeTimestamp(stats *runtimeapi.LinuxPodSandboxStats) int64 {
	if stats == nil {
		return 0
	}
	latest := int64(0)
	consider := func(value int64) {
		if value > latest {
			latest = value
		}
	}
	if stats.Cpu != nil {
		consider(stats.Cpu.Timestamp)
	}
	if stats.Memory != nil {
		consider(stats.Memory.Timestamp)
	}
	if stats.Network != nil {
		consider(stats.Network.Timestamp)
	}
	if stats.Process != nil {
		consider(stats.Process.Timestamp)
	}
	for _, container := range stats.Containers {
		if container != nil && container.WritableLayer != nil {
			consider(container.WritableLayer.Timestamp)
		}
	}
	return latest
}

func appendAllRuntimeMissing(sample *sandboxobservability.RuntimeSample, reason sandboxobservability.RuntimeMetricMissingReason, detail string) {
	metrics := []sandboxobservability.RuntimeMetricName{
		sandboxobservability.RuntimeMetricCPUUtilization,
		sandboxobservability.RuntimeMetricCPUUsage,
		sandboxobservability.RuntimeMetricCPUTime,
		sandboxobservability.RuntimeMetricCPULimit,
		sandboxobservability.RuntimeMetricMemoryUsage,
		sandboxobservability.RuntimeMetricMemoryWorkingSet,
		sandboxobservability.RuntimeMetricMemoryAvailable,
		sandboxobservability.RuntimeMetricMemoryLimit,
		sandboxobservability.RuntimeMetricMemoryUtilization,
		sandboxobservability.RuntimeMetricProcessCount,
		sandboxobservability.RuntimeMetricRootFSWritableUsage,
		sandboxobservability.RuntimeMetricRootFSWritableInodes,
	}
	for _, metric := range metrics {
		appendMissing(sample, metric, nil, reason, detail)
	}
	for _, direction := range []sandboxobservability.RuntimeMetricDirection{sandboxobservability.RuntimeMetricDirectionReceive, sandboxobservability.RuntimeMetricDirectionTransmit} {
		appendMissing(sample, sandboxobservability.RuntimeMetricNetworkIO, directionDimensions(direction), reason, detail)
		appendMissing(sample, sandboxobservability.RuntimeMetricNetworkErrors, directionDimensions(direction), reason, detail)
	}
}

func appendMissing(sample *sandboxobservability.RuntimeSample, metric sandboxobservability.RuntimeMetricName, dimensions map[string]string, reason sandboxobservability.RuntimeMetricMissingReason, detail string) {
	sample.Missing = append(sample.Missing, sandboxobservability.RuntimeMetricMissing{
		Metric:     metric,
		Dimensions: dimensions,
		Reason:     reason,
		Detail:     detail,
	})
}

func directionDimensions(direction sandboxobservability.RuntimeMetricDirection) map[string]string {
	return map[string]string{"direction": string(direction)}
}

func sumInterfaceValue(interfaces []*runtimeapi.NetworkInterfaceUsage, value func(*runtimeapi.NetworkInterfaceUsage) *runtimeapi.UInt64Value) *uint64 {
	var total uint64
	found := false
	for _, item := range interfaces {
		if item == nil || isLoopbackInterface(item.Name) {
			continue
		}
		wrapped := value(item)
		if wrapped == nil {
			continue
		}
		total += wrapped.Value
		found = true
	}
	if !found {
		return nil
	}
	return &total
}

func isLoopbackInterface(name string) bool {
	name = strings.ToLower(strings.TrimSpace(name))
	return name == "lo" || name == "loopback"
}

func uint64Value(value *runtimeapi.UInt64Value) *uint64 {
	if value == nil {
		return nil
	}
	copy := value.Value
	return &copy
}

func cloneUint64(value *uint64) *uint64 {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

func cloneFloat64(value *float64) *float64 {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

func runtimeSampleID(sample sandboxobservability.RuntimeSample) string {
	hash := sha256.New()
	_, _ = hash.Write([]byte(sample.TeamID))
	_, _ = hash.Write([]byte{0})
	_, _ = hash.Write([]byte(sample.SandboxID))
	_, _ = hash.Write([]byte{0})
	_, _ = hash.Write([]byte(strconv.FormatInt(sample.RuntimeGeneration, 10)))
	_, _ = hash.Write([]byte{0})
	_, _ = hash.Write([]byte(sample.SeriesEpoch))
	_, _ = hash.Write([]byte{0})
	_, _ = hash.Write([]byte(strconv.FormatInt(sample.ObservedAt.UnixNano(), 10)))
	return "ctld-runtime:" + hex.EncodeToString(hash.Sum(nil))
}
