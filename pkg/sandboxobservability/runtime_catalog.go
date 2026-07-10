package sandboxobservability

import "strings"

var runtimeMetricDescriptors = []RuntimeMetricDescriptor{
	{Name: RuntimeMetricCPUUtilization, Kind: RuntimeMetricKindGauge, Unit: RuntimeMetricUnitRatio, Dimensions: []string{}, Description: "Fraction of the sandbox CPU limit currently in use."},
	{Name: RuntimeMetricCPUUsage, Kind: RuntimeMetricKindGauge, Unit: RuntimeMetricUnitCores, Dimensions: []string{}, Description: "CPU cores currently used by the sandbox."},
	{Name: RuntimeMetricCPUTime, Kind: RuntimeMetricKindCounter, Unit: RuntimeMetricUnitSecond, Dimensions: []string{}, Description: "Cumulative CPU time consumed by the sandbox."},
	{Name: RuntimeMetricCPULimit, Kind: RuntimeMetricKindGauge, Unit: RuntimeMetricUnitCores, Dimensions: []string{}, Description: "CPU cores allocated to the sandbox."},
	{Name: RuntimeMetricMemoryUsage, Kind: RuntimeMetricKindGauge, Unit: RuntimeMetricUnitBytes, Dimensions: []string{}, Description: "Current sandbox cgroup memory usage."},
	{Name: RuntimeMetricMemoryWorkingSet, Kind: RuntimeMetricKindGauge, Unit: RuntimeMetricUnitBytes, Dimensions: []string{}, Description: "Current sandbox memory working set."},
	{Name: RuntimeMetricMemoryAvailable, Kind: RuntimeMetricKindGauge, Unit: RuntimeMetricUnitBytes, Dimensions: []string{}, Description: "Memory available within the sandbox limit."},
	{Name: RuntimeMetricMemoryLimit, Kind: RuntimeMetricKindGauge, Unit: RuntimeMetricUnitBytes, Dimensions: []string{}, Description: "Memory bytes allocated to the sandbox."},
	{Name: RuntimeMetricMemoryUtilization, Kind: RuntimeMetricKindGauge, Unit: RuntimeMetricUnitRatio, Dimensions: []string{}, Description: "Fraction of the sandbox memory limit currently in use."},
	{Name: RuntimeMetricNetworkIO, Kind: RuntimeMetricKindCounter, Unit: RuntimeMetricUnitBytes, Dimensions: []string{"direction"}, Description: "Cumulative network bytes received or transmitted by the sandbox."},
	{Name: RuntimeMetricNetworkErrors, Kind: RuntimeMetricKindCounter, Unit: RuntimeMetricUnitCount, Dimensions: []string{"direction"}, Description: "Cumulative network errors observed for the sandbox."},
	{Name: RuntimeMetricProcessCount, Kind: RuntimeMetricKindGauge, Unit: RuntimeMetricUnitCount, Dimensions: []string{}, Description: "Current process count in the sandbox."},
	{Name: RuntimeMetricRootFSWritableUsage, Kind: RuntimeMetricKindGauge, Unit: RuntimeMetricUnitBytes, Dimensions: []string{}, Description: "Bytes used by the writable rootfs layer when supported."},
	{Name: RuntimeMetricRootFSWritableInodes, Kind: RuntimeMetricKindGauge, Unit: RuntimeMetricUnitCount, Dimensions: []string{}, Description: "Inodes used by the writable rootfs layer when supported."},
}

var runtimeMetricDescriptorByName = func() map[RuntimeMetricName]RuntimeMetricDescriptor {
	result := make(map[RuntimeMetricName]RuntimeMetricDescriptor, len(runtimeMetricDescriptors))
	for _, descriptor := range runtimeMetricDescriptors {
		result[descriptor.Name] = descriptor
	}
	return result
}()

// RuntimeMetricCatalogSnapshot returns a defensive copy of the bounded catalog.
func RuntimeMetricCatalogSnapshot() RuntimeMetricCatalog {
	metrics := make([]RuntimeMetricDescriptor, len(runtimeMetricDescriptors))
	for i, descriptor := range runtimeMetricDescriptors {
		dimensions := make([]string, len(descriptor.Dimensions))
		copy(dimensions, descriptor.Dimensions)
		descriptor.Dimensions = dimensions
		metrics[i] = descriptor
	}
	return RuntimeMetricCatalog{Metrics: metrics}
}

func RuntimeMetricDescriptorFor(name RuntimeMetricName) (RuntimeMetricDescriptor, bool) {
	descriptor, ok := runtimeMetricDescriptorByName[RuntimeMetricName(strings.TrimSpace(string(name)))]
	if !ok {
		return RuntimeMetricDescriptor{}, false
	}
	dimensions := make([]string, len(descriptor.Dimensions))
	copy(dimensions, descriptor.Dimensions)
	descriptor.Dimensions = dimensions
	return descriptor, true
}

func ValidRuntimeMetricName(name RuntimeMetricName) bool {
	_, ok := RuntimeMetricDescriptorFor(name)
	return ok
}

func ValidRuntimeMetricMissingReason(reason RuntimeMetricMissingReason) bool {
	switch reason {
	case RuntimeMetricMissingUnavailable, RuntimeMetricMissingUnsupported, RuntimeMetricMissingCollectionError:
		return true
	default:
		return false
	}
}

func ValidRuntimeMetricStatistic(statistic RuntimeMetricStatistic) bool {
	switch statistic {
	case RuntimeMetricStatisticAuto,
		RuntimeMetricStatisticAverage,
		RuntimeMetricStatisticMinimum,
		RuntimeMetricStatisticMaximum,
		RuntimeMetricStatisticLast,
		RuntimeMetricStatisticRate:
		return true
	default:
		return false
	}
}
