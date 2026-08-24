package runtimemetrics

import (
	"crypto/sha256"
	"encoding/hex"
	"strconv"
	"strings"

	"github.com/sandbox0-ai/sandbox0/pkg/sandboxobservability"
)

const nanoScale = 1_000_000_000

func appendMissing(sample *sandboxobservability.RuntimeSample, metric sandboxobservability.RuntimeMetricName, dimensions map[string]string, reason sandboxobservability.RuntimeMetricMissingReason, detail string) {
	sample.Missing = append(sample.Missing, sandboxobservability.RuntimeMetricMissing{
		Metric: metric, Dimensions: dimensions, Reason: reason, Detail: detail,
	})
}

func isLoopbackInterface(name string) bool {
	name = strings.ToLower(strings.TrimSpace(name))
	return name == "lo" || name == "loopback"
}

func runtimeSampleID(sample sandboxobservability.RuntimeSample) string {
	hash := sha256.New()
	for _, value := range []string{
		sample.TeamID,
		sample.SandboxID,
		strconv.FormatInt(sample.RuntimeGeneration, 10),
		sample.SeriesEpoch,
		strconv.FormatInt(sample.ObservedAt.UnixNano(), 10),
	} {
		_, _ = hash.Write([]byte(value))
		_, _ = hash.Write([]byte{0})
	}
	return "ctld-runtime:" + hex.EncodeToString(hash.Sum(nil))
}
