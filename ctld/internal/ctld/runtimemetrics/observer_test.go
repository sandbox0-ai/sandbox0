package runtimemetrics

import (
	"errors"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
)

func TestObserverRecordsPartialCollection(t *testing.T) {
	registry := prometheus.NewRegistry()
	observer := NewObserver(registry)

	observer.ObserveCollection(2*time.Second, CollectResult{
		Matched:  3,
		Enqueued: 1,
		Dropped:  1,
		Failed:   1,
	}, errors.New("one sandbox stats request failed"))

	assert.Equal(t, float64(1), testutil.ToFloat64(observer.collections.WithLabelValues("partial")))
	assert.Equal(t, float64(3), testutil.ToFloat64(observer.targets))
	assert.Equal(t, float64(1), testutil.ToFloat64(observer.samples.WithLabelValues("enqueued")))
	assert.Equal(t, float64(1), testutil.ToFloat64(observer.samples.WithLabelValues("dropped")))
	assert.Equal(t, float64(1), testutil.ToFloat64(observer.samples.WithLabelValues("failed")))
}
