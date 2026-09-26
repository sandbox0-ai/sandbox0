package metrics

import (
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
)

// RegisterDatabasePool exposes acquisition waits separately from SQL execution
// time. Collection reads one in-memory snapshot and performs no database calls.
func RegisterDatabasePool(registry prometheus.Registerer, pool *pgxpool.Pool) {
	if registry == nil || pool == nil {
		return
	}
	registry.MustRegister(newDatabasePoolCollector(pool))
}

type databasePoolMetric struct {
	desc      *prometheus.Desc
	valueType prometheus.ValueType
	value     func(*pgxpool.Stat) float64
}

type databasePoolCollector struct {
	pool    *pgxpool.Pool
	metrics []databasePoolMetric
}

func newDatabasePoolCollector(pool *pgxpool.Pool) *databasePoolCollector {
	c := &databasePoolCollector{pool: pool}
	add := func(name, help string, typ prometheus.ValueType, value func(*pgxpool.Stat) float64) {
		c.metrics = append(c.metrics, databasePoolMetric{
			desc: prometheus.NewDesc("manager_pg_pool_"+name, help, nil, nil), valueType: typ, value: value,
		})
	}
	add("max_connections", "Maximum number of manager PostgreSQL pool connections", prometheus.GaugeValue, func(s *pgxpool.Stat) float64 { return float64(s.MaxConns()) })
	add("connections", "Total manager PostgreSQL pool connections including those being constructed", prometheus.GaugeValue, func(s *pgxpool.Stat) float64 { return float64(s.TotalConns()) })
	add("acquired_connections", "Manager PostgreSQL pool connections currently acquired", prometheus.GaugeValue, func(s *pgxpool.Stat) float64 { return float64(s.AcquiredConns()) })
	add("idle_connections", "Idle manager PostgreSQL pool connections", prometheus.GaugeValue, func(s *pgxpool.Stat) float64 { return float64(s.IdleConns()) })
	add("constructing_connections", "Manager PostgreSQL pool connections being constructed", prometheus.GaugeValue, func(s *pgxpool.Stat) float64 { return float64(s.ConstructingConns()) })
	add("acquires_total", "Successful manager PostgreSQL pool acquires", prometheus.CounterValue, func(s *pgxpool.Stat) float64 { return float64(s.AcquireCount()) })
	add("acquire_seconds_total", "Total duration of successful pool acquires including connection construction and waiting, excluding SQL execution", prometheus.CounterValue, func(s *pgxpool.Stat) float64 { return s.AcquireDuration().Seconds() })
	add("empty_acquires_total", "Successful acquires that waited because no pool connection was available", prometheus.CounterValue, func(s *pgxpool.Stat) float64 { return float64(s.EmptyAcquireCount()) })
	add("empty_acquire_wait_seconds_total", "Total wait for successful acquires made while the pool was empty", prometheus.CounterValue, func(s *pgxpool.Stat) float64 { return s.EmptyAcquireWaitTime().Seconds() })
	add("canceled_acquires_total", "Manager PostgreSQL pool acquires canceled by their context", prometheus.CounterValue, func(s *pgxpool.Stat) float64 { return float64(s.CanceledAcquireCount()) })
	add("new_connections_total", "Manager PostgreSQL pool connections opened", prometheus.CounterValue, func(s *pgxpool.Stat) float64 { return float64(s.NewConnsCount()) })
	return c
}

func (c *databasePoolCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, m := range c.metrics {
		ch <- m.desc
	}
}

func (c *databasePoolCollector) Collect(ch chan<- prometheus.Metric) {
	snapshot := c.pool.Stat()
	for _, m := range c.metrics {
		ch <- prometheus.MustNewConstMetric(m.desc, m.valueType, m.value(snapshot))
	}
}
