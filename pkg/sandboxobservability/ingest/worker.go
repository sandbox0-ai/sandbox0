package ingest

import (
	"fmt"
	"time"
)

const (
	DefaultQueueSize     = 1024
	DefaultBatchSize     = 100
	DefaultFlushInterval = time.Second
	DefaultMaxRetries    = 3
	DefaultRetryBackoff  = 100 * time.Millisecond
)

type Config struct {
	QueueSize     int
	BatchSize     int
	FlushInterval time.Duration
	MaxRetries    int
	RetryBackoff  time.Duration
}

func normalizeConfig(cfg Config) (Config, error) {
	if cfg.QueueSize == 0 {
		cfg.QueueSize = DefaultQueueSize
	}
	if cfg.BatchSize == 0 {
		cfg.BatchSize = DefaultBatchSize
	}
	if cfg.FlushInterval == 0 {
		cfg.FlushInterval = DefaultFlushInterval
	}
	if cfg.MaxRetries == 0 {
		cfg.MaxRetries = DefaultMaxRetries
	}
	if cfg.RetryBackoff == 0 {
		cfg.RetryBackoff = DefaultRetryBackoff
	}
	if cfg.QueueSize < 0 {
		return Config{}, fmt.Errorf("queue_size must be non-negative")
	}
	if cfg.BatchSize < 0 {
		return Config{}, fmt.Errorf("batch_size must be non-negative")
	}
	if cfg.FlushInterval < 0 {
		return Config{}, fmt.Errorf("flush_interval must be non-negative")
	}
	if cfg.MaxRetries < 0 {
		return Config{}, fmt.Errorf("max_retries must be non-negative")
	}
	if cfg.RetryBackoff < 0 {
		return Config{}, fmt.Errorf("retry_backoff must be non-negative")
	}
	return cfg, nil
}
