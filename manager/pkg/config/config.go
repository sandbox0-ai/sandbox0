package config

import (
	"os"
	"strconv"
	"time"
)

// Config holds the configuration for the manager
type Config struct {
	// HTTP Server
	HTTPPort int

	// Kubernetes
	KubeConfig     string
	Namespace      string
	LeaderElection bool
	ResyncPeriod   time.Duration

	// Database
	DatabaseURL string

	// Cleanup Controller
	CleanupInterval time.Duration

	// Logging
	LogLevel string

	// Metrics
	MetricsPort int
}

// LoadConfig loads configuration from environment variables
func LoadConfig() *Config {
	return &Config{
		HTTPPort:        getEnvInt("HTTP_PORT", 8080),
		KubeConfig:      os.Getenv("KUBECONFIG"),
		Namespace:       getEnv("NAMESPACE", "default"),
		LeaderElection:  getEnvBool("LEADER_ELECTION", true),
		ResyncPeriod:    getEnvDuration("RESYNC_PERIOD", 30*time.Second),
		DatabaseURL:     getEnv("DATABASE_URL", ""),
		CleanupInterval: getEnvDuration("CLEANUP_INTERVAL", 60*time.Second),
		LogLevel:        getEnv("LOG_LEVEL", "info"),
		MetricsPort:     getEnvInt("METRICS_PORT", 9090),
	}
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getEnvInt(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if intValue, err := strconv.Atoi(value); err == nil {
			return intValue
		}
	}
	return defaultValue
}

func getEnvBool(key string, defaultValue bool) bool {
	if value := os.Getenv(key); value != "" {
		if boolValue, err := strconv.ParseBool(value); err == nil {
			return boolValue
		}
	}
	return defaultValue
}

func getEnvDuration(key string, defaultValue time.Duration) time.Duration {
	if value := os.Getenv(key); value != "" {
		if duration, err := time.ParseDuration(value); err == nil {
			return duration
		}
	}
	return defaultValue
}
