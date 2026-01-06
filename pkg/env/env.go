package env

import (
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/joho/godotenv"
)

// Load attempts to load the .env file from the current directory or parent directories.
func Load() {
	if err := godotenv.Load(); err == nil {
		return
	}

	// Try to find .env in parent directories
	curr, err := os.Getwd()
	if err != nil {
		return
	}

	for {
		envPath := filepath.Join(curr, ".env")
		if _, err := os.Stat(envPath); err == nil {
			if err := godotenv.Load(envPath); err == nil {
				return
			}
		}

		parent := filepath.Dir(curr)
		if parent == curr {
			break
		}
		curr = parent
	}

	// Also try one specific path: the infra directory if we can guess where it is
	// This is a bit of a hack but helpful in some dev environments
	if home, err := os.UserHomeDir(); err == nil {
		infraEnv := filepath.Join(home, "sandbox0", "infra", ".env")
		if _, err := os.Stat(infraEnv); err == nil {
			_ = godotenv.Load(infraEnv)
		}
	}
}

// LoadFromPath loads environment variables from a specific file.
func LoadFromPath(path string) error {
	return godotenv.Load(path)
}

// GetEnv returns the value of an environment variable or a default value.
func GetEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

// GetEnvInt returns the value of an environment variable as an int or a default value.
func GetEnvInt(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if intValue, err := strconv.Atoi(value); err == nil {
			return intValue
		}
	}
	return defaultValue
}

// GetEnvInt64 returns the value of an environment variable as an int64 or a default value.
func GetEnvInt64(key string, defaultValue int64) int64 {
	if value := os.Getenv(key); value != "" {
		if intValue, err := strconv.ParseInt(value, 10, 64); err == nil {
			return intValue
		}
	}
	return defaultValue
}

// GetEnvBool returns the value of an environment variable as a bool or a default value.
func GetEnvBool(key string, defaultValue bool) bool {
	if value := os.Getenv(key); value != "" {
		if boolValue, err := strconv.ParseBool(value); err == nil {
			return boolValue
		}
	}
	return defaultValue
}

// GetEnvDuration returns the value of an environment variable as a duration or a default value.
func GetEnvDuration(key string, defaultValue time.Duration) time.Duration {
	if value := os.Getenv(key); value != "" {
		if duration, err := time.ParseDuration(value); err == nil {
			return duration
		}
	}
	return defaultValue
}
