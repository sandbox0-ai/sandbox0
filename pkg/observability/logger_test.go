package observability

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"go.uber.org/zap"
)

func TestNewLoggerUsesSharedJSONFormat(t *testing.T) {
	output := filepath.Join(t.TempDir(), "service.log")
	logger, err := NewLogger(LoggerConfig{
		ServiceName: "test-service",
		OutputPaths: []string{
			output,
		},
	})
	if err != nil {
		t.Fatalf("NewLogger() error = %v", err)
	}

	logger.Info("hello")
	_ = logger.Sync()

	entry := readLogEntry(t, output)
	if entry["service"] != "test-service" {
		t.Fatalf("service = %v, want test-service", entry["service"])
	}
	if entry["level"] != "info" {
		t.Fatalf("level = %v, want info", entry["level"])
	}
	if entry["msg"] != "hello" {
		t.Fatalf("msg = %v, want hello", entry["msg"])
	}
	if _, ok := entry["ts"]; !ok {
		t.Fatal("missing ts field")
	}
	if _, ok := entry["caller"]; !ok {
		t.Fatal("missing caller field")
	}
	if _, ok := entry["message"]; ok {
		t.Fatal("unexpected legacy message field")
	}
	if _, ok := entry["timestamp"]; ok {
		t.Fatal("unexpected legacy timestamp field")
	}
}

func TestNewLoggerDefaultsInvalidLevelToInfo(t *testing.T) {
	output := filepath.Join(t.TempDir(), "service.log")
	logger, err := NewLogger(LoggerConfig{
		ServiceName: "test-service",
		Level:       "invalid",
		OutputPaths: []string{
			output,
		},
	})
	if err != nil {
		t.Fatalf("NewLogger() error = %v", err)
	}

	logger.Debug("debug message")
	logger.Info("info message")
	_ = logger.Sync()

	entry := readLogEntry(t, output)
	if entry["msg"] != "info message" {
		t.Fatalf("msg = %v, want info message", entry["msg"])
	}
}

func TestNewMigrateLoggerFormatsMessages(t *testing.T) {
	output := filepath.Join(t.TempDir(), "migration.log")
	logger, err := NewLogger(LoggerConfig{
		ServiceName: "test-service",
		OutputPaths: []string{
			output,
		},
	})
	if err != nil {
		t.Fatalf("NewLogger() error = %v", err)
	}

	migrateLogger := NewMigrateLogger(logger)
	migrateLogger.Printf("applied %d migrations", 3)
	_ = logger.Sync()

	entry := readLogEntry(t, output)
	if entry["msg"] != "applied 3 migrations" {
		t.Fatalf("msg = %v, want applied 3 migrations", entry["msg"])
	}
}

func TestNewMigrateLoggerAllowsNilLogger(t *testing.T) {
	NewMigrateLogger(nil).Printf("ignored")
}

func readLogEntry(t *testing.T, path string) map[string]any {
	t.Helper()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read log file: %v", err)
	}
	var entry map[string]any
	if err := json.Unmarshal(data, &entry); err != nil {
		t.Fatalf("decode log entry: %v", err)
	}
	return entry
}

func TestNewLoggerIncludesExtraFields(t *testing.T) {
	output := filepath.Join(t.TempDir(), "service.log")
	logger, err := NewLogger(LoggerConfig{
		ServiceName: "test-service",
		OutputPaths: []string{
			output,
		},
		Fields: []zap.Field{
			zap.String("region", "us-east-1"),
		},
	})
	if err != nil {
		t.Fatalf("NewLogger() error = %v", err)
	}

	logger.Info("hello")
	_ = logger.Sync()

	entry := readLogEntry(t, output)
	if entry["region"] != "us-east-1" {
		t.Fatalf("region = %v, want us-east-1", entry["region"])
	}
}
