package observability

import (
	"fmt"
	"strings"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// LoggerConfig configures the shared zap logger used by sandbox0 services.
type LoggerConfig struct {
	ServiceName      string
	Level            string
	Development      bool
	OutputPaths      []string
	ErrorOutputPaths []string
	Fields           []zap.Field
}

// NewLogger creates a production JSON zap logger with the shared sandbox0 log format.
func NewLogger(cfg LoggerConfig) (*zap.Logger, error) {
	zapCfg := zap.Config{
		Level:       zap.NewAtomicLevelAt(parseLoggerLevel(cfg.Level)),
		Development: cfg.Development,
		Encoding:    "json",
		EncoderConfig: zapcore.EncoderConfig{
			TimeKey:        "ts",
			LevelKey:       "level",
			NameKey:        "logger",
			CallerKey:      "caller",
			FunctionKey:    zapcore.OmitKey,
			MessageKey:     "msg",
			StacktraceKey:  "stacktrace",
			LineEnding:     zapcore.DefaultLineEnding,
			EncodeLevel:    zapcore.LowercaseLevelEncoder,
			EncodeTime:     zapcore.ISO8601TimeEncoder,
			EncodeDuration: zapcore.SecondsDurationEncoder,
			EncodeCaller:   zapcore.ShortCallerEncoder,
		},
		OutputPaths:      cfg.OutputPaths,
		ErrorOutputPaths: cfg.ErrorOutputPaths,
	}
	if len(zapCfg.OutputPaths) == 0 {
		zapCfg.OutputPaths = []string{"stdout"}
	}
	if len(zapCfg.ErrorOutputPaths) == 0 {
		zapCfg.ErrorOutputPaths = []string{"stderr"}
	}

	logger, err := zapCfg.Build()
	if err != nil {
		return nil, err
	}
	fields := make([]zap.Field, 0, len(cfg.Fields)+2)
	if cfg.ServiceName != "" {
		fields = append(fields, zap.String("service", cfg.ServiceName))
		fields = append(fields, zap.String("service.name", cfg.ServiceName))
	}
	fields = append(fields, cfg.Fields...)
	if len(fields) > 0 {
		logger = logger.With(fields...)
	}
	return logger, nil
}

func parseLoggerLevel(level string) zapcore.Level {
	switch strings.ToLower(strings.TrimSpace(level)) {
	case "debug":
		return zapcore.DebugLevel
	case "warn", "warning":
		return zapcore.WarnLevel
	case "error":
		return zapcore.ErrorLevel
	default:
		return zapcore.InfoLevel
	}
}

// MigrateLogger adapts zap logging to migration packages.
type MigrateLogger interface {
	Printf(format string, args ...any)
	Fatalf(format string, args ...any)
}

// NewMigrateLogger returns a logger compatible with migration libraries.
func NewMigrateLogger(logger *zap.Logger) MigrateLogger {
	return migrateLogger{logger: logger}
}

type migrateLogger struct {
	logger *zap.Logger
}

func (l migrateLogger) Printf(format string, args ...any) {
	if l.logger == nil {
		return
	}
	l.logger.Info(fmt.Sprintf(format, args...))
}

func (l migrateLogger) Fatalf(format string, args ...any) {
	if l.logger == nil {
		return
	}
	l.logger.Fatal(fmt.Sprintf(format, args...))
}
