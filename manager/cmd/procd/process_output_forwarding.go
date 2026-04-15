package main

import (
	"os"
	"strconv"
	"strings"

	"github.com/sandbox0-ai/sandbox0/manager/procd/pkg/process"
	"go.uber.org/zap"
)

const (
	processOutputLogsEnvVar             = "SANDBOX0_PROCESS_LOGS"
	processOutputLogsMaxLineBytesEnvVar = "SANDBOX0_PROCESS_LOG_MAX_BYTES"
)

func configureProcessOutputForwarding(logger *zap.Logger) {
	enabled, err := processOutputLogsEnabled()
	if err != nil {
		if logger != nil {
			logger.Warn("Ignoring invalid process log setting; defaulting to enabled",
				zap.String("env", processOutputLogsEnvVar),
				zap.Error(err),
			)
		}
		enabled = true
	}
	if !enabled {
		process.SetDefaultOutputForwarder(nil)
		if logger != nil {
			logger.Info("Sandbox process output forwarding disabled",
				zap.String("env", processOutputLogsEnvVar),
			)
		}
		return
	}

	maxLineBytes := process.DefaultContainerLogMaxLineBytes
	if raw := strings.TrimSpace(os.Getenv(processOutputLogsMaxLineBytesEnvVar)); raw != "" {
		parsed, parseErr := strconv.Atoi(raw)
		if parseErr != nil || parsed <= 0 {
			if logger != nil {
				logger.Warn("Ignoring invalid process output log line limit",
					zap.String("env", processOutputLogsMaxLineBytesEnvVar),
					zap.String("value", raw),
				)
			}
		} else {
			maxLineBytes = parsed
		}
	}

	process.SetDefaultOutputForwarder(process.NewContainerLogForwarder(process.ContainerLogForwarderOptions{
		Stdout:       os.Stdout,
		Stderr:       os.Stderr,
		MaxLineBytes: maxLineBytes,
	}))
	if logger != nil {
		logger.Info("Sandbox process output forwarding enabled",
			zap.String("env", processOutputLogsEnvVar),
			zap.Int("max_line_bytes", maxLineBytes),
		)
	}
}

func processOutputLogsEnabled() (bool, error) {
	raw := strings.TrimSpace(os.Getenv(processOutputLogsEnvVar))
	if raw == "" {
		return true, nil
	}
	return strconv.ParseBool(raw)
}
