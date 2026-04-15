package main

import (
	"os"
	"strconv"
	"strings"

	"github.com/sandbox0-ai/sandbox0/manager/procd/pkg/process"
	"go.uber.org/zap"
)

const (
	processOutputForwardingEnvVar        = "SANDBOX0_FORWARD_PROCESS_OUTPUT_TO_CONTAINER_LOGS"
	processOutputForwardingMaxLineEnvVar = "SANDBOX0_PROCESS_OUTPUT_LOG_MAX_LINE_BYTES"
)

func configureProcessOutputForwarding(logger *zap.Logger) {
	enabled, err := boolEnv(processOutputForwardingEnvVar)
	if err != nil {
		if logger != nil {
			logger.Warn("Ignoring invalid process output forwarding setting",
				zap.String("env", processOutputForwardingEnvVar),
				zap.Error(err),
			)
		}
		return
	}
	if !enabled {
		process.SetDefaultOutputForwarder(nil)
		return
	}

	maxLineBytes := process.DefaultContainerLogMaxLineBytes
	if raw := strings.TrimSpace(os.Getenv(processOutputForwardingMaxLineEnvVar)); raw != "" {
		parsed, parseErr := strconv.Atoi(raw)
		if parseErr != nil || parsed <= 0 {
			if logger != nil {
				logger.Warn("Ignoring invalid process output log line limit",
					zap.String("env", processOutputForwardingMaxLineEnvVar),
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
			zap.String("env", processOutputForwardingEnvVar),
			zap.Int("max_line_bytes", maxLineBytes),
		)
	}
}

func boolEnv(name string) (bool, error) {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return false, nil
	}
	return strconv.ParseBool(raw)
}
