package main

import (
	"fmt"
	"net"
	"regexp"
	"strconv"
	"strings"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/nodeauth"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/nodeauthority"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/runtimeslotreconciler"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/runtimeslotterminal"
	"github.com/sandbox0-ai/sandbox0/pkg/config"
	"go.uber.org/zap"
)

func buildManagerNodeAuthority(
	cfg *config.ManagerConfig,
	store nodeauthority.Store,
) (*nodeauthority.Component, error) {
	if cfg == nil {
		return nil, nil
	}
	nodeConfig := cfg.NodeAuthority
	if !nodeConfig.Enabled {
		if nodeConfig.Terminal.Enabled || strings.TrimSpace(nodeConfig.Terminal.NomadEndpointsFile) != "" {
			return nil, fmt.Errorf("manager node authority must be enabled when terminal reconciliation is configured")
		}
		return nil, nil
	}
	if store == nil {
		return nil, fmt.Errorf("manager node authority store is required")
	}
	host := strings.TrimSpace(nodeConfig.ListenHost)
	if host != nodeConfig.ListenHost {
		return nil, fmt.Errorf("manager node authority listen host must be canonical")
	}
	if nodeConfig.Port < 1 || nodeConfig.Port > 65535 {
		return nil, fmt.Errorf("manager node authority port must be between 1 and 65535")
	}
	identities := make([]nodeauth.CertificateIdentity, 0, len(nodeConfig.Identities))
	for _, identity := range nodeConfig.Identities {
		identities = append(identities, nodeauth.CertificateIdentity{
			CommonName: identity.CommonName, ClusterID: identity.ClusterID,
			NodeID: identity.NodeID, NodeUID: identity.NodeUID, AgentUID: identity.AgentUID,
		})
	}
	return nodeauthority.New(nodeauthority.Config{
		Store: store, Address: net.JoinHostPort(host, strconv.Itoa(nodeConfig.Port)),
		RegionID: cfg.RegionID,
		CertFile: nodeConfig.CertFile, KeyFile: nodeConfig.KeyFile,
		ClientCAFile: nodeConfig.ClientCAFile, Identities: identities,
		WriterLeaseTTL:          nodeConfig.WriterLeaseTTL.Duration,
		WriterRenewalGrace:      nodeConfig.WriterRenewalGrace.Duration,
		RuntimeSlotHeartbeatTTL: nodeConfig.RuntimeSlotHeartbeatTTL.Duration,
		Terminal: runtimeslotterminal.Config{
			Enabled:            nodeConfig.Terminal.Enabled,
			NomadEndpointsFile: nodeConfig.Terminal.NomadEndpointsFile,
			Interval:           nodeConfig.Terminal.Interval.Duration,
			PassTimeout:        nodeConfig.Terminal.PassTimeout.Duration,
			ScanLimit:          nodeConfig.Terminal.ScanLimit,
		},
	})
}

func logManagerRuntimeSlotTerminalPass(logger *zap.Logger, report runtimeslotreconciler.WorkerReport) {
	if logger == nil {
		return
	}
	fields := []zap.Field{
		zap.Int("migration_reservations_released", report.Result.MigrationReservationsReleased),
		zap.Int("candidates", report.Result.Candidates),
		zap.Int("completed", report.Result.Completed),
		zap.Int("skipped", report.Result.Skipped),
		zap.Int("failed", report.Result.Failed),
		zap.Int("carrier_refill_requested", report.Result.RefillRequested),
		zap.Duration("duration", report.Duration),
	}
	if report.Error != nil {
		// A batch may join hundreds of slot errors. Journald can drop an
		// oversized JSON line, hiding the failure and its retry count.
		logger.Warn("Runtime slot terminal reconcile pass failed", append(fields,
			zap.String("first_error", terminalErrorSummary(report.Error)))...)
		return
	}
	if report.Result.Completed > 0 || report.Result.Skipped > 0 || report.Result.RefillRequested > 0 || report.Result.MigrationReservationsReleased > 0 {
		logger.Info("Runtime slot terminal reconcile pass completed", fields...)
	}
}

var terminalSensitiveValue = regexp.MustCompile(`(?i)(Bearer[[:space:]]+|(?:authorization|password|secret|token)[=:][[:space:]]*)[^ ,;]+`)

func terminalErrorSummary(err error) string {
	if err == nil {
		return ""
	}
	first := strings.SplitN(err.Error(), "\n", 2)[0]
	first = terminalSensitiveValue.ReplaceAllString(first, "[REDACTED]")
	runes := []rune(first)
	if len(runes) > 512 {
		first = string(runes[:512])
	}
	return first
}
