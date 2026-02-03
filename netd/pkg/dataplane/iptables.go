package dataplane

import (
	"os/exec"

	"go.uber.org/zap"
)

func resolveIptablesBinary(preferNFT bool, logger *zap.Logger) string {
	if preferNFT {
		if path, err := exec.LookPath("iptables-nft"); err == nil && path != "" {
			logger.Info("Using iptables-nft backend", zap.String("binary", path))
			return path
		}
	}
	if path, err := exec.LookPath("iptables"); err == nil && path != "" {
		if preferNFT {
			logger.Warn("iptables-nft not found, falling back to iptables",
				zap.String("binary", path),
			)
		}
		return path
	}
	if preferNFT {
		logger.Warn("iptables-nft not found, using iptables as fallback")
	}
	return "iptables"
}
