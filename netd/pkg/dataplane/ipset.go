package dataplane

import (
	"context"
	"fmt"
	"os/exec"

	"go.uber.org/zap"
)

func (dp *DataPlane) ipsetName(kind, sandboxID string) string {
	id := sandboxID
	if len(id) > 8 {
		id = id[:8]
	}
	return fmt.Sprintf("netd-%s-%s", kind, id)
}

func (dp *DataPlane) applyIPSet(ctx context.Context, name string, cidrs []string, rules *SandboxRules) bool {
	if len(cidrs) == 0 {
		return false
	}
	if err := dp.ensureIPSet(ctx, name); err != nil {
		dp.logger.Warn("Failed to ensure ipset, falling back to per-rule",
			zap.String("set", name),
			zap.Error(err),
		)
		return false
	}
	if err := dp.flushIPSet(ctx, name); err != nil {
		dp.logger.Warn("Failed to flush ipset, falling back to per-rule",
			zap.String("set", name),
			zap.Error(err),
		)
		return false
	}
	for _, cidr := range cidrs {
		if err := dp.addIPSetEntry(ctx, name, cidr); err != nil {
			dp.logger.Warn("Failed to add ipset entry, falling back to per-rule",
				zap.String("set", name),
				zap.String("cidr", cidr),
				zap.Error(err),
			)
			return false
		}
	}
	if rules != nil {
		rules.IPSets = append(rules.IPSets, name)
	}
	return true
}

func (dp *DataPlane) ensureIPSet(ctx context.Context, name string) error {
	return dp.runIPSet(ctx, "create", name, "hash:net", "-exist")
}

func (dp *DataPlane) flushIPSet(ctx context.Context, name string) error {
	return dp.runIPSet(ctx, "flush", name)
}

func (dp *DataPlane) addIPSetEntry(ctx context.Context, name, cidr string) error {
	return dp.runIPSet(ctx, "add", name, cidr, "-exist")
}

func (dp *DataPlane) destroyIPSet(ctx context.Context, name string) {
	_ = dp.runIPSet(ctx, "destroy", name)
}

func (dp *DataPlane) runIPSet(ctx context.Context, args ...string) error {
	cmd := exec.CommandContext(ctx, "ipset", args...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		dp.logger.Debug("ipset command failed",
			zap.Strings("args", args),
			zap.String("output", string(output)),
			zap.Error(err),
		)
		return fmt.Errorf("ipset %v: %w (%s)", args, err, string(output))
	}
	return nil
}
