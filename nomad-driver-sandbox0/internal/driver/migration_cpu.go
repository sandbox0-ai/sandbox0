package driver

import (
	"context"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/gvisorcli"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
)

// Optional migration evidence must not consume the ordinary create/start
// deadline. Hosts unable to finish this native check remain ineligible.
const migrationCPULaunchVerifyTimeout = 100 * time.Millisecond

// CPU eligibility is prepared before warm-slot advertisement. Failure preserves
// ordinary runtime service but leaves future migration without launch evidence.
func (h *taskHandle) prepareMigrationCPULaunch(ctx context.Context) {
	if recorder, ok := h.runner.(gvisorcli.LaunchCPURecorder); ok {
		if err := recorder.PrepareCPULaunch(ctx); err != nil {
			h.logger.Warn("migration CPU warm observation unavailable", "error", err)
		}
	}
}

func (h *taskHandle) beginMigrationCPULaunch(ctx context.Context, request ClaimRequest) gvisorcli.CPULaunchVerifier {
	if request.MigrationRestore != nil || request.Stage == nil || request.Runtime == nil {
		return nil
	}
	recorder, ok := h.runner.(gvisorcli.LaunchCPURecorder)
	if !ok {
		return nil
	}
	witness, err := recorder.BeginCPULaunch(ctx, request.Resources.CPUSetCPUs)
	if err != nil {
		h.logger.Warn("migration CPU launch recording unavailable", "error", err)
		return nil
	}
	return witness
}

func (h *taskHandle) completeMigrationCPULaunch(ctx context.Context, request ClaimRequest, witness gvisorcli.CPULaunchVerifier) *protocol.MigrationCPULaunch {
	if witness == nil {
		return nil
	}
	observation, executable, err := witness.Complete(ctx)
	if err != nil {
		h.logger.Warn("migration CPU launch verification failed", "error", err)
		return nil
	}
	if observation == nil {
		return nil
	}
	target := protocol.NodeChannelTarget{SlotID: h.taskConfig.ID, ClusterID: request.Resources.ClusterID,
		AllocationID: h.taskConfig.AllocID, NodeID: h.taskConfig.NodeID, NodeUID: request.Stage.Identity.NodeUID,
		NodeBootID: request.Stage.Identity.BootID, ControlEndpoint: "unix://" + h.socketPath}
	launch, err := protocol.BindMigrationCPULaunch(target, request, *observation, executable)
	if err != nil {
		h.logger.Warn("migration CPU launch binding failed", "error", err)
		return nil
	}
	return launch
}
