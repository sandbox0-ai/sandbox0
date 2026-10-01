package driver

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/processidentity"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
)

// A plugin replacement can preserve an exact running guest only when the node
// still owns its original physical writer. A missing owner remains crash
// recovery; an unavailable serving owner never authorizes destructive fallback.
func (h *taskHandle) recoverLiveRootFS() (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	info, err := loadRootFSRuntimeInfo(ctx, h.rootfs)
	if err != nil {
		return true, err
	}
	if info.LiveUpdateProtocol != 1 {
		return false, nil
	}
	adopter, ok := h.rootfs.(interface {
		AdoptLiveConsumer(context.Context, rootfshandoff.StageRequest, RootFSConsumerRequest) (RootFSConsumerLease, error)
	})
	if !ok {
		return true, fmt.Errorf("RootFS runtime cannot adopt a live consumer")
	}
	state, err := h.runner.State(ctx, h.containerID)
	if err != nil {
		return true, err
	}
	if strings.ToLower(strings.TrimSpace(state.Status)) != "running" || state.ID != h.containerID || state.Bundle != h.bundleDir || state.PID <= 0 {
		return true, fmt.Errorf("running guest identity cannot be proved")
	}
	namespace, err := os.Readlink("/proc/self/ns/mnt")
	if err != nil {
		return true, err
	}
	owner, err := processidentity.Current()
	if err != nil {
		return true, err
	}
	consumer := RootFSConsumerRequest{ActiveKey: h.taskConfig.ID, ContainerID: h.containerID, StableMount: h.rootMount, HostMountNamespace: namespace, RenewalProtocol: 1, OwnerProcess: owner}
	if h.netnsPath() != "" && h.stage.ExpectedPolicyToken.NetNSIdentity != "" {
		consumer.NetNSPath = h.netnsPath()
		consumer.NetNSIdentity = h.stage.ExpectedPolicyToken.NetNSIdentity
		consumer.NetworkChain = h.networkChain
	}
	lease, err := adopter.AdoptLiveConsumer(ctx, *h.stage, consumer)
	if err != nil {
		return true, err
	}
	if lease.LeaseID == "" || !lease.ExpiresAt.After(time.Now()) {
		return true, fmt.Errorf("adopted consumer lease is invalid")
	}
	h.recoveredConsumerLease = &lease
	h.setPhase(phaseActive)
	h.startExitWatch()
	return true, nil
}
