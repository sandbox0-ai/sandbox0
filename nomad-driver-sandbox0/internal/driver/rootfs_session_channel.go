// Copyright 2026 Sandbox0 Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package driver

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/containerd/errdefs"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
)

type rootFSSessionNodeChannelExecutor struct {
	clusterID     string
	nodeID        string
	nodeUID       string
	control       *protocol.NodeClient
	cleaner       runtimeSlotCleaner
	forker        runtimeSlotRunningForker
	network       *protocol.RuntimeSlotNetworkClient
	networkSource runtimeSlotNetworkPrepareSource
}

var _ protocol.NodeChannelExecutor = (*rootFSSessionNodeChannelExecutor)(nil)
var _ protocol.NodeChannelRunningForkExecutor = (*rootFSSessionNodeChannelExecutor)(nil)
var _ protocol.NodeChannelNetworkExecutor = (*rootFSSessionNodeChannelExecutor)(nil)

type runtimeSlotNetworkPrepareSource interface {
	runtimeSlotNetworkPrepareRequest(protocol.NodeNetworkPrepareControlRequest) (protocol.RuntimeSlotNetworkPrepareRequest, error)
}

type runtimeSlotRunningForker interface {
	CaptureRunningRootFSFork(
		context.Context,
		protocol.NodeChannelTarget,
		protocol.NodeRunningForkControlRequest,
	) (rootfshandoff.RunningForkCheckpointResult, error)
}

func newRootFSSessionNodeChannelAgent(
	config PluginConfig,
	nomadConfig NomadAllocationConfig,
	cleaner runtimeSlotCleaner,
	network *protocol.RuntimeSlotNetworkClient,
) (*protocol.NodeChannelAgentSet, error) {
	if !nomadConfig.RuntimeSlotChannelEnabled {
		return nil, nil
	}
	if network == nil {
		return nil, fmt.Errorf("ctld runtime slot network control is required for the node channel: %w", errdefs.ErrFailedPrecondition)
	}
	rawControlRoot := nomadConfig.RuntimeSlotControlRoot
	controlRoot := filepath.Clean(strings.TrimSpace(rawControlRoot))
	if rawControlRoot != controlRoot || !filepath.IsAbs(controlRoot) || controlRoot == string(filepath.Separator) {
		return nil, fmt.Errorf("runtime slot control root must be a non-root absolute path: %w", errdefs.ErrInvalidArgument)
	}
	if err := os.MkdirAll(controlRoot, 0o750); err != nil {
		return nil, fmt.Errorf("create runtime slot control root: %w", err)
	}
	control, err := protocol.NewNodeClient(protocol.NodeClientConfig{AllowedSocketRoot: controlRoot})
	if err != nil {
		return nil, fmt.Errorf("create runtime slot local control client: %w", err)
	}
	executor := &rootFSSessionNodeChannelExecutor{
		clusterID: strings.TrimSpace(nomadConfig.ClusterID),
		nodeID:    strings.TrimSpace(nomadConfig.NodeID),
		nodeUID:   strings.TrimSpace(nomadConfig.RuntimeSlotNodeUID),
		control:   control,
		cleaner:   cleaner,
		network:   network,
	}
	if forker, ok := cleaner.(runtimeSlotRunningForker); ok {
		executor.forker = forker
	}
	if network != nil {
		source, ok := cleaner.(runtimeSlotNetworkPrepareSource)
		if !ok {
			return nil, fmt.Errorf("runtime slot cleaner cannot resolve ctld network namespaces: %w", errdefs.ErrFailedPrecondition)
		}
		executor.networkSource = source
	}
	if executor.clusterID == "" || executor.nodeID == "" {
		return nil, fmt.Errorf("runtime slot cluster and Nomad node IDs are required: %w", errdefs.ErrInvalidArgument)
	}
	agentConfig := protocol.NodeChannelAgentConfig{
		BaseURL: config.RootFSAuthorityURL, CAFile: config.RootFSAuthorityCAFile,
		ClientCertFile: config.RootFSAuthorityClientCertFile,
		ClientKeyFile:  config.RootFSAuthorityClientKeyFile,
		TokenFile:      config.RootFSAuthorityTokenFile,
		PeerURISAN:     nomadConfig.RuntimeSlotChannelPeerURISAN,
		ClusterID:      executor.clusterID,
		NodeID:         executor.nodeID,
		NodeUID:        executor.nodeUID, NodeBootIDFile: config.RuntimeSlotNodeBootIDFile,
		Executor: executor,
	}
	if executor.forker != nil {
		agentConfig.RunningForkExecutor = executor
	}
	if network != nil {
		agentConfig.NetworkExecutor = executor
	}
	agent, err := protocol.NewNodeChannelAgentSet(protocol.NodeChannelAgentSetConfig{Agent: agentConfig})
	if err != nil {
		return nil, fmt.Errorf("create runtime slot node channel agent set: %w", err)
	}
	return agent, nil
}

func (e *rootFSSessionNodeChannelExecutor) PrepareNetwork(
	ctx context.Context,
	target protocol.NodeChannelTarget,
	request protocol.NodeNetworkPrepareControlRequest,
) (rootfshandoff.NetworkPolicyToken, error) {
	if err := e.validateTarget(target); err != nil {
		return rootfshandoff.NetworkPolicyToken{}, err
	}
	if e.network == nil || e.networkSource == nil {
		return rootfshandoff.NetworkPolicyToken{}, fmt.Errorf("ctld runtime slot network control is unavailable: %w", errdefs.ErrFailedPrecondition)
	}
	local, err := e.networkSource.runtimeSlotNetworkPrepareRequest(request)
	if err != nil {
		return rootfshandoff.NetworkPolicyToken{}, err
	}
	return e.network.Prepare(ctx, local)
}

func (e *rootFSSessionNodeChannelExecutor) Claim(
	ctx context.Context,
	target protocol.NodeChannelTarget,
	request protocol.NodeClaimControlRequest,
) (protocol.NodeControlResponse, error) {
	if err := e.validateTarget(target); err != nil {
		return protocol.NodeControlResponse{}, err
	}
	if e.control == nil {
		return protocol.NodeControlResponse{}, fmt.Errorf("runtime slot local control client is unavailable: %w", errdefs.ErrUnavailable)
	}
	return e.control.Claim(ctx, target.ControlEndpoint, request)
}

func (e *rootFSSessionNodeChannelExecutor) CommandReady(
	ctx context.Context,
	target protocol.NodeChannelTarget,
	request protocol.CommandReadyControlRequest,
) (protocol.NodeControlResponse, error) {
	if err := e.validateTarget(target); err != nil {
		return protocol.NodeControlResponse{}, err
	}
	if e.control == nil {
		return protocol.NodeControlResponse{}, fmt.Errorf("runtime slot local control client is unavailable: %w", errdefs.ErrUnavailable)
	}
	return e.control.CommandReady(ctx, target.ControlEndpoint, request)
}

func (e *rootFSSessionNodeChannelExecutor) RunningFork(
	ctx context.Context,
	target protocol.NodeChannelTarget,
	request protocol.NodeRunningForkControlRequest,
) (rootfshandoff.RunningForkCheckpointResult, error) {
	if err := e.validateTarget(target); err != nil {
		return rootfshandoff.RunningForkCheckpointResult{}, err
	}
	if e.forker == nil {
		return rootfshandoff.RunningForkCheckpointResult{},
			fmt.Errorf("runtime slot running-fork controller is unavailable: %w", errdefs.ErrUnavailable)
	}
	return e.forker.CaptureRunningRootFSFork(ctx, target, request)
}

func (e *rootFSSessionNodeChannelExecutor) Cleanup(
	ctx context.Context,
	target protocol.NodeChannelTarget,
	request protocol.NodeCleanupControlRequest,
) (protocol.NodeCleanupControlProof, error) {
	if err := e.validateTarget(target); err != nil {
		return protocol.NodeCleanupControlProof{}, err
	}
	if e.cleaner == nil {
		return protocol.NodeCleanupControlProof{}, fmt.Errorf("runtime slot local cleaner is unavailable: %w", errdefs.ErrUnavailable)
	}
	return e.cleaner.CleanupRuntimeSlot(ctx, request)
}

func (e *rootFSSessionNodeChannelExecutor) validateTarget(target protocol.NodeChannelTarget) error {
	if e == nil || target.ClusterID != e.clusterID || target.NodeID != e.nodeID || target.NodeUID != e.nodeUID {
		return fmt.Errorf("runtime slot node channel target does not match this node: %w", errdefs.ErrPermissionDenied)
	}
	return nil
}
