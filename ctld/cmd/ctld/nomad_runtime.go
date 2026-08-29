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

package main

import (
	"strings"

	apiconfig "github.com/sandbox0-ai/sandbox0/pkg/config"
	"github.com/sandbox0-ai/sandbox0/pkg/nomadruntime"
	"go.uber.org/zap"
)

type nomadRuntimeFactory func(*zap.Logger) (primaryService, error)

func configuredNomadRuntimeFactory(
	ctldConfig *apiconfig.CtldConfig,
	networkSocket string,
) (nomadRuntimeFactory, error) {
	if ctldConfig == nil || !ctldConfig.NomadRuntime.Enabled {
		return nil, nil
	}
	source := ctldConfig.NomadRuntime
	directFS := true
	if source.DirectFS != nil {
		directFS = *source.DirectFS
	}
	runtimeConfig := nomadruntime.Config{
		SocketPath:                            source.SocketPath,
		RunscPath:                             source.RunscPath,
		RunscRoot:                             source.RunscRoot,
		Platform:                              source.Platform,
		Overlay2:                              source.Overlay2,
		FileAccess:                            source.FileAccess,
		DirectFS:                              directFS,
		RootFSStatePath:                       source.StatePath,
		RootFSBranchRoot:                      source.BranchRoot,
		RootFSMountRoot:                       source.MountRoot,
		RootFSConsumerMountRoot:               source.ConsumerMountRoot,
		RootFSConsumerNetNSRoot:               source.ConsumerNetNSRoot,
		RootFSMaxDirtyTailBytes:               source.MaxDirtyTailBytes,
		RootFSMaxNodeDirtyTailBytes:           source.MaxNodeDirtyTailBytes,
		RootFSDirtyTailRetirementReserveBytes: source.DirtyTailRetirementReserveBytes,
		RootFSNBDDevices:                      append([]string(nil), source.NBDDevices...),
		RootFSObjectType:                      ctldConfig.RootFSObjectStorage.Type,
		RootFSObjectBucket:                    ctldConfig.RootFSObjectStorage.Bucket,
		RootFSObjectRegion:                    ctldConfig.RootFSObjectStorage.Region,
		RootFSObjectEndpoint:                  ctldConfig.RootFSObjectStorage.Endpoint,
		RootFSObjectAccessKey:                 ctldConfig.RootFSObjectStorage.AccessKey,
		RootFSObjectSecretKey:                 ctldConfig.RootFSObjectStorage.SecretKey,
		RootFSObjectSessionToken:              ctldConfig.RootFSObjectStorage.SessionToken,
		RootFSObjectEncryptionEnabled:         ctldConfig.RootFSObjectStorage.ObjectEncryptionEnabled,
		RootFSObjectEncryptionKeyPath:         ctldConfig.RootFSObjectStorage.ObjectEncryptionKeyPath,
		RootFSObjectEncryptionPassphrase:      ctldConfig.RootFSObjectStorage.ObjectEncryptionPassphrase,
		RootFSObjectEncryptionAlgorithm:       ctldConfig.RootFSObjectStorage.ObjectEncryptionAlgo,
		RootFSAuthorityURL:                    source.AuthorityURL,
		RootFSAuthorityCAFile:                 source.AuthorityCAFile,
		RootFSAuthorityClientCertFile:         source.AuthorityClientCertFile,
		RootFSAuthorityClientKeyFile:          source.AuthorityClientKeyFile,
		RootFSAuthorityTokenFile:              source.AuthorityTokenFile,
		RuntimeSlotNodeBootIDFile:             source.NodeBootIDFile,
		RuntimeSlotJournalPath:                source.RuntimeSlotJournalPath,
		RuntimeResourceCgroupRoot:             source.ResourceCgroupRoot,
	}
	runtimeConfig.ApplyDefaults()
	nomadConfig := nomadruntime.NomadAllocationConfig{
		ClusterID:                     strings.TrimSpace(ctldConfig.DefaultClusterId),
		Address:                       source.NomadAddress,
		NodeID:                        source.NomadNodeID,
		Namespace:                     source.NomadNamespace,
		JobID:                         source.NomadJobID,
		TokenFile:                     source.NomadTokenFile,
		CAFile:                        source.NomadCAFile,
		CertFile:                      source.NomadCertFile,
		KeyFile:                       source.NomadKeyFile,
		RuntimeSlotChannelEnabled:     true,
		RuntimeSlotNodeUID:            source.NodeUID,
		RuntimeSlotChannelPeerURISAN:  source.AuthorityPeerURISAN,
		RuntimeSlotControlRoot:        source.ControlRoot,
		RuntimeSlotNodeControlTimeout: source.NodeControlTimeout.Duration,
		RuntimeSlotCtldNetworkSocket:  networkSocket,
		RuntimeResourceCPUMillicores:  source.ResourceCPUMillicores,
		RuntimeResourceMemoryBytes:    source.ResourceMemoryBytes,
		RuntimeResourceCPUSetCPUs:     source.ResourceCPUSetCPUs,
		RuntimeResourceCPUSetMems:     source.ResourceCPUSetMems,
	}
	// NewService performs only static validation. Exclusive node resources are
	// opened later, after this ctld instance acquires the HA primary lease.
	if _, err := nomadruntime.NewService(runtimeConfig, nomadConfig, zap.NewNop()); err != nil {
		return nil, err
	}
	return func(logger *zap.Logger) (primaryService, error) {
		return nomadruntime.NewService(runtimeConfig, nomadConfig, logger)
	}, nil
}
