package portal

import (
	"context"
	"testing"

	"github.com/sandbox0-ai/sandbox0/pkg/teamquota/activeconnections"
	"github.com/sandbox0-ai/sandbox0/pkg/teamquota/storageoperations"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/storagequota"
)

type permissiveActiveConnectionQuota struct{}

func (*permissiveActiveConnectionQuota) Acquire(context.Context, string) (activeconnections.Lease, error) {
	return nil, nil
}

func (*permissiveActiveConnectionQuota) Close() error { return nil }

type recordingMountedVolumeTeamQuotaServer struct {
	storageQuotaSet      bool
	storageOperationsSet bool
	activeConnectionsSet bool
	storageQuota         *storagequota.Service
	storageOperations    storageoperations.Quota
	activeConnections    activeconnections.Quota
}

func (s *recordingMountedVolumeTeamQuotaServer) SetStorageQuota(quota *storagequota.Service) {
	s.storageQuotaSet = true
	s.storageQuota = quota
}

func (s *recordingMountedVolumeTeamQuotaServer) SetStorageOperationQuota(quota storageoperations.Quota) {
	s.storageOperationsSet = true
	s.storageOperations = quota
}

func (s *recordingMountedVolumeTeamQuotaServer) SetActiveConnectionQuota(quota activeconnections.Quota) {
	s.activeConnectionsSet = true
	s.activeConnections = quota
}

func TestConfigureMountedVolumeTeamQuotaWiresDistributedQuotas(t *testing.T) {
	server := &recordingMountedVolumeTeamQuotaServer{}
	operations := permissiveStorageOperationQuota{}
	connections := &permissiveActiveConnectionQuota{}

	configureMountedVolumeTeamQuota(server, nil, operations, connections)

	if !server.storageQuotaSet {
		t.Fatal("storage quota was not configured")
	}
	if !server.storageOperationsSet {
		t.Fatal("storage operation quota was not configured")
	}
	if server.storageOperations != operations {
		t.Fatalf("storage operation quota = %#v, want provided quota", server.storageOperations)
	}
	if !server.activeConnectionsSet {
		t.Fatal("active connection quota was not configured")
	}
	if server.activeConnections != connections {
		t.Fatalf("active connection quota = %#v, want provided quota", server.activeConnections)
	}
}
