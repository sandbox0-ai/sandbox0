package portal

import (
	"context"
	"fmt"
	"net"
	"os"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/sandbox0-ai/sandbox0/pkg/volumeportal"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

type CSIServer struct {
	csi.UnimplementedIdentityServer
	csi.UnimplementedControllerServer
	csi.UnimplementedNodeServer

	nodeName string
	manager  *Manager
	server   *grpc.Server
	listener net.Listener
}

func NewCSIServer(nodeName string, manager *Manager) *CSIServer {
	return &CSIServer{nodeName: nodeName, manager: manager}
}

func (s *CSIServer) Serve(socketPath string) error {
	if s.manager == nil {
		return fmt.Errorf("portal manager is required")
	}
	listener, err := listenUnix(socketPath)
	if err != nil {
		return err
	}
	s.listener = listener
	s.server = grpc.NewServer()
	csi.RegisterIdentityServer(s.server, s)
	csi.RegisterControllerServer(s.server, s)
	csi.RegisterNodeServer(s.server, s)
	return s.server.Serve(listener)
}

func (s *CSIServer) Stop() {
	if s.server != nil {
		s.server.Stop()
	}
	if s.listener != nil {
		_ = s.listener.Close()
	}
}

func (s *CSIServer) GetPluginInfo(context.Context, *csi.GetPluginInfoRequest) (*csi.GetPluginInfoResponse, error) {
	return &csi.GetPluginInfoResponse{
		Name:          volumeportal.DriverName,
		VendorVersion: "0.1.0",
	}, nil
}

func (s *CSIServer) GetPluginCapabilities(context.Context, *csi.GetPluginCapabilitiesRequest) (*csi.GetPluginCapabilitiesResponse, error) {
	return &csi.GetPluginCapabilitiesResponse{
		Capabilities: []*csi.PluginCapability{{
			Type: &csi.PluginCapability_Service_{
				Service: &csi.PluginCapability_Service{
					Type: csi.PluginCapability_Service_VOLUME_ACCESSIBILITY_CONSTRAINTS,
				},
			},
		}},
	}, nil
}

func (s *CSIServer) Probe(context.Context, *csi.ProbeRequest) (*csi.ProbeResponse, error) {
	return &csi.ProbeResponse{Ready: wrapperspb.Bool(true)}, nil
}

func (s *CSIServer) ControllerGetCapabilities(context.Context, *csi.ControllerGetCapabilitiesRequest) (*csi.ControllerGetCapabilitiesResponse, error) {
	return &csi.ControllerGetCapabilitiesResponse{}, nil
}

func (s *CSIServer) NodeGetInfo(context.Context, *csi.NodeGetInfoRequest) (*csi.NodeGetInfoResponse, error) {
	if s.nodeName == "" {
		host, _ := os.Hostname()
		return &csi.NodeGetInfoResponse{NodeId: host}, nil
	}
	return &csi.NodeGetInfoResponse{NodeId: s.nodeName}, nil
}

func (s *CSIServer) NodeGetCapabilities(context.Context, *csi.NodeGetCapabilitiesRequest) (*csi.NodeGetCapabilitiesResponse, error) {
	return &csi.NodeGetCapabilitiesResponse{
		Capabilities: []*csi.NodeServiceCapability{{
			Type: &csi.NodeServiceCapability_Rpc{
				Rpc: &csi.NodeServiceCapability_RPC{
					Type: csi.NodeServiceCapability_RPC_SINGLE_NODE_MULTI_WRITER,
				},
			},
		}},
	}, nil
}

func (s *CSIServer) NodePublishVolume(ctx context.Context, req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	if req.GetTargetPath() == "" {
		return nil, status.Error(codes.InvalidArgument, "target path is required")
	}
	if req.GetVolumeCapability() == nil {
		return nil, status.Error(codes.InvalidArgument, "volume capability is required")
	}
	pub := publishRequestFromContext(req.GetTargetPath(), req.GetVolumeContext())
	if err := s.manager.PublishPortal(ctx, pub); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &csi.NodePublishVolumeResponse{}, nil
}

func (s *CSIServer) NodeUnpublishVolume(_ context.Context, req *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	if req.GetTargetPath() == "" {
		return nil, status.Error(codes.InvalidArgument, "target path is required")
	}
	if err := s.manager.UnpublishPortal(req.GetTargetPath()); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &csi.NodeUnpublishVolumeResponse{}, nil
}
