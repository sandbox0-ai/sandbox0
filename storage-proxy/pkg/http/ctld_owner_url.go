package http

import (
	"context"
	"net/http"
	"net/url"

	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/db"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/volume"
)

func (s *Server) resolveVolumeMountURL(ctx context.Context, mount *db.VolumeMount) (*url.URL, error) {
	if s == nil || s.podResolver == nil || mount == nil || mount.PodID == "" {
		return nil, nil
	}
	targetURL, err := s.podResolver.ResolvePodURL(ctx, mount.PodID)
	if err != nil {
		return nil, err
	}
	if targetURL == nil {
		return nil, nil
	}
	targetURL = cloneURL(targetURL)
	opts := volume.DecodeMountOptions(mount.MountOptions)
	ownerPort := opts.OwnerPort
	if ownerPort == 0 && opts.OwnerKind == volume.OwnerKindCtld {
		ownerPort = 8095
	}
	if ownerPort > 0 {
		targetURL.Host = hostWithPort(targetURL.Host, ownerPort)
	}
	return targetURL, nil
}

func (s *Server) ctldHTTPClientOrDefault() *http.Client {
	if s != nil && s.ctldHTTPClient != nil {
		return s.ctldHTTPClient
	}
	return &http.Client{Timeout: ctldapi.DefaultRequestTimeout}
}
