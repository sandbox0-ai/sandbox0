package portal

import (
	"context"
	"net/http"
	"strings"

	apiconfig "github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/db"
	fsserver "github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/fsserver"
	sphttp "github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/http"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/notify"
	pb "github.com/sandbox0-ai/sandbox0/storage-proxy/proto/fs"
	"github.com/sirupsen/logrus"
)

const volumeFileAffinityTeamHeader = "X-Sandbox0-Volume-Team-Id"

type mountedVolumeOwnerAttacher interface {
	AttachOwner(ctx context.Context, req ctldapi.AttachVolumeOwnerRequest) (ctldapi.AttachVolumeOwnerResponse, error)
}

type mountedVolumeFileRPC struct {
	*fsserver.FileSystemServer
	volumes  *localVolumeManager
	attacher mountedVolumeOwnerAttacher
}

func (r *mountedVolumeFileRPC) MountVolume(ctx context.Context, req *pb.MountVolumeRequest) (*pb.MountVolumeResponse, error) {
	if r == nil || r.FileSystemServer == nil {
		return nil, nil
	}
	if req == nil || strings.TrimSpace(req.VolumeId) == "" {
		return r.FileSystemServer.MountVolume(ctx, req)
	}
	if r.volumes != nil {
		if _, err := r.volumes.GetVolume(req.VolumeId); err == nil {
			return r.FileSystemServer.MountVolume(ctx, req)
		}
	}
	claims := internalauth.ClaimsFromContext(ctx)
	if r.attacher != nil && claims != nil && strings.TrimSpace(claims.TeamID) != "" {
		if _, err := r.attacher.AttachOwner(ctx, ctldapi.AttachVolumeOwnerRequest{
			TeamID:          claims.TeamID,
			SandboxVolumeID: req.VolumeId,
		}); err != nil {
			return nil, err
		}
	}
	return r.FileSystemServer.MountVolume(ctx, req)
}

func newMountedVolumeAPIHandler(storageCfg *apiconfig.StorageProxyConfig, repo *db.Repository, volumes *localVolumeManager, logger *logrus.Logger, attacher mountedVolumeOwnerAttacher) http.Handler {
	if repo == nil || volumes == nil || logger == nil {
		return nil
	}

	queueSize := 256
	if storageCfg != nil && storageCfg.WatchEventQueueSize > 0 {
		queueSize = storageCfg.WatchEventQueueSize
	}
	eventHub := notify.NewHub(logger, queueSize)
	fs := fsserver.NewFileSystemServer(volumes, repo, eventHub, notify.NewLocalBroadcaster(eventHub), logger, nil, nil)
	fileRPC := &mountedVolumeFileRPC{FileSystemServer: fs, volumes: volumes, attacher: attacher}
	server := sphttp.NewServer(logger, storageCfg, nil, repo, nil, "", nil, nil, nil, nil, volumes, fileRPC, eventHub)

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !isMountedVolumeAPIPath(r.URL.Path) {
			http.NotFound(w, r)
			return
		}

		teamID := strings.TrimSpace(r.Header.Get(volumeFileAffinityTeamHeader))
		if teamID == "" {
			http.Error(w, "missing team id", http.StatusUnauthorized)
			return
		}

		claims := &internalauth.Claims{
			Caller:   internalauth.ServiceStorageProxy,
			Target:   internalauth.ServiceCtld,
			TeamID:   teamID,
			IsSystem: true,
		}
		server.ServeHTTP(w, r.WithContext(internalauth.WithClaims(r.Context(), claims)))
	})
}

func isMountedVolumeAPIPath(path string) bool {
	if !strings.HasPrefix(path, "/sandboxvolumes/") {
		return false
	}
	return strings.Contains(path, "/files")
}
