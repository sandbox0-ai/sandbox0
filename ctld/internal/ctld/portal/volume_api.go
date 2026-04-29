package portal

import (
	"net/http"
	"strings"

	apiconfig "github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/db"
	fsserver "github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/fsserver"
	sphttp "github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/http"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/notify"
	"github.com/sirupsen/logrus"
)

const volumeFileAffinityTeamHeader = "X-Sandbox0-Volume-Team-Id"

func newMountedVolumeAPIHandler(storageCfg *apiconfig.StorageProxyConfig, repo *db.Repository, volumes *localVolumeManager, logger *logrus.Logger) http.Handler {
	if repo == nil || volumes == nil || logger == nil {
		return nil
	}

	queueSize := 256
	if storageCfg != nil && storageCfg.WatchEventQueueSize > 0 {
		queueSize = storageCfg.WatchEventQueueSize
	}
	eventHub := notify.NewHub(logger, queueSize)
	fileRPC := fsserver.NewFileSystemServer(volumes, repo, eventHub, notify.NewLocalBroadcaster(eventHub), logger, nil)
	server := sphttp.NewServer(logger, storageCfg, nil, repo, nil, "", nil, nil, nil, volumes, fileRPC, eventHub)

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
