package http

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"time"

	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/db"
	pb "github.com/sandbox0-ai/sandbox0/storage-proxy/proto/fs"
	"github.com/sirupsen/logrus"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

const volumeFileAffinityRoutedPodHeader = "X-Sandbox0-Volume-Routed-Pod"

type volumeFilePodResolver interface {
	ResolvePodURL(ctx context.Context, podID string) (*url.URL, error)
}

type volumeOwnerRedirectError struct {
	PodID     string
	TargetURL *url.URL
	Cause     error
}

func (e *volumeOwnerRedirectError) Error() string {
	if e == nil {
		return ""
	}
	if e.Cause != nil {
		return fmt.Sprintf("volume owner %q redirect required: %v", e.PodID, e.Cause)
	}
	return fmt.Sprintf("volume owner %q redirect required", e.PodID)
}

func (e *volumeOwnerRedirectError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

type kubernetesVolumeFilePodResolver struct {
	logger   *logrus.Logger
	client   kubernetes.Interface
	httpPort int
}

func newKubernetesVolumeFilePodResolver(logger *logrus.Logger, client kubernetes.Interface, cfg *config.StorageProxyConfig) volumeFilePodResolver {
	if client == nil || cfg == nil || cfg.HTTPPort == 0 {
		return nil
	}
	return &kubernetesVolumeFilePodResolver{
		logger:   logger,
		client:   client,
		httpPort: cfg.HTTPPort,
	}
}

func (r *kubernetesVolumeFilePodResolver) ResolvePodURL(ctx context.Context, podID string) (*url.URL, error) {
	if r == nil || r.client == nil {
		return nil, errors.New("kubernetes pod resolver unavailable")
	}
	if podID == "" {
		return nil, errors.New("missing pod id")
	}

	var namespace string
	var name string
	if strings.Contains(podID, "/") {
		parts := strings.SplitN(podID, "/", 2)
		namespace = parts[0]
		name = parts[1]
	} else {
		name = podID
	}

	if namespace != "" {
		pod, err := r.client.CoreV1().Pods(namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return nil, err
		}
		return buildStorageProxyPodURL(pod.Status.PodIP, r.httpPort)
	}

	pods, err := r.client.CoreV1().Pods("").List(ctx, metav1.ListOptions{
		FieldSelector: "metadata.name=" + name,
		Limit:         2,
	})
	if err != nil {
		return nil, err
	}
	if len(pods.Items) == 0 {
		return nil, fmt.Errorf("pod %q not found", podID)
	}
	return buildStorageProxyPodURL(pods.Items[0].Status.PodIP, r.httpPort)
}

func buildStorageProxyPodURL(podIP string, httpPort int) (*url.URL, error) {
	if strings.TrimSpace(podIP) == "" {
		return nil, errors.New("pod ip is empty")
	}
	return url.Parse(fmt.Sprintf("http://%s:%d", podIP, httpPort))
}

func (s *Server) prepareOrProxyVolumeFileRequest(w http.ResponseWriter, r *http.Request, volumeID string) (context.Context, *db.SandboxVolume, func(), bool) {
	volumeRecord, err := s.loadAuthorizedVolume(r.Context(), volumeID)
	if err != nil {
		s.writeVolumeFileError(w, err)
		return r.Context(), nil, func() {}, true
	}

	proxied, err := s.proxyVolumeRequestToOwnerIfNeeded(w, r, volumeID)
	if err != nil {
		s.writeVolumeFileError(w, err)
		return r.Context(), nil, func() {}, true
	}
	if proxied {
		return r.Context(), volumeRecord, func() {}, true
	}

	cleanup, err := s.prepareVolumeFileMount(r.Context(), volumeID)
	if err != nil {
		var redirectErr *volumeOwnerRedirectError
		if errors.As(err, &redirectErr) && redirectErr != nil && redirectErr.TargetURL != nil && r.Header.Get(volumeFileAffinityRoutedPodHeader) == "" {
			s.proxyVolumeRequestToOwner(w, r, redirectErr.TargetURL, redirectErr.PodID)
			return r.Context(), volumeRecord, func() {}, true
		}
		s.writeVolumeFileError(w, err)
		return r.Context(), nil, func() {}, true
	}

	return r.Context(), volumeRecord, cleanup, false
}

func (s *Server) prepareVolumeFileMount(ctx context.Context, volumeID string) (func(), error) {
	if s.volMgr == nil || s.fileRPC == nil {
		return func() {}, errVolumeFileUnavailable
	}

	cleanup, err := s.volMgr.AcquireDirectVolumeFileMount(ctx, volumeID, func(mountCtx context.Context) (string, error) {
		resp, err := s.fileRPC.MountVolume(mountCtx, &pb.MountVolumeRequest{
			VolumeId: volumeID,
			Config:   &pb.VolumeConfig{},
		})
		if err != nil {
			return "", translateVolumeRPCError(err)
		}
		if resp == nil || resp.MountSessionId == "" {
			return "", nil
		}
		return resp.MountSessionId, nil
	})
	if err != nil {
		targetURL, ownerPodID, resolveErr := s.resolveRemoteVolumeOwnerURL(ctx, volumeID)
		if resolveErr == nil && targetURL != nil {
			return func() {}, &volumeOwnerRedirectError{
				PodID:     ownerPodID,
				TargetURL: targetURL,
				Cause:     err,
			}
		}
		return func() {}, err
	}
	return cleanup, nil
}

func (s *Server) proxyVolumeRequestToOwnerIfNeeded(w http.ResponseWriter, r *http.Request, volumeID string) (bool, error) {
	if s == nil || s.repo == nil || s.podResolver == nil || volumeID == "" {
		return false, nil
	}
	if r != nil && r.Header.Get(volumeFileAffinityRoutedPodHeader) != "" {
		return false, nil
	}

	targetURL, ownerPodID, err := s.resolveRemoteVolumeOwnerURL(r.Context(), volumeID)
	if err != nil {
		return false, err
	}
	if targetURL == nil {
		return false, nil
	}

	s.proxyVolumeRequestToOwner(w, r, targetURL, ownerPodID)
	return true, nil
}

func (s *Server) resolveRemoteVolumeOwnerURL(ctx context.Context, volumeID string) (*url.URL, string, error) {
	if s == nil || s.repo == nil || s.podResolver == nil || volumeID == "" {
		return nil, "", nil
	}

	heartbeatTimeout := 15
	if s.cfg != nil && s.cfg.HeartbeatTimeout > 0 {
		heartbeatTimeout = s.cfg.HeartbeatTimeout
	}
	mounts, err := s.repo.GetActiveMounts(ctx, volumeID, heartbeatTimeout)
	if err != nil {
		return nil, "", err
	}
	owner := s.selectPreferredVolumeOwner(mounts)
	if owner == nil || owner.PodID == "" || owner.PodID == s.selfPodID {
		return nil, "", nil
	}

	targetURL, err := s.podResolver.ResolvePodURL(ctx, owner.PodID)
	if err != nil {
		return nil, "", err
	}
	return targetURL, owner.PodID, nil
}

func (s *Server) selectPreferredVolumeOwner(mounts []*db.VolumeMount) *db.VolumeMount {
	var chosen *db.VolumeMount
	for _, mount := range mounts {
		if mount == nil || mount.PodID == "" {
			continue
		}
		if s.selfClusterID != "" && mount.ClusterID != "" && mount.ClusterID != s.selfClusterID {
			continue
		}
		if mount.PodID == s.selfPodID {
			return mount
		}
		if chosen == nil {
			chosen = mount
			continue
		}
		if mount.MountedAt.Before(chosen.MountedAt) {
			chosen = mount
			continue
		}
		if mount.MountedAt.Equal(chosen.MountedAt) && mount.PodID < chosen.PodID {
			chosen = mount
		}
	}
	return chosen
}

func (s *Server) proxyVolumeRequestToOwner(w http.ResponseWriter, r *http.Request, targetURL *url.URL, ownerPodID string) {
	if targetURL == nil {
		s.writeVolumeFileError(w, errVolumeFileUnavailable)
		return
	}

	proxy := &httputil.ReverseProxy{
		Director: func(req *http.Request) {
			req.URL.Scheme = targetURL.Scheme
			req.URL.Host = targetURL.Host
			req.Host = targetURL.Host
			req.Header.Set(volumeFileAffinityRoutedPodHeader, ownerPodID)
		},
		Transport: &http.Transport{
			MaxIdleConns:        50,
			MaxIdleConnsPerHost: 20,
			IdleConnTimeout:     90 * time.Second,
		},
		ErrorHandler: func(rw http.ResponseWriter, req *http.Request, err error) {
			if s != nil && s.logger != nil {
				s.logger.WithError(err).WithFields(logrus.Fields{
					"volume_id": req.PathValue("id"),
					"owner_pod": ownerPodID,
					"target":    targetURL.String(),
				}).Warn("Failed to proxy volume file request to owner pod")
			}
			_ = spec.WriteError(rw, http.StatusBadGateway, spec.CodeUnavailable, "volume owner unavailable")
		},
	}

	proxy.ServeHTTP(w, r)
}
