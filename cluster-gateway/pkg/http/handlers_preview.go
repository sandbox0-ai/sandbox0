package http

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/cluster-gateway/pkg/client"
	"github.com/sandbox0-ai/sandbox0/cluster-gateway/pkg/middleware"
	mgr "github.com/sandbox0-ai/sandbox0/manager/pkg/service"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	"github.com/sandbox0-ai/sandbox0/pkg/proxy"
	"go.uber.org/zap"
)

const (
	previewCookieName      = "__Host-sandbox0-preview"
	previewBootstrapPath   = "/.sandbox0/preview/bootstrap"
	previewProxyPermission = "sandbox.preview.proxy"
	defaultPreviewTTL      = 15 * time.Minute
	maximumPreviewTTL      = time.Hour
	minimumPreviewTTL      = 30 * time.Second
)

type sandboxPreviewCreateRequest struct {
	Port       int    `json:"port" binding:"required,min=1,max=65535"`
	Protocol   string `json:"protocol"`
	Path       string `json:"path"`
	TTLSeconds int    `json:"ttl_seconds"`
}

type sandboxPreviewRenewRequest struct {
	TTLSeconds int `json:"ttl_seconds"`
}

type sandboxPreviewGrant struct {
	ID                string    `json:"id"`
	SandboxID         string    `json:"sandbox_id"`
	Port              int       `json:"port"`
	Protocol          string    `json:"protocol"`
	URL               string    `json:"url"`
	TargetURL         string    `json:"target_url"`
	ExpiresAt         time.Time `json:"expires_at"`
	RuntimeGeneration int64     `json:"runtime_generation"`
}

func (s *Server) createSandboxPreview(c *gin.Context) {
	if s.previewGrants == nil {
		spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "preview authorization store is unavailable")
		return
	}
	var request sandboxPreviewCreateRequest
	if err := c.ShouldBindJSON(&request); err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "invalid preview request")
		return
	}
	protocol, targetPath, ttl, err := normalizePreviewRequest(request.Protocol, request.Path, request.TTLSeconds)
	if err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, err.Error())
		return
	}
	sandboxID := strings.TrimSpace(c.Param("id"))
	sandbox, _, err := s.getSandboxAndProcdURL(c, sandboxID)
	if err != nil {
		return
	}
	if basePort, parseErr := portFromURL(sandbox.InternalAddr); parseErr == nil && basePort == request.Port {
		spec.JSONError(c, http.StatusForbidden, spec.CodeForbidden, "reserved port is not previewable")
		return
	}
	targetURL, err := s.previewTargetURL(sandboxID, request.Port, targetPath)
	if err != nil {
		spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, err.Error())
		return
	}
	id, err := randomPreviewValue(18)
	if err != nil {
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to create preview authorization")
		return
	}
	bootstrapSecret, err := randomPreviewValue(32)
	if err != nil {
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to create preview authorization")
		return
	}
	authCtx := middleware.GetAuthContext(c)
	expiresAt := time.Now().Add(ttl).UTC()
	record := previewGrantRecord{
		ID:                id,
		SandboxID:         sandboxID,
		TeamID:            authCtx.TeamID,
		UserID:            authCtx.UserID,
		Port:              request.Port,
		Protocol:          protocol,
		RuntimeGeneration: sandbox.RuntimeGeneration,
		BootstrapHash:     hashPreviewSecret(bootstrapSecret),
		ExpiresAt:         expiresAt,
	}
	if err := s.previewGrants.Put(c.Request.Context(), record); err != nil {
		s.logger.Warn("Failed to store preview grant", zap.String("sandbox_id", sandboxID), zap.Error(err))
		spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "preview authorization store is unavailable")
		return
	}
	bootstrapURL := *targetURL
	bootstrapURL.Path = previewBootstrapPath
	bootstrapURL.RawPath = ""
	query := bootstrapURL.Query()
	query.Set("token", id+"."+bootstrapSecret)
	query.Set("next", targetPath)
	bootstrapURL.RawQuery = query.Encode()
	bootstrapURL.Fragment = ""
	c.Header("Cache-Control", "no-store")
	spec.JSONSuccess(c, http.StatusCreated, previewGrantResponse(record, bootstrapURL.String(), targetURL.String()))
}

func (s *Server) renewSandboxPreview(c *gin.Context) {
	if s.previewGrants == nil {
		spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "preview authorization store is unavailable")
		return
	}
	var request sandboxPreviewRenewRequest
	if err := c.ShouldBindJSON(&request); err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "invalid preview renewal request")
		return
	}
	_, _, ttl, err := normalizePreviewRequest("http", "/", request.TTLSeconds)
	if err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, err.Error())
		return
	}
	id := strings.TrimSpace(c.Param("preview_id"))
	record, err := s.previewGrants.Get(c.Request.Context(), id)
	if err != nil {
		writePreviewStoreLookupError(c, err)
		return
	}
	authCtx := middleware.GetAuthContext(c)
	if record.SandboxID != c.Param("id") || record.TeamID != authCtx.TeamID || record.UserID != authCtx.UserID {
		spec.JSONError(c, http.StatusNotFound, spec.CodeNotFound, "preview grant not found")
		return
	}
	sandbox, _, err := s.getSandboxAndProcdURL(c, record.SandboxID)
	if err != nil {
		return
	}
	if sandbox.RuntimeGeneration != record.RuntimeGeneration {
		_ = s.previewGrants.Delete(c.Request.Context(), id)
		spec.JSONError(c, http.StatusConflict, spec.CodeConflict, "preview grant belongs to a previous sandbox runtime")
		return
	}
	record, err = s.previewGrants.Renew(c.Request.Context(), id, time.Now().Add(ttl).UTC())
	if err != nil {
		writePreviewStoreLookupError(c, err)
		return
	}
	if record.SandboxID != c.Param("id") || record.TeamID != authCtx.TeamID || record.UserID != authCtx.UserID || record.RuntimeGeneration != sandbox.RuntimeGeneration {
		_ = s.previewGrants.Delete(c.Request.Context(), id)
		spec.JSONError(c, http.StatusNotFound, spec.CodeNotFound, "preview grant not found")
		return
	}
	targetURL, err := s.previewTargetURL(record.SandboxID, record.Port, "/")
	if err != nil {
		spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, err.Error())
		return
	}
	c.Header("Cache-Control", "no-store")
	spec.JSONSuccess(c, http.StatusOK, previewGrantResponse(record, targetURL.String(), targetURL.String()))
}

func (s *Server) deleteSandboxPreview(c *gin.Context) {
	if s.previewGrants == nil {
		spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "preview authorization store is unavailable")
		return
	}
	id := strings.TrimSpace(c.Param("preview_id"))
	record, err := s.previewGrants.Get(c.Request.Context(), id)
	if err != nil {
		writePreviewStoreLookupError(c, err)
		return
	}
	authCtx := middleware.GetAuthContext(c)
	if record.SandboxID != c.Param("id") || record.TeamID != authCtx.TeamID || record.UserID != authCtx.UserID {
		spec.JSONError(c, http.StatusNotFound, spec.CodeNotFound, "preview grant not found")
		return
	}
	if err := s.previewGrants.Delete(c.Request.Context(), id); err != nil {
		spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "preview authorization store is unavailable")
		return
	}
	spec.JSONSuccess(c, http.StatusOK, gin.H{"message": "preview grant revoked"})
}

func writePreviewStoreLookupError(c *gin.Context, err error) {
	if errors.Is(err, errPreviewGrantNotFound) || errors.Is(err, errPreviewBootstrapInvalid) {
		spec.JSONError(c, http.StatusNotFound, spec.CodeNotFound, "preview grant not found")
		return
	}
	spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "preview authorization store is unavailable")
}

func normalizePreviewRequest(protocol, path string, ttlSeconds int) (string, string, time.Duration, error) {
	protocol = strings.ToLower(strings.TrimSpace(protocol))
	if protocol == "" {
		protocol = "http"
	}
	if protocol != "http" && protocol != "https" {
		return "", "", 0, fmt.Errorf("protocol must be http or https")
	}
	path, err := normalizePreviewPath(path)
	if err != nil {
		return "", "", 0, err
	}
	ttl := defaultPreviewTTL
	if ttlSeconds != 0 {
		ttl = time.Duration(ttlSeconds) * time.Second
	}
	if ttl < minimumPreviewTTL || ttl > maximumPreviewTTL {
		return "", "", 0, fmt.Errorf("ttl_seconds must be between %d and %d", int(minimumPreviewTTL.Seconds()), int(maximumPreviewTTL.Seconds()))
	}
	return protocol, path, ttl, nil
}

func normalizePreviewPath(path string) (string, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		path = "/"
	}
	parsed, err := url.Parse(path)
	if err != nil || !strings.HasPrefix(path, "/") || strings.HasPrefix(path, "//") || strings.Contains(path, "\\") || parsed.IsAbs() || parsed.Host != "" {
		return "", fmt.Errorf("path must be a same-origin absolute path")
	}
	return path, nil
}

func (s *Server) previewTargetURL(sandboxID string, port int, path string) (*url.URL, error) {
	if s.cfg == nil || !s.cfg.PublicExposureEnabled {
		return nil, fmt.Errorf("public exposure is disabled")
	}
	region := strings.Trim(strings.TrimSpace(s.cfg.PublicRegionID), ".")
	if region == "" {
		return nil, fmt.Errorf("preview domain is not configured")
	}
	root := strings.Trim(strings.TrimSpace(s.cfg.PublicRootDomain), ".")
	if root == "" {
		root = defaultPublicRootDomain
	}
	label, err := naming.BuildExposureHostLabel(sandboxID, port)
	if err != nil {
		return nil, fmt.Errorf("invalid preview address")
	}
	target, err := url.Parse("https://" + label + "." + region + "." + root + path)
	if err != nil {
		return nil, fmt.Errorf("invalid preview address")
	}
	return target, nil
}

func previewGrantResponse(record previewGrantRecord, accessURL, targetURL string) sandboxPreviewGrant {
	return sandboxPreviewGrant{
		ID:                record.ID,
		SandboxID:         record.SandboxID,
		Port:              record.Port,
		Protocol:          record.Protocol,
		URL:               accessURL,
		TargetURL:         targetURL,
		ExpiresAt:         record.ExpiresAt,
		RuntimeGeneration: record.RuntimeGeneration,
	}
}

func randomPreviewValue(size int) (string, error) {
	value := make([]byte, size)
	if _, err := rand.Read(value); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(value), nil
}

func hashPreviewSecret(secret string) string {
	hash := sha256.Sum256([]byte(secret))
	return hex.EncodeToString(hash[:])
}

func splitPreviewCredential(value string) (string, string, bool) {
	id, secret, ok := strings.Cut(strings.TrimSpace(value), ".")
	return id, secret, ok && id != "" && secret != ""
}

func (s *Server) handlePreviewBootstrap(c *gin.Context, sandboxID string, port int) {
	id, bootstrapSecret, ok := splitPreviewCredential(c.Query("token"))
	if !ok {
		spec.JSONError(c, http.StatusNotFound, spec.CodeNotFound, "preview authorization not found")
		return
	}
	sessionSecret, err := randomPreviewValue(32)
	if err != nil {
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to activate preview authorization")
		return
	}
	record, err := s.previewGrants.ConsumeBootstrap(c.Request.Context(), id, hashPreviewSecret(bootstrapSecret), hashPreviewSecret(sessionSecret))
	if err != nil || record.SandboxID != sandboxID || record.Port != port {
		spec.JSONError(c, http.StatusNotFound, spec.CodeNotFound, "preview authorization not found")
		return
	}
	next := c.Query("next")
	if normalized, normalizeErr := normalizePreviewPath(next); normalizeErr == nil {
		next = normalized
	} else {
		next = "/"
	}
	http.SetCookie(c.Writer, &http.Cookie{
		Name:        previewCookieName,
		Value:       id + "." + sessionSecret,
		Path:        "/",
		HttpOnly:    true,
		Secure:      true,
		SameSite:    http.SameSiteNoneMode,
		Partitioned: true,
	})
	c.Header("Cache-Control", "no-store")
	c.Header("Referrer-Policy", "no-referrer")
	c.Redirect(http.StatusSeeOther, next)
}

func (s *Server) previewGrantForRequest(c *gin.Context, sandboxID string, port int) (previewGrantRecord, bool) {
	cookie, err := c.Request.Cookie(previewCookieName)
	if err != nil {
		return previewGrantRecord{}, false
	}
	id, sessionSecret, ok := splitPreviewCredential(cookie.Value)
	if !ok {
		return previewGrantRecord{}, false
	}
	record, err := s.previewGrants.Get(c.Request.Context(), id)
	if err != nil || record.SandboxID != sandboxID || record.Port != port || record.SessionHash == "" || !secureStringEqual(record.SessionHash, hashPreviewSecret(sessionSecret)) {
		return previewGrantRecord{}, false
	}
	return record, true
}

func (s *Server) proxySandboxPreview(c *gin.Context, sandbox *mgr.Sandbox, record previewGrantRecord) {
	if sandbox.TeamID != record.TeamID || sandbox.RuntimeGeneration != record.RuntimeGeneration || sandboxNeedsRuntime(sandbox) {
		spec.JSONError(c, http.StatusUnauthorized, spec.CodeUnauthorized, "preview authorization is no longer valid")
		return
	}
	procdURL, err := url.Parse(sandbox.InternalAddr)
	if err != nil {
		spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "sandbox runtime is unavailable")
		return
	}
	token, err := s.internalAuthGen.Generate("procd", record.TeamID, record.UserID, internalauth.GenerateOptions{
		SandboxID:   record.SandboxID,
		Permissions: []string{previewProxyPermission},
	})
	if err != nil {
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "internal authentication failed")
		return
	}
	originalPath := c.Request.URL.Path
	c.Request.URL.Path = "/api/v1/preview/" + record.Protocol + "/" + strconv.Itoa(record.Port) + originalPath
	c.Request = proxy.WithUpstreamTimeoutDisabledRequest(c.Request)
	_ = proxy.DisableResponseWriteDeadline(c.Writer)
	modifier := func(request *http.Request) {
		request.Header.Set(internalauth.TeamIDHeader, record.TeamID)
		request.Header.Set(internalauth.DefaultTokenHeader, token)
		request.Header.Set("X-Sandbox0-Preview-Origin", "https://"+c.Request.Host)
		removeRequestCookie(request, previewCookieName)
	}
	router, err := proxy.NewRouter(procdURL.String(), s.logger, s.cfg.ProxyTimeout.Duration, proxy.WithRequestModifier(modifier), proxy.WithHTTPClient(s.outboundHTTPClient()))
	if err != nil {
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "preview proxy initialization failed")
		return
	}
	router.ProxyToTarget(c)
}

func removeRequestCookie(request *http.Request, name string) {
	cookies := request.Cookies()
	request.Header.Del("Cookie")
	for _, cookie := range cookies {
		if cookie.Name != name {
			request.AddCookie(cookie)
		}
	}
}

func (s *Server) getSandboxForPreview(c *gin.Context, sandboxID string) (*mgr.Sandbox, error) {
	sandbox, err := s.getSandboxInternalCached(c.Request.Context(), sandboxID)
	if err != nil {
		if errors.Is(err, client.ErrSandboxNotFound) {
			spec.JSONError(c, http.StatusNotFound, spec.CodeNotFound, "sandbox not found")
		} else {
			spec.JSONError(c, http.StatusServiceUnavailable, spec.CodeUnavailable, "manager service unavailable")
		}
		return nil, err
	}
	return sandbox, nil
}
