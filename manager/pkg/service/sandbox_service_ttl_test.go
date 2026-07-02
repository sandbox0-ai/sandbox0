package service

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	corelisters "k8s.io/client-go/listers/core/v1"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"
)

type fixedClock struct {
	now time.Time
}

func (c fixedClock) Now() time.Time                  { return c.now }
func (c fixedClock) Since(t time.Time) time.Duration { return c.now.Sub(t) }
func (c fixedClock) Until(t time.Time) time.Duration { return t.Sub(c.now) }

func newSandboxServiceForTTLTests(t *testing.T, pod *corev1.Pod, defaultTTL time.Duration) (*SandboxService, *fake.Clientset) {
	t.Helper()

	client := fake.NewSimpleClientset(pod.DeepCopy())
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{
		cache.NamespaceIndex: cache.MetaNamespaceIndexFunc,
	})
	require.NoError(t, indexer.Add(pod.DeepCopy()))

	return &SandboxService{
		k8sClient: client,
		podLister: corelisters.NewPodLister(indexer),
		clock: fixedClock{
			now: time.Date(2026, time.March, 7, 12, 0, 0, 0, time.UTC),
		},
		config: SandboxServiceConfig{
			DefaultTTL: defaultTTL,
		},
		logger: zap.NewNop(),
	}, client
}

func testSandboxPod() *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "sandbox-1",
			Namespace: "default",
			Labels: map[string]string{
				controller.LabelSandboxID: "sandbox-1",
			},
			Annotations: map[string]string{
				controller.AnnotationTeamID: "team-1",
				controller.AnnotationUserID: "user-1",
			},
		},
		Status: corev1.PodStatus{
			PodIP: "10.0.0.10",
		},
	}
}

func TestClaimConfigForPersistence(t *testing.T) {
	t.Run("omitted ttl uses default", func(t *testing.T) {
		svc := &SandboxService{config: SandboxServiceConfig{DefaultTTL: 5 * time.Minute}}
		cfg := svc.claimConfigForPersistence(nil)
		require.NotNil(t, cfg)
		require.NotNil(t, cfg.TTL)
		assert.Equal(t, int32(300), *cfg.TTL)
		assert.Nil(t, cfg.HardTTL)
	})

	t.Run("explicit zero remains disabled", func(t *testing.T) {
		svc := &SandboxService{config: SandboxServiceConfig{DefaultTTL: 5 * time.Minute}}
		cfg := svc.claimConfigForPersistence(&SandboxConfig{
			TTL:     int32Ptr(0),
			HardTTL: int32Ptr(0),
		})
		require.NotNil(t, cfg)
		require.NotNil(t, cfg.TTL)
		require.NotNil(t, cfg.HardTTL)
		assert.Equal(t, int32(0), *cfg.TTL)
		assert.Equal(t, int32(0), *cfg.HardTTL)
	})

	t.Run("zero default keeps ttl disabled when omitted", func(t *testing.T) {
		svc := &SandboxService{config: SandboxServiceConfig{DefaultTTL: 0}}
		assert.Nil(t, svc.claimConfigForPersistence(nil))
	})
}

func TestUpdateSandboxZeroTTLDisablesExpirations(t *testing.T) {
	pod := testSandboxPod()
	pod.Annotations[controller.AnnotationExpiresAt] = "2026-03-07T12:05:00Z"
	pod.Annotations[controller.AnnotationHardExpiresAt] = "2026-03-07T12:10:00Z"
	pod.Annotations[controller.AnnotationConfig] = `{"ttl":300,"hard_ttl":600}`

	svc, client := newSandboxServiceForTTLTests(t, pod, 0)

	updated, err := svc.UpdateSandbox(context.Background(), pod.Name, &SandboxUpdateConfig{
		TTL:     int32Ptr(0),
		HardTTL: int32Ptr(0),
	})
	require.NoError(t, err)
	assert.True(t, updated.ExpiresAt.IsZero())
	assert.True(t, updated.HardExpiresAt.IsZero())

	stored, err := client.CoreV1().Pods(pod.Namespace).Get(context.Background(), pod.Name, metav1.GetOptions{})
	require.NoError(t, err)
	assert.Empty(t, stored.Annotations[controller.AnnotationExpiresAt])
	assert.Empty(t, stored.Annotations[controller.AnnotationHardExpiresAt])

	var cfg SandboxConfig
	require.NoError(t, json.Unmarshal([]byte(stored.Annotations[controller.AnnotationConfig]), &cfg))
	require.NotNil(t, cfg.TTL)
	require.NotNil(t, cfg.HardTTL)
	assert.Equal(t, int32(0), *cfg.TTL)
	assert.Equal(t, int32(0), *cfg.HardTTL)
}

func TestUpdateSandboxPausedRecordIgnoresStaleRuntimePod(t *testing.T) {
	pod := testSandboxPod()
	pod.Annotations[controller.AnnotationExpiresAt] = "2026-03-07T12:05:00Z"
	pod.Annotations[controller.AnnotationConfig] = `{"ttl":300}`

	svc, client := newSandboxServiceForTTLTests(t, pod, 0)
	svc.sandboxStore = &memorySandboxStore{records: map[string]*SandboxRecord{
		"sandbox-1": {
			ID:                  "sandbox-1",
			TeamID:              "team-1",
			UserID:              "user-1",
			TemplateID:          "default",
			TemplateName:        "default",
			TemplateNamespace:   "tpl-default",
			Status:              SandboxStatusPaused,
			Config:              SandboxConfig{TTL: int32Ptr(300)},
			CurrentPodName:      pod.Name,
			CurrentPodNamespace: pod.Namespace,
			RuntimeGeneration:   3,
			ExpiresAt:           time.Date(2026, time.March, 7, 12, 5, 0, 0, time.UTC),
		},
	}}

	updated, err := svc.UpdateSandbox(context.Background(), "sandbox-1", &SandboxUpdateConfig{
		TTL: int32Ptr(0),
	})
	require.NoError(t, err)
	assert.Equal(t, SandboxStatusPaused, updated.Status)
	assert.True(t, updated.ExpiresAt.IsZero())

	record, err := svc.sandboxStore.GetSandbox(context.Background(), "sandbox-1")
	require.NoError(t, err)
	require.NotNil(t, record.Config.TTL)
	assert.Equal(t, SandboxStatusPaused, record.Status)
	assert.Equal(t, int32(0), *record.Config.TTL)
	assert.True(t, record.ExpiresAt.IsZero())

	for _, action := range client.Actions() {
		if action.GetVerb() == "update" && action.GetResource().Resource == "pods" {
			t.Fatalf("unexpected stale runtime pod update: %#v", action)
		}
	}
	stored, err := client.CoreV1().Pods(pod.Namespace).Get(context.Background(), pod.Name, metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, "2026-03-07T12:05:00Z", stored.Annotations[controller.AnnotationExpiresAt])
}

func TestPersistUpdatedSandboxPodDoesNotOverwritePausedRecord(t *testing.T) {
	pod := testSandboxPod()
	pod.Annotations[controller.AnnotationConfig] = `{"ttl":0}`

	svc, _ := newSandboxServiceForTTLTests(t, pod, 0)
	store := &memorySandboxStore{records: map[string]*SandboxRecord{
		"sandbox-1": {
			ID:                "sandbox-1",
			TeamID:            "team-1",
			UserID:            "user-1",
			TemplateID:        "default",
			TemplateName:      "default",
			TemplateNamespace: "tpl-default",
			Status:            SandboxStatusPaused,
			Config:            SandboxConfig{TTL: int32Ptr(300)},
			RuntimeGeneration: 3,
		},
	}}
	svc.sandboxStore = store

	require.NoError(t, svc.persistUpdatedSandboxPod(context.Background(), pod))
	record, err := store.GetSandbox(context.Background(), "sandbox-1")
	require.NoError(t, err)
	require.NotNil(t, record.Config.TTL)
	assert.Equal(t, SandboxStatusPaused, record.Status)
	assert.Equal(t, int32(300), *record.Config.TTL)
}

func TestPersistUpdatedSandboxPodStoresRuntimeMetadata(t *testing.T) {
	pod := testSandboxPod()
	pod.Labels[controller.LabelTemplateID] = "default"
	pod.Labels[controller.LabelTemplateLogicalID] = "default"
	pod.Labels[controller.LabelOwnerKind] = "managed-agent"
	pod.Annotations[controller.AnnotationOwnerKind] = "managed-agent"
	pod.Annotations[controller.AnnotationConfig] = `{"ttl":300}`
	pod.Annotations[controller.AnnotationWebhookStateVolumeID] = "webhook-volume-1"

	svc, _ := newSandboxServiceForTTLTests(t, pod, 0)
	store := &memorySandboxStore{records: map[string]*SandboxRecord{}}
	templateNamespace, err := naming.TemplateNamespaceForTeam("team-1")
	require.NoError(t, err)
	svc.sandboxStore = store
	svc.templateLister = staticTemplateLister{templates: []*v1alpha1.SandboxTemplate{{
		ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: templateNamespace},
	}}}

	require.NoError(t, svc.persistUpdatedSandboxPod(context.Background(), pod))
	record, err := store.GetSandbox(context.Background(), "sandbox-1")
	require.NoError(t, err)
	require.NotNil(t, record)
	assert.Equal(t, "webhook-volume-1", record.WebhookStateVolumeID)
	assert.Equal(t, "managed-agent", record.OwnerKind)
}

func TestRefreshSandboxPersistsExpirationRecord(t *testing.T) {
	pod := testSandboxPod()
	pod.Labels[controller.LabelTemplateID] = "default"
	pod.Labels[controller.LabelTemplateLogicalID] = "default"
	pod.Annotations[controller.AnnotationConfig] = `{"ttl":60,"hard_ttl":120}`
	pod.Annotations[controller.AnnotationExpiresAt] = "2026-03-07T12:01:00Z"
	pod.Annotations[controller.AnnotationHardExpiresAt] = "2026-03-07T12:02:00Z"

	svc, _ := newSandboxServiceForTTLTests(t, pod, 0)
	store := &memorySandboxStore{records: map[string]*SandboxRecord{}}
	templateNamespace, err := naming.TemplateNamespaceForTeam("team-1")
	require.NoError(t, err)
	svc.sandboxStore = store
	svc.templateLister = staticTemplateLister{templates: []*v1alpha1.SandboxTemplate{{
		ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: templateNamespace},
	}}}

	resp, err := svc.RefreshSandbox(context.Background(), "sandbox-1", &RefreshRequest{Duration: 90})
	require.NoError(t, err)

	record, err := store.GetSandbox(context.Background(), "sandbox-1")
	require.NoError(t, err)
	require.NotNil(t, record)
	assert.Equal(t, resp.ExpiresAt, record.ExpiresAt)
	assert.Equal(t, resp.HardExpiresAt, record.HardExpiresAt)
	assert.Equal(t, time.Date(2026, time.March, 7, 12, 1, 30, 0, time.UTC), record.ExpiresAt)
	assert.Equal(t, time.Date(2026, time.March, 7, 12, 2, 0, 0, time.UTC), record.HardExpiresAt)
}

func TestUpdateSandboxEnvVarsUpdatesProcdAndConfig(t *testing.T) {
	pod := testSandboxPod()
	pod.Annotations[controller.AnnotationConfig] = `{"env_vars":{"OLD":"old"},"ttl":300}`

	var procdReq UpdateSandboxEnvVarsRequest
	procdServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut || r.URL.Path != "/api/v1/sandbox/env_vars" {
			t.Fatalf("unexpected procd request %s %s", r.Method, r.URL.Path)
		}
		if got := r.Header.Get("X-Internal-Token"); got != "token" {
			t.Fatalf("X-Internal-Token = %q, want token", got)
		}
		if err := json.NewDecoder(r.Body).Decode(&procdReq); err != nil {
			t.Fatalf("decode procd request: %v", err)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"success": true,
			"data": map[string]any{
				"env_vars": procdReq.EnvVars,
			},
		})
	}))
	t.Cleanup(procdServer.Close)
	procdPort := configurePodForProcdServer(t, pod, procdServer.URL)

	svc, client := newSandboxServiceForTTLTests(t, pod, 0)
	svc.config.ProcdPort = procdPort
	svc.procdClient = NewProcdClientWithHTTPClient(procdServer.Client())
	svc.internalTokenGenerator = staticTokenGenerator{}

	_, err := svc.UpdateSandbox(context.Background(), pod.Name, &SandboxUpdateConfig{
		EnvVars: map[string]string{
			"APP_ENV": "test",
		},
	})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"APP_ENV": "test"}, procdReq.EnvVars)

	stored, err := client.CoreV1().Pods(pod.Namespace).Get(context.Background(), pod.Name, metav1.GetOptions{})
	require.NoError(t, err)
	var cfg SandboxConfig
	require.NoError(t, json.Unmarshal([]byte(stored.Annotations[controller.AnnotationConfig]), &cfg))
	assert.Equal(t, map[string]string{"APP_ENV": "test"}, cfg.EnvVars)
	require.NotNil(t, cfg.TTL)
	assert.Equal(t, int32(300), *cfg.TTL)
}

func TestUpdateSandboxEnvVarsClearsConfig(t *testing.T) {
	pod := testSandboxPod()
	pod.Annotations[controller.AnnotationConfig] = `{"env_vars":{"OLD":"old"}}`

	var procdReq UpdateSandboxEnvVarsRequest
	procdServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&procdReq); err != nil {
			t.Fatalf("decode procd request: %v", err)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"success": true,
			"data":    map[string]any{"env_vars": map[string]string{}},
		})
	}))
	t.Cleanup(procdServer.Close)
	procdPort := configurePodForProcdServer(t, pod, procdServer.URL)

	svc, client := newSandboxServiceForTTLTests(t, pod, 0)
	svc.config.ProcdPort = procdPort
	svc.procdClient = NewProcdClientWithHTTPClient(procdServer.Client())
	svc.internalTokenGenerator = staticTokenGenerator{}

	_, err := svc.UpdateSandbox(context.Background(), pod.Name, &SandboxUpdateConfig{
		EnvVars: map[string]string{},
	})
	require.NoError(t, err)
	assert.Empty(t, procdReq.EnvVars)

	stored, err := client.CoreV1().Pods(pod.Namespace).Get(context.Background(), pod.Name, metav1.GetOptions{})
	require.NoError(t, err)
	var cfg SandboxConfig
	require.NoError(t, json.Unmarshal([]byte(stored.Annotations[controller.AnnotationConfig]), &cfg))
	assert.Empty(t, cfg.EnvVars)
}

func TestUpdateSandboxRejectsInvalidTTLState(t *testing.T) {
	tests := []struct {
		name string
		cfg  *SandboxUpdateConfig
	}{
		{name: "negative ttl", cfg: &SandboxUpdateConfig{TTL: int32Ptr(-1)}},
		{name: "negative hard ttl", cfg: &SandboxUpdateConfig{HardTTL: int32Ptr(-1)}},
		{name: "ttl beyond existing hard ttl", cfg: &SandboxUpdateConfig{TTL: int32Ptr(700)}},
		{name: "ttl beyond updated hard ttl", cfg: &SandboxUpdateConfig{TTL: int32Ptr(1000), HardTTL: int32Ptr(60)}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pod := testSandboxPod()
			pod.Annotations[controller.AnnotationExpiresAt] = "2026-03-07T12:05:00Z"
			pod.Annotations[controller.AnnotationHardExpiresAt] = "2026-03-07T12:10:00Z"
			pod.Annotations[controller.AnnotationConfig] = `{"ttl":300,"hard_ttl":600}`

			svc, client := newSandboxServiceForTTLTests(t, pod, 0)

			_, err := svc.UpdateSandbox(context.Background(), pod.Name, tt.cfg)
			if !errors.Is(err, ErrInvalidClaimRequest) {
				t.Fatalf("UpdateSandbox() error = %v, want ErrInvalidClaimRequest", err)
			}

			stored, getErr := client.CoreV1().Pods(pod.Namespace).Get(context.Background(), pod.Name, metav1.GetOptions{})
			require.NoError(t, getErr)
			assert.Equal(t, "2026-03-07T12:05:00Z", stored.Annotations[controller.AnnotationExpiresAt])
			assert.Equal(t, "2026-03-07T12:10:00Z", stored.Annotations[controller.AnnotationHardExpiresAt])
		})
	}
}

func TestRefreshSandboxDisabledTTLRemainsDisabled(t *testing.T) {
	pod := testSandboxPod()
	pod.Annotations[controller.AnnotationConfig] = `{"ttl":0,"hard_ttl":0}`

	svc, client := newSandboxServiceForTTLTests(t, pod, 0)

	resp, err := svc.RefreshSandbox(context.Background(), pod.Name, &RefreshRequest{})
	require.NoError(t, err)
	assert.True(t, resp.ExpiresAt.IsZero())
	assert.True(t, resp.HardExpiresAt.IsZero())

	stored, err := client.CoreV1().Pods(pod.Namespace).Get(context.Background(), pod.Name, metav1.GetOptions{})
	require.NoError(t, err)
	assert.Empty(t, stored.Annotations[controller.AnnotationExpiresAt])
	assert.Empty(t, stored.Annotations[controller.AnnotationHardExpiresAt])
}

func TestRefreshSandboxRejectsInvalidTTLState(t *testing.T) {
	tests := []struct {
		name string
		req  *RefreshRequest
	}{
		{name: "negative duration", req: &RefreshRequest{Duration: -1}},
		{name: "duration beyond hard ttl", req: &RefreshRequest{Duration: 700}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pod := testSandboxPod()
			pod.Annotations[controller.AnnotationExpiresAt] = "2026-03-07T12:05:00Z"
			pod.Annotations[controller.AnnotationHardExpiresAt] = "2026-03-07T12:10:00Z"
			pod.Annotations[controller.AnnotationConfig] = `{"ttl":300,"hard_ttl":600}`

			svc, client := newSandboxServiceForTTLTests(t, pod, 0)

			_, err := svc.RefreshSandbox(context.Background(), pod.Name, tt.req)
			if !errors.Is(err, ErrInvalidClaimRequest) {
				t.Fatalf("RefreshSandbox() error = %v, want ErrInvalidClaimRequest", err)
			}

			stored, getErr := client.CoreV1().Pods(pod.Namespace).Get(context.Background(), pod.Name, metav1.GetOptions{})
			require.NoError(t, getErr)
			assert.Equal(t, "2026-03-07T12:05:00Z", stored.Annotations[controller.AnnotationExpiresAt])
			assert.Equal(t, "2026-03-07T12:10:00Z", stored.Annotations[controller.AnnotationHardExpiresAt])
		})
	}
}

func TestRefreshSandboxRetriesPodUpdateConflict(t *testing.T) {
	pod := testSandboxPod()
	pod.Annotations[controller.AnnotationConfig] = `{"ttl":300,"hard_ttl":600}`

	svc, client := newSandboxServiceForTTLTests(t, pod, 0)
	updates := 0
	client.PrependReactor("update", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
		if action.GetSubresource() != "" {
			return false, nil, nil
		}
		updates++
		if updates == 1 {
			return true, nil, apierrors.NewConflict(schema.GroupResource{Resource: "pods"}, pod.Name, errors.New("stale pod"))
		}
		return false, nil, nil
	})

	resp, err := svc.RefreshSandbox(context.Background(), pod.Name, &RefreshRequest{Duration: 120})
	require.NoError(t, err)
	if updates != 2 {
		t.Fatalf("pod update calls = %d, want 2", updates)
	}
	assert.Equal(t, time.Date(2026, time.March, 7, 12, 2, 0, 0, time.UTC), resp.ExpiresAt)
	assert.Equal(t, time.Date(2026, time.March, 7, 12, 10, 0, 0, time.UTC), resp.HardExpiresAt)

	stored, err := client.CoreV1().Pods(pod.Namespace).Get(context.Background(), pod.Name, metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, "2026-03-07T12:02:00Z", stored.Annotations[controller.AnnotationExpiresAt])
	assert.Equal(t, "2026-03-07T12:10:00Z", stored.Annotations[controller.AnnotationHardExpiresAt])
}

func configurePodForProcdServer(t *testing.T, pod *corev1.Pod, rawURL string) int {
	t.Helper()
	parsed, err := url.Parse(rawURL)
	require.NoError(t, err)
	host, portText, err := net.SplitHostPort(parsed.Host)
	require.NoError(t, err)
	port, err := strconv.Atoi(portText)
	require.NoError(t, err)
	if pod.Annotations == nil {
		pod.Annotations = map[string]string{}
	}
	if pod.Labels == nil {
		pod.Labels = map[string]string{}
	}
	pod.Annotations[controller.AnnotationSandboxID] = pod.Name
	pod.Status.PodIP = host
	return port
}
