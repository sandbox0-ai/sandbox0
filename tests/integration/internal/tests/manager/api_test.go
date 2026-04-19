package manager

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
	clientset "github.com/sandbox0-ai/sandbox0/manager/pkg/generated/clientset/versioned"
	clientsetfake "github.com/sandbox0-ai/sandbox0/manager/pkg/generated/clientset/versioned/fake"
	managerhttp "github.com/sandbox0-ai/sandbox0/manager/pkg/http"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/namespacepolicy"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/service"
	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	"github.com/sandbox0-ai/sandbox0/pkg/observability"
	obsmetrics "github.com/sandbox0-ai/sandbox0/pkg/observability/metrics"
	"github.com/sandbox0-ai/sandbox0/tests/integration/internal/utils"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
)

type managerTestEnv struct {
	server     *httptest.Server
	token      string
	podIndexer cache.Indexer
	k8sClient  kubernetes.Interface
}

type managerTestEnvOptions struct {
	sandboxConfig          service.SandboxServiceConfig
	internalTokenGenerator service.TokenGenerator
	procdClient            *service.ProcdClient
	volumeMetadata         service.SandboxVolumeMetadataClient
}

func newManagerTestEnv(t *testing.T) *managerTestEnv {
	return newManagerTestEnvWithOptions(t, managerTestEnvOptions{})
}

func newManagerTestEnvWithProcd(t *testing.T) *managerTestEnv {
	t.Helper()

	procdServer := newProcdStubServer(t)
	t.Cleanup(procdServer.Close)

	procdClient := newProcdClientForURL(t, procdServer.URL)

	privateKey, _, err := createInternalKeys()
	utils.RequireNoError(t, err, "create procd keys")

	procdGen := internalauth.NewGenerator(internalauth.DefaultGeneratorConfig("manager", privateKey))

	return newManagerTestEnvWithOptions(t, managerTestEnvOptions{
		sandboxConfig: service.SandboxServiceConfig{
			DefaultTTL:             time.Hour,
			PauseMinMemoryRequest:  "10Mi",
			PauseMinMemoryLimit:    "32Mi",
			PauseMemoryBufferRatio: 1.1,
			PauseMinCPU:            "10m",
			ProcdPort:              49983,
			ProcdClientTimeout:     5 * time.Second,
			ProcdInitTimeout:       5 * time.Second,
		},
		internalTokenGenerator: service.NewInternalTokenGenerator(procdGen),
		procdClient:            procdClient,
	})
}

func newManagerTestEnvWithOptions(t *testing.T, opts managerTestEnvOptions) *managerTestEnv {
	t.Helper()

	k8sClient := k8sfake.NewClientset()
	crdClient := clientsetfake.NewSimpleClientset()

	configPath := filepath.Join(t.TempDir(), "config.yaml")
	err := os.WriteFile(configPath, []byte(""), 0o600)
	utils.RequireNoError(t, err, "write manager config")
	t.Setenv("CONFIG_PATH", configPath)
	_, publicPEM, err := internalauth.GenerateEd25519KeyPair()
	utils.RequireNoError(t, err, "generate procd public key")
	publicKeyPath := filepath.Join(t.TempDir(), "internal_jwt_public.key")
	err = os.WriteFile(publicKeyPath, publicPEM, 0o600)
	utils.RequireNoError(t, err, "write procd public key")
	previousPublicKeyPath := internalauth.DefaultInternalJWTPublicKeyPath
	internalauth.DefaultInternalJWTPublicKeyPath = publicKeyPath
	t.Cleanup(func() {
		internalauth.DefaultInternalJWTPublicKeyPath = previousPublicKeyPath
	})

	podIndexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	nodeIndexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	secretIndexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	namespaceIndexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	podLister := corelisters.NewPodLister(podIndexer)
	nodeLister := corelisters.NewNodeLister(nodeIndexer)
	secretLister := corelisters.NewSecretLister(secretIndexer)
	namespaceLister := corelisters.NewNamespaceLister(namespaceIndexer)
	sandboxIndex := service.NewSandboxIndex()

	templateLister := &testTemplateLister{
		client: crdClient,
	}
	logger := zap.NewNop()

	obsProvider, err := observability.New(observability.Config{
		ServiceName:    "manager-test",
		Logger:         logger,
		DisableTracing: true,
		DisableMetrics: true,
		DisableLogging: true,
	})
	utils.RequireNoError(t, err, "create observability provider")
	t.Cleanup(func() {
		_ = obsProvider.Shutdown(context.Background())
	})

	managerMetrics := obsmetrics.NewManager(obsProvider.MetricsRegistryOrNil())

	managerCfg := config.LoadManagerConfig()
	sandboxService := service.NewSandboxService(
		k8sClient,
		podLister,
		nil,
		sandboxIndex,
		secretLister,
		templateLister,
		nil,
		nil,
		opts.internalTokenGenerator,
		nil,
		opts.sandboxConfig,
		logger,
		managerMetrics,
	)
	if opts.procdClient != nil {
		sandboxService.SetProcdClient(opts.procdClient)
	}
	if opts.volumeMetadata != nil {
		sandboxService.SetVolumeMetadataClient(opts.volumeMetadata)
	}

	templateService := service.NewTemplateService(
		k8sClient,
		crdClient,
		templateLister,
		namespaceLister,
		nil,
		managerCfg.Registry,
		logger,
	)
	baselineReconciler, err := namespacepolicy.NewReconciler(k8sClient, namespacepolicy.Config{
		SystemNamespace: "sandbox0-system",
		ProcdPort:       49983,
	}, logger)
	utils.RequireNoError(t, err, "create template namespace baseline reconciler")
	templateService.SetNamespacePolicyReconciler(baselineReconciler)
	registryService := service.NewRegistryService(nil, logger)
	clusterService := service.NewClusterService(
		k8sClient,
		podLister,
		nodeLister,
		templateLister,
		logger,
	)

	privateKey, publicKey, err := createInternalKeys()
	utils.RequireNoError(t, err, "create internal keys")

	gen := internalauth.NewGenerator(internalauth.DefaultGeneratorConfig("cluster-gateway", privateKey))
	token, err := gen.Generate("manager", "team-1", "user-1", internalauth.GenerateOptions{})
	utils.RequireNoError(t, err, "generate internal token")

	cfg := internalauth.DefaultValidatorConfig("manager", publicKey)
	cfg.AllowedCallers = []string{"cluster-gateway"}
	validator := internalauth.NewValidator(cfg)

	server := managerhttp.NewServer(
		sandboxService,
		nil,
		nil,
		templateService,
		registryService,
		nil,
		nil,
		false,
		clusterService,
		validator,
		logger,
		0,
		obsProvider,
		"sandbox0.app",
		"test-region",
	)

	httpServer := httptest.NewServer(server.Handler())
	t.Cleanup(httpServer.Close)

	return &managerTestEnv{
		server:     httpServer,
		token:      token,
		podIndexer: podIndexer,
		k8sClient:  k8sClient,
	}
}

func createInternalKeys() (internalauth.PrivateKeyType, internalauth.PublicKeyType, error) {
	privatePEM, publicPEM, err := internalauth.GenerateEd25519KeyPair()
	if err != nil {
		return nil, nil, err
	}
	privateKey, err := internalauth.LoadEd25519PrivateKey(privatePEM)
	if err != nil {
		return nil, nil, err
	}
	publicKey, err := internalauth.LoadEd25519PublicKey(publicPEM)
	if err != nil {
		return nil, nil, err
	}
	return privateKey, publicKey, nil
}

func TestCreateTemplateLegacyEnsuresNamespaceIngressBaseline(t *testing.T) {
	env := newManagerTestEnv(t)

	resp, body := doRequest(t, env.server.Client(), http.MethodPost, env.server.URL+"/internal/v1/templates", env.token, map[string]any{
		"metadata": map[string]any{"name": "demo"},
		"spec":     map[string]any{},
	})
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d, body = %s", resp.StatusCode, string(body))
	}

	namespace, err := naming.TemplateNamespaceForBuiltin("demo")
	utils.RequireNoError(t, err, "resolve template namespace")

	policies, err := env.k8sClient.NetworkingV1().NetworkPolicies(namespace).List(context.Background(), metav1.ListOptions{})
	utils.RequireNoError(t, err, "list namespace baseline policies")
	if len(policies.Items) != 2 {
		t.Fatalf("networkpolicy count = %d, want 2", len(policies.Items))
	}
}

func TestClaimSandboxBindsDeclaredVolumePortal(t *testing.T) {
	recorder := &initializeRequestRecorder{}
	procdServer := newInitializeRecordingProcdServer(t, recorder, service.InitializeResponse{
		SandboxID: "initialized",
	})
	t.Cleanup(procdServer.Close)
	ctldRecorder := &volumePortalBindRecorder{}
	ctldServer := newVolumePortalBindRecordingCtldServer(t, ctldRecorder, ctldapi.BindVolumePortalResponse{
		SandboxVolumeID: "vol-1",
		MountPoint:      "/workspace/data",
		MountedAt:       time.Now().UTC().Format(time.RFC3339),
	})
	t.Cleanup(ctldServer.Close)

	procdClient := newProcdClientForURL(t, procdServer.URL)
	ctldHTTPClient := newRewriteHTTPClientForURL(t, ctldServer.URL)
	privateKey, _, err := createInternalKeys()
	utils.RequireNoError(t, err, "create procd keys")
	procdGen := internalauth.NewGenerator(internalauth.DefaultGeneratorConfig("manager", privateKey))

	env := newManagerTestEnvWithOptions(t, managerTestEnvOptions{
		sandboxConfig: service.SandboxServiceConfig{
			DefaultTTL:             time.Hour,
			PauseMinMemoryRequest:  "10Mi",
			PauseMinMemoryLimit:    "32Mi",
			PauseMemoryBufferRatio: 1.1,
			PauseMinCPU:            "10m",
			ProcdPort:              49983,
			ProcdClientTimeout:     5 * time.Second,
			ProcdInitTimeout:       5 * time.Second,
			CtldPort:               8095,
			CtldHTTPClient:         ctldHTTPClient,
		},
		internalTokenGenerator: service.NewInternalTokenGenerator(procdGen),
		procdClient:            procdClient,
		volumeMetadata:         staticVolumeMetadataClient{accessMode: "RWO"},
	})

	templateName := "claim-bootstrap"
	resp, body := doRequest(t, env.server.Client(), http.MethodPost, env.server.URL+"/internal/v1/templates", env.token, map[string]any{
		"metadata": map[string]any{"name": templateName},
		"spec": map[string]any{
			"volumeMounts": []map[string]any{{
				"name":      "data",
				"mountPath": "/workspace/data",
			}},
		},
	})
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create template status = %d, body = %s", resp.StatusCode, string(body))
	}

	namespace, err := naming.TemplateNamespaceForBuiltin(templateName)
	utils.RequireNoError(t, err, "resolve template namespace")
	addNode(t, env, "node-a", "10.0.0.1")
	addIdleReadyPodForTemplate(t, env, &v1alpha1.SandboxTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: templateName, Namespace: namespace},
		Spec: v1alpha1.SandboxTemplateSpec{
			VolumeMounts: []v1alpha1.VolumeMountSpec{{Name: "data", MountPath: "/workspace/data"}},
		},
	}, "idle-bootstrap", "10.0.0.10", "node-a")

	resp, body = doRequest(t, env.server.Client(), http.MethodPost, env.server.URL+"/api/v1/sandboxes", env.token, map[string]any{
		"template": templateName,
		"mounts": []map[string]any{{
			"sandboxvolume_id": "vol-1",
			"mount_point":      "/workspace/data",
		}},
	})
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("claim status = %d, body = %s", resp.StatusCode, string(body))
	}

	claimResp, errInfo, err := spec.DecodeResponse[service.ClaimResponse](bytes.NewReader(body))
	if err != nil {
		t.Fatalf("decode claim response: %v", err)
	}
	if errInfo != nil {
		t.Fatalf("unexpected claim error: %+v", errInfo)
	}
	if claimResp == nil || len(claimResp.BootstrapMounts) != 1 {
		t.Fatalf("bootstrap mounts = %+v, want 1 entry", claimResp)
	}
	if claimResp.BootstrapMounts[0].State != "mounted" {
		t.Fatalf("claim bootstrap state = %q, want mounted", claimResp.BootstrapMounts[0].State)
	}
	bindReq := ctldRecorder.Get()
	if bindReq.SandboxVolumeID != "vol-1" || bindReq.MountPath != "/workspace/data" || bindReq.PortalName != "data" {
		t.Fatalf("unexpected ctld bind request: %+v", bindReq)
	}

	initReq := recorder.Get()
	if initReq.SandboxID != claimResp.SandboxID || initReq.TeamID != "team-1" {
		t.Fatalf("unexpected initialize request: %+v", initReq)
	}
}

type testTemplateLister struct {
	client clientset.Interface
}

func (t *testTemplateLister) List() ([]*v1alpha1.SandboxTemplate, error) {
	list, err := t.client.Sandbox0V1alpha1().SandboxTemplates(metav1.NamespaceAll).List(context.Background(), metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	templates := make([]*v1alpha1.SandboxTemplate, 0, len(list.Items))
	for i := range list.Items {
		templates = append(templates, &list.Items[i])
	}
	return templates, nil
}

func (t *testTemplateLister) Get(namespace, name string) (*v1alpha1.SandboxTemplate, error) {
	template, err := t.client.Sandbox0V1alpha1().SandboxTemplates(namespace).Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		if errors.IsNotFound(err) {
			return nil, errors.NewNotFound(v1alpha1.Resource("sandboxtemplate"), name)
		}
		return nil, err
	}
	return template, nil
}

func doRequest(t *testing.T, client *http.Client, method, url, token string, body any) (*http.Response, []byte) {
	t.Helper()

	var payload io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		utils.RequireNoError(t, err, "marshal request body")
		payload = bytes.NewReader(encoded)
	}

	req, err := http.NewRequest(method, url, payload)
	utils.RequireNoError(t, err, "create request")
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("X-Internal-Token", token)
	}

	resp, err := client.Do(req)
	utils.RequireNoError(t, err, "send request")

	defer resp.Body.Close()
	respBody, err := io.ReadAll(resp.Body)
	utils.RequireNoError(t, err, "read response")

	return resp, respBody
}

func addSandboxPod(t *testing.T, env *managerTestEnv, name, teamID, userID string, phase corev1.PodPhase) {
	t.Helper()
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "default",
			Labels: map[string]string{
				controller.LabelSandboxID: name,
			},
			Annotations: map[string]string{
				controller.AnnotationTeamID: teamID,
				controller.AnnotationUserID: userID,
			},
		},
		Status: corev1.PodStatus{Phase: phase},
	}
	_, err := env.k8sClient.CoreV1().Pods(pod.Namespace).Create(context.Background(), pod, metav1.CreateOptions{})
	utils.RequireNoError(t, err, "create pod in fake client")
	utils.RequireNoError(t, env.podIndexer.Add(pod), "add pod to indexer")
}

func addIdleReadyPod(t *testing.T, env *managerTestEnv, namespace, name, templateID, podIP string) {
	t.Helper()
	addIdleReadyPodForTemplate(t, env, &v1alpha1.SandboxTemplate{
		ObjectMeta: metav1.ObjectMeta{
			Name:      templateID,
			Namespace: namespace,
		},
	}, name, podIP, "")
}

func addIdleReadyPodForTemplate(t *testing.T, env *managerTestEnv, template *v1alpha1.SandboxTemplate, name, podIP, nodeName string) {
	t.Helper()
	templateHash, err := controller.TemplateSpecHash(template)
	utils.RequireNoError(t, err, "compute template spec hash")
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: template.Namespace,
			UID:       types.UID("pod-" + name),
			Labels: map[string]string{
				controller.LabelTemplateID: template.Name,
				controller.LabelPoolType:   controller.PoolTypeIdle,
			},
			Annotations: map[string]string{
				controller.AnnotationTemplateSpecHash: templateHash,
			},
			ResourceVersion: "1",
		},
		Spec: corev1.PodSpec{NodeName: nodeName},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			PodIP: podIP,
			Conditions: []corev1.PodCondition{{
				Type:   corev1.PodReady,
				Status: corev1.ConditionTrue,
			}},
		},
	}
	_, err = env.k8sClient.CoreV1().Pods(template.Namespace).Create(context.Background(), pod, metav1.CreateOptions{})
	utils.RequireNoError(t, err, "create ready idle pod in fake client")
	utils.RequireNoError(t, env.podIndexer.Add(pod), "add ready idle pod to indexer")
}

func addNode(t *testing.T, env *managerTestEnv, name, internalIP string) {
	t.Helper()
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Status: corev1.NodeStatus{
			Addresses: []corev1.NodeAddress{{
				Type:    corev1.NodeInternalIP,
				Address: internalIP,
			}},
		},
	}
	_, err := env.k8sClient.CoreV1().Nodes().Create(context.Background(), node, metav1.CreateOptions{})
	utils.RequireNoError(t, err, "create node in fake client")
}

type initializeRequestRecorder struct {
	request service.InitializeRequest
}

func (r *initializeRequestRecorder) Set(req service.InitializeRequest) {
	r.request = req
}

type volumePortalBindRecorder struct {
	request ctldapi.BindVolumePortalRequest
}

type staticVolumeMetadataClient struct {
	accessMode string
}

func (c staticVolumeMetadataClient) Get(_ context.Context, teamID, userID, volumeID string) (*service.SandboxVolumeInfo, error) {
	return &service.SandboxVolumeInfo{
		ID:         volumeID,
		TeamID:     teamID,
		UserID:     userID,
		AccessMode: c.accessMode,
	}, nil
}

func (r *volumePortalBindRecorder) Set(req ctldapi.BindVolumePortalRequest) {
	r.request = req
}

func (r *volumePortalBindRecorder) Get() ctldapi.BindVolumePortalRequest {
	return r.request
}

func newVolumePortalBindRecordingCtldServer(t *testing.T, recorder *volumePortalBindRecorder, response ctldapi.BindVolumePortalResponse) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/volume-portals/bind", func(w http.ResponseWriter, r *http.Request) {
		var req ctldapi.BindVolumePortalRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode ctld bind request: %v", err)
		}
		if recorder != nil {
			recorder.Set(req)
		}
		_ = json.NewEncoder(w).Encode(response)
	})
	return httptest.NewServer(mux)
}

func (r *initializeRequestRecorder) Get() service.InitializeRequest {
	return r.request
}

func newInitializeRecordingProcdServer(t *testing.T, recorder *initializeRequestRecorder, response service.InitializeResponse) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/initialize", func(w http.ResponseWriter, r *http.Request) {
		var req service.InitializeRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode initialize request: %v", err)
		}
		if recorder != nil {
			recorder.Set(req)
		}
		_ = spec.WriteSuccess(w, http.StatusOK, response)
	})
	return httptest.NewServer(mux)
}

type rewriteTransport struct {
	base      *url.URL
	transport http.RoundTripper
}

func (r rewriteTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	clone := req.Clone(req.Context())
	clone.URL.Scheme = r.base.Scheme
	clone.URL.Host = r.base.Host
	return r.transport.RoundTrip(clone)
}

func newProcdClientForURL(t *testing.T, baseURL string) *service.ProcdClient {
	t.Helper()
	return service.NewProcdClientWithHTTPClient(newRewriteHTTPClientForURL(t, baseURL))
}

func newRewriteHTTPClientForURL(t *testing.T, baseURL string) *http.Client {
	t.Helper()
	parsed, err := url.Parse(baseURL)
	utils.RequireNoError(t, err, "parse url")

	return &http.Client{
		Timeout: 5 * time.Second,
		Transport: rewriteTransport{
			base:      parsed,
			transport: http.DefaultTransport,
		},
	}
}

func newProcdStubServer(t *testing.T) *httptest.Server {
	t.Helper()

	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/sandbox/pause", func(w http.ResponseWriter, r *http.Request) {
		response := service.PauseResponse{
			Paused: true,
			ResourceUsage: &service.SandboxResourceUsage{
				ContainerMemoryWorkingSet: 64 * 1024 * 1024,
			},
		}
		_ = json.NewEncoder(w).Encode(response)
	})
	mux.HandleFunc("/api/v1/sandbox/resume", func(w http.ResponseWriter, r *http.Request) {
		response := service.ResumeResponse{Resumed: true}
		_ = json.NewEncoder(w).Encode(response)
	})
	mux.HandleFunc("/api/v1/sandbox/stats", func(w http.ResponseWriter, r *http.Request) {
		response := service.StatsResponse{
			SandboxResourceUsage: service.SandboxResourceUsage{
				ContainerMemoryWorkingSet: 64 * 1024 * 1024,
				ContextCount:              1,
			},
		}
		_ = json.NewEncoder(w).Encode(response)
	})
	mux.HandleFunc("/api/v1/initialize", func(w http.ResponseWriter, r *http.Request) {
		response := service.InitializeResponse{SandboxID: "initialized"}
		_ = json.NewEncoder(w).Encode(response)
	})

	return httptest.NewServer(mux)
}
