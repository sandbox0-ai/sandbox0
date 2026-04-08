package controller

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"
	k8sscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	grpc_health_v1 "google.golang.org/grpc/health/grpc_health_v1"
)

const (
	defaultProbeTimeoutSeconds = 1
	defaultProbePeriodSeconds  = 10
	defaultSuccessThreshold    = 1
	defaultFailureThreshold    = 3
)

type SandboxReadinessFailure struct {
	Name    string
	Reason  string
	Message string
}

type SandboxReadinessEvaluator interface {
	FirstFailure(ctx context.Context, pod *corev1.Pod) (*SandboxReadinessFailure, error)
}

type readinessProbeRunner interface {
	Run(ctx context.Context, pod *corev1.Pod, containerName string, probe *corev1.Probe) error
}

type managedReadinessEvaluator struct {
	runner readinessProbeRunner
	now    func() time.Time

	mu     sync.Mutex
	states map[string]managedProbeState
}

type managedProbeState struct {
	Initialized          bool
	Ready                bool
	LastChecked          time.Time
	ConsecutiveSuccesses int
	ConsecutiveFailures  int
	LastMessage          string
}

type kubernetesReadinessProbeRunner struct {
	k8sClient  kubernetes.Interface
	restConfig *rest.Config
	httpClient *http.Client
}

func NewManagedReadinessEvaluator(k8sClient kubernetes.Interface, restConfig *rest.Config, logger *zap.Logger) SandboxReadinessEvaluator {
	if k8sClient == nil || restConfig == nil {
		return nil
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = &tls.Config{InsecureSkipVerify: true} // Kubelet readiness probes do not verify pod certs.
	return &managedReadinessEvaluator{
		runner: &kubernetesReadinessProbeRunner{
			k8sClient:  k8sClient,
			restConfig: restConfig,
			httpClient: &http.Client{Transport: transport},
		},
		now:    time.Now,
		states: make(map[string]managedProbeState),
	}
}

func (e *managedReadinessEvaluator) FirstFailure(ctx context.Context, pod *corev1.Pod) (*SandboxReadinessFailure, error) {
	if e == nil || pod == nil {
		return nil, nil
	}

	probes, err := managedReadinessProbesFromPod(pod)
	if err != nil {
		return &SandboxReadinessFailure{
			Reason:  "ManagedReadinessInvalid",
			Message: fmt.Sprintf("sandbox0-managed readiness config is invalid: %v", err),
		}, nil
	}
	for _, configured := range probes {
		ready, message := e.evaluateProbe(ctx, pod, configured)
		if ready {
			continue
		}
		return &SandboxReadinessFailure{
			Name:    configured.Name,
			Reason:  "SidecarNotReady",
			Message: message,
		}, nil
	}
	return nil, nil
}

func (e *managedReadinessEvaluator) evaluateProbe(ctx context.Context, pod *corev1.Pod, configured v1alpha1.ManagedSidecarReadinessProbe) (bool, string) {
	if configured.Probe == nil {
		return false, fmt.Sprintf("sidecar %s readiness probe is missing", configured.Name)
	}
	if _, ok := findContainer(pod, configured.Name); !ok {
		return false, fmt.Sprintf("sidecar %s is missing from pod spec", configured.Name)
	}

	now := e.now()
	if delay := probeInitialDelay(configured.Probe); delay > 0 {
		if now.Before(probeStartTime(pod).Add(delay)) {
			return false, fmt.Sprintf("sidecar %s readiness probe initial delay has not elapsed", configured.Name)
		}
	}

	key, err := managedProbeStateKey(pod, configured)
	if err != nil {
		return false, fmt.Sprintf("sidecar %s readiness probe config is invalid: %v", configured.Name, err)
	}

	e.mu.Lock()
	state := e.states[key]
	if state.Initialized && now.Sub(state.LastChecked) < probePeriod(configured.Probe) {
		e.mu.Unlock()
		if state.Ready {
			return true, ""
		}
		return false, state.LastMessage
	}
	e.mu.Unlock()

	runCtx, cancel := context.WithTimeout(ctx, probeTimeout(configured.Probe))
	defer cancel()

	err = e.runner.Run(runCtx, pod, configured.Name, configured.Probe)

	e.mu.Lock()
	defer e.mu.Unlock()
	state = e.states[key]
	state.Initialized = true
	state.LastChecked = now
	if err == nil {
		state.ConsecutiveSuccesses++
		state.ConsecutiveFailures = 0
		if state.Ready || state.ConsecutiveSuccesses >= int(probeSuccessThreshold(configured.Probe)) {
			state.Ready = true
			state.LastMessage = ""
			e.states[key] = state
			return true, ""
		}
		state.Ready = false
		state.LastMessage = fmt.Sprintf("sidecar %s readiness probe is warming up", configured.Name)
		e.states[key] = state
		return false, state.LastMessage
	}

	state.ConsecutiveFailures++
	state.ConsecutiveSuccesses = 0
	if state.Ready && state.ConsecutiveFailures < int(probeFailureThreshold(configured.Probe)) {
		state.LastMessage = fmt.Sprintf("sidecar %s readiness probe failed but is still within failure threshold", configured.Name)
		e.states[key] = state
		return true, ""
	}
	state.Ready = false
	state.LastMessage = fmt.Sprintf("sidecar %s readiness probe failed: %v", configured.Name, err)
	e.states[key] = state
	return false, state.LastMessage
}

func managedReadinessProbesFromPod(pod *corev1.Pod) ([]v1alpha1.ManagedSidecarReadinessProbe, error) {
	if pod == nil || pod.Annotations == nil {
		return nil, nil
	}
	raw := strings.TrimSpace(pod.Annotations[v1alpha1.ManagedReadinessProbesAnnotation])
	if raw == "" {
		return nil, nil
	}
	var probes []v1alpha1.ManagedSidecarReadinessProbe
	if err := json.Unmarshal([]byte(raw), &probes); err != nil {
		return nil, err
	}
	return probes, nil
}

func managedProbeStateKey(pod *corev1.Pod, configured v1alpha1.ManagedSidecarReadinessProbe) (string, error) {
	data, err := json.Marshal(configured.Probe)
	if err != nil {
		return "", err
	}
	return string(pod.UID) + "/" + configured.Name + "/" + string(data), nil
}

func probeStartTime(pod *corev1.Pod) time.Time {
	if pod != nil && pod.Status.StartTime != nil {
		return pod.Status.StartTime.Time
	}
	if pod != nil && !pod.CreationTimestamp.Time.IsZero() {
		return pod.CreationTimestamp.Time
	}
	return time.Time{}
}

func probeInitialDelay(probe *corev1.Probe) time.Duration {
	if probe == nil || probe.InitialDelaySeconds <= 0 {
		return 0
	}
	return time.Duration(probe.InitialDelaySeconds) * time.Second
}

func probeTimeout(probe *corev1.Probe) time.Duration {
	if probe == nil || probe.TimeoutSeconds <= 0 {
		return defaultProbeTimeoutSeconds * time.Second
	}
	return time.Duration(probe.TimeoutSeconds) * time.Second
}

func probePeriod(probe *corev1.Probe) time.Duration {
	if probe == nil || probe.PeriodSeconds <= 0 {
		return defaultProbePeriodSeconds * time.Second
	}
	return time.Duration(probe.PeriodSeconds) * time.Second
}

func probeSuccessThreshold(probe *corev1.Probe) int32 {
	if probe == nil || probe.SuccessThreshold <= 0 {
		return defaultSuccessThreshold
	}
	return probe.SuccessThreshold
}

func probeFailureThreshold(probe *corev1.Probe) int32 {
	if probe == nil || probe.FailureThreshold <= 0 {
		return defaultFailureThreshold
	}
	return probe.FailureThreshold
}

func findContainer(pod *corev1.Pod, name string) (*corev1.Container, bool) {
	if pod == nil {
		return nil, false
	}
	for i := range pod.Spec.Containers {
		if pod.Spec.Containers[i].Name == name {
			return &pod.Spec.Containers[i], true
		}
	}
	return nil, false
}

func (r *kubernetesReadinessProbeRunner) Run(ctx context.Context, pod *corev1.Pod, containerName string, probe *corev1.Probe) error {
	switch {
	case probe == nil:
		return fmt.Errorf("probe is missing")
	case probe.Exec != nil:
		return r.runExecProbe(ctx, pod, containerName, probe.Exec.Command)
	case probe.HTTPGet != nil:
		return r.runHTTPProbe(ctx, pod, containerName, probe.HTTPGet)
	case probe.TCPSocket != nil:
		return r.runTCPProbe(ctx, pod, containerName, probe.TCPSocket)
	case probe.GRPC != nil:
		return r.runGRPCProbe(ctx, pod, probe.GRPC)
	default:
		return fmt.Errorf("probe handler is missing")
	}
}

func (r *kubernetesReadinessProbeRunner) runExecProbe(ctx context.Context, pod *corev1.Pod, containerName string, command []string) error {
	if len(command) == 0 {
		return fmt.Errorf("exec command is empty")
	}
	req := r.k8sClient.CoreV1().RESTClient().Post().
		Namespace(pod.Namespace).
		Resource("pods").
		Name(pod.Name).
		SubResource("exec").
		VersionedParams(&corev1.PodExecOptions{
			Container: containerName,
			Command:   command,
			Stdout:    true,
			Stderr:    true,
		}, k8sscheme.ParameterCodec)

	executor, err := remotecommand.NewSPDYExecutor(r.restConfig, http.MethodPost, req.URL())
	if err != nil {
		return err
	}

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	if err := executor.StreamWithContext(ctx, remotecommand.StreamOptions{
		Stdout: &stdout,
		Stderr: &stderr,
	}); err != nil {
		if out := strings.TrimSpace(stderr.String()); out != "" {
			return fmt.Errorf("%w: %s", err, out)
		}
		return err
	}
	return nil
}

func (r *kubernetesReadinessProbeRunner) runHTTPProbe(ctx context.Context, pod *corev1.Pod, containerName string, action *corev1.HTTPGetAction) error {
	host := strings.TrimSpace(action.Host)
	if host == "" {
		host = strings.TrimSpace(pod.Status.PodIP)
	}
	if host == "" {
		return fmt.Errorf("pod IP is not assigned")
	}
	port, err := resolveProbePort(action.Port, pod, containerName)
	if err != nil {
		return err
	}
	scheme := strings.ToLower(string(action.Scheme))
	if scheme == "" {
		scheme = "http"
	}
	path := action.Path
	if path == "" {
		path = "/"
	}
	target := (&url.URL{
		Scheme: scheme,
		Host:   net.JoinHostPort(host, port),
		Path:   path,
	}).String()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return err
	}
	for _, header := range action.HTTPHeaders {
		req.Header.Add(header.Name, header.Value)
	}
	resp, err := r.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 400 {
		return fmt.Errorf("unexpected status %d", resp.StatusCode)
	}
	return nil
}

func (r *kubernetesReadinessProbeRunner) runTCPProbe(ctx context.Context, pod *corev1.Pod, containerName string, action *corev1.TCPSocketAction) error {
	host := strings.TrimSpace(action.Host)
	if host == "" {
		host = strings.TrimSpace(pod.Status.PodIP)
	}
	if host == "" {
		return fmt.Errorf("pod IP is not assigned")
	}
	port, err := resolveProbePort(action.Port, pod, containerName)
	if err != nil {
		return err
	}
	dialer := net.Dialer{}
	conn, err := dialer.DialContext(ctx, "tcp", net.JoinHostPort(host, port))
	if err != nil {
		return err
	}
	return conn.Close()
}

func (r *kubernetesReadinessProbeRunner) runGRPCProbe(ctx context.Context, pod *corev1.Pod, action *corev1.GRPCAction) error {
	host := strings.TrimSpace(pod.Status.PodIP)
	if host == "" {
		return fmt.Errorf("pod IP is not assigned")
	}
	conn, err := grpc.DialContext(ctx, net.JoinHostPort(host, fmt.Sprintf("%d", action.Port)),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		return err
	}
	defer conn.Close()
	service := ""
	if action.Service != nil {
		service = *action.Service
	}
	resp, err := grpc_health_v1.NewHealthClient(conn).Check(ctx, &grpc_health_v1.HealthCheckRequest{Service: service})
	if err != nil {
		return err
	}
	if resp.GetStatus() != grpc_health_v1.HealthCheckResponse_SERVING {
		return fmt.Errorf("unexpected grpc health status %s", resp.GetStatus().String())
	}
	return nil
}

func resolveProbePort(port intstr.IntOrString, pod *corev1.Pod, containerName string) (string, error) {
	if port.Type == intstr.Int {
		if port.IntValue() <= 0 {
			return "", fmt.Errorf("probe port must be greater than zero")
		}
		return fmt.Sprintf("%d", port.IntValue()), nil
	}
	container, ok := findContainer(pod, containerName)
	if !ok {
		return "", fmt.Errorf("sidecar %s is missing from pod spec", containerName)
	}
	name := strings.TrimSpace(port.StrVal)
	for _, declared := range container.Ports {
		if declared.Name == name {
			return fmt.Sprintf("%d", declared.ContainerPort), nil
		}
	}
	return "", fmt.Errorf("named probe port %q is not declared on sidecar %s", name, containerName)
}
