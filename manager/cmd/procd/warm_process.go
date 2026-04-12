package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	ctxpkg "github.com/sandbox0-ai/sandbox0/manager/procd/pkg/context"
	"github.com/sandbox0-ai/sandbox0/manager/procd/pkg/process"
	"github.com/sandbox0-ai/sandbox0/pkg/sandboxprobe"
	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/util/intstr"
)

const warmProcessesEnvVar = "SANDBOX0_WARM_PROCESSES"

var warmProcessExit = os.Exit

type warmProcessSpec struct {
	Name    string            `json:"name,omitempty"`
	Type    string            `json:"type"`
	Alias   string            `json:"alias,omitempty"`
	Command []string          `json:"command,omitempty"`
	CWD     string            `json:"cwd,omitempty"`
	EnvVars map[string]string `json:"envVars,omitempty"`
	Probes  *warmProbeSet     `json:"probes,omitempty"`
}

type warmProbeSet struct {
	Startup   *warmProbeSpec `json:"startup,omitempty"`
	Readiness *warmProbeSpec `json:"readiness,omitempty"`
	Liveness  *warmProbeSpec `json:"liveness,omitempty"`
}

type warmProbeSpec struct {
	Process             *processProbeSpec   `json:"process,omitempty"`
	Exec                *execProbeSpec      `json:"exec,omitempty"`
	HTTPGet             *httpGetProbeSpec   `json:"httpGet,omitempty"`
	TCPSocket           *tcpSocketProbeSpec `json:"tcpSocket,omitempty"`
	TimeoutSeconds      int32               `json:"timeoutSeconds,omitempty"`
	InitialDelaySeconds int32               `json:"initialDelaySeconds,omitempty"`
}

type processProbeSpec struct{}

type execProbeSpec struct {
	Command []string `json:"command"`
}

type httpGetProbeSpec struct {
	Host        string             `json:"host,omitempty"`
	Path        string             `json:"path,omitempty"`
	Port        intstr.IntOrString `json:"port"`
	Scheme      string             `json:"scheme,omitempty"`
	HTTPHeaders []httpHeader       `json:"httpHeaders,omitempty"`
}

type httpHeader struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

type tcpSocketProbeSpec struct {
	Host string             `json:"host,omitempty"`
	Port intstr.IntOrString `json:"port"`
}

type warmProcessRuntime struct {
	Spec      warmProcessSpec
	ContextID string
	StartedAt time.Time
}

type warmProcessProber struct {
	manager              *ctxpkg.Manager
	processes            []warmProcessRuntime
	logger               *zap.Logger
	exitOnFailedLiveness bool
	exitOnce             sync.Once
}

func parseWarmProcesses(raw string) ([]warmProcessSpec, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}

	var specs []warmProcessSpec
	if err := json.Unmarshal([]byte(raw), &specs); err != nil {
		return nil, err
	}
	for i, spec := range specs {
		if err := validateWarmProcessSpec(i, spec); err != nil {
			return nil, err
		}
	}
	return specs, nil
}

func validateWarmProcessSpec(index int, spec warmProcessSpec) error {
	field := fmt.Sprintf("warmProcesses[%d]", index)
	switch process.ProcessType(spec.Type) {
	case process.ProcessTypeREPL:
		if len(spec.Command) > 0 {
			return fmt.Errorf("%s.command is only valid for cmd warm processes", field)
		}
	case process.ProcessTypeCMD:
		if len(spec.Command) == 0 || strings.TrimSpace(spec.Command[0]) == "" {
			return fmt.Errorf("%s.command[0] is required for cmd warm processes", field)
		}
	default:
		return fmt.Errorf("%s.type must be one of: repl, cmd", field)
	}
	if err := validateWarmProbeSet(spec.Probes, field+".probes"); err != nil {
		return err
	}
	return nil
}

func validateWarmProbeSet(probes *warmProbeSet, field string) error {
	if probes == nil {
		return nil
	}
	if err := validateWarmProbe(probes.Startup, field+".startup"); err != nil {
		return err
	}
	if err := validateWarmProbe(probes.Readiness, field+".readiness"); err != nil {
		return err
	}
	return validateWarmProbe(probes.Liveness, field+".liveness")
}

func validateWarmProbe(probe *warmProbeSpec, field string) error {
	if probe == nil {
		return nil
	}
	if probe.TimeoutSeconds < 0 {
		return fmt.Errorf("%s.timeoutSeconds must be non-negative", field)
	}
	if probe.InitialDelaySeconds < 0 {
		return fmt.Errorf("%s.initialDelaySeconds must be non-negative", field)
	}
	configured := 0
	if probe.Process != nil {
		configured++
	}
	if probe.Exec != nil {
		configured++
		if len(probe.Exec.Command) == 0 || strings.TrimSpace(probe.Exec.Command[0]) == "" {
			return fmt.Errorf("%s.exec.command[0] is required", field)
		}
	}
	if probe.HTTPGet != nil {
		configured++
		if _, err := probePort(probe.HTTPGet.Port); err != nil {
			return fmt.Errorf("%s.httpGet.port: %w", field, err)
		}
		if probe.HTTPGet.Scheme != "" && !strings.EqualFold(probe.HTTPGet.Scheme, "http") && !strings.EqualFold(probe.HTTPGet.Scheme, "https") {
			return fmt.Errorf("%s.httpGet.scheme must be http or https", field)
		}
	}
	if probe.TCPSocket != nil {
		configured++
		if _, err := probePort(probe.TCPSocket.Port); err != nil {
			return fmt.Errorf("%s.tcpSocket.port: %w", field, err)
		}
	}
	if configured == 0 {
		return fmt.Errorf("%s must configure one of process, exec, httpGet, or tcpSocket", field)
	}
	if configured > 1 {
		return fmt.Errorf("%s must configure only one of process, exec, httpGet, or tcpSocket", field)
	}
	return nil
}

func startWarmProcesses(manager *ctxpkg.Manager, logger *zap.Logger) ([]warmProcessRuntime, error) {
	if manager == nil {
		return nil, nil
	}
	specs, err := parseWarmProcesses(os.Getenv(warmProcessesEnvVar))
	if err != nil {
		return nil, fmt.Errorf("parse %s: %w", warmProcessesEnvVar, err)
	}
	if len(specs) == 0 {
		return nil, nil
	}

	processes := make([]warmProcessRuntime, 0, len(specs))
	for i, spec := range specs {
		ctx, err := manager.CreateContextWithPolicyAndREPLConfig(process.ProcessConfig{
			Type:    process.ProcessType(spec.Type),
			Alias:   spec.Alias,
			Command: append([]string(nil), spec.Command...),
			CWD:     spec.CWD,
			EnvVars: cloneStringMap(spec.EnvVars),
		}, nil, ctxpkg.CleanupPolicy{})
		if err != nil {
			return nil, fmt.Errorf("start warm process %d: %w", i, err)
		}
		ctx.SetCleanupPolicy(ctxpkg.CleanupPolicy{})
		ctx.AddExitHandler(func(event process.ExitEvent) {
			if logger != nil {
				logger.Error("Warm process exited; terminating procd for Kubernetes restart",
					zap.String("context_id", ctx.ID),
					zap.Int("exit_code", event.ExitCode),
					zap.String("state", string(event.State)),
				)
			}
			warmProcessExit(1)
		})
		processes = append(processes, warmProcessRuntime{Spec: spec, ContextID: ctx.ID, StartedAt: time.Now()})
		if logger != nil {
			logger.Info("Started warm process",
				zap.String("name", spec.Name),
				zap.String("context_id", ctx.ID),
				zap.String("type", spec.Type),
				zap.String("alias", spec.Alias),
				zap.Strings("command", spec.Command),
			)
		}
	}
	return processes, nil
}

func (r *warmProcessProber) Probe(kind sandboxprobe.Kind) sandboxprobe.Response {
	if r == nil {
		return sandboxprobe.Failed(kind, "SandboxProbeUnavailable", "warm process prober is unavailable", nil)
	}
	if !sandboxprobe.ValidKind(kind) {
		return sandboxprobe.Failed(kind, "InvalidProbeKind", fmt.Sprintf("unsupported probe kind %q", kind), nil)
	}

	checks := make([]sandboxprobe.CheckResult, 0, len(r.processes)+1)
	checks = append(checks, sandboxprobe.CheckResult{Name: "procd", Target: "self", Status: sandboxprobe.StatusPassed, Reason: "ProcdRunning"})
	for _, proc := range r.processes {
		checks = append(checks, r.probeProcess(kind, proc))
	}

	result := aggregateProbeResults(kind, checks)
	if kind == sandboxprobe.KindLiveness && result.Status == sandboxprobe.StatusFailed {
		r.exitAfterFailedLiveness(result)
	}
	return result
}

func (r *warmProcessProber) probeProcess(kind sandboxprobe.Kind, proc warmProcessRuntime) sandboxprobe.CheckResult {
	name := proc.Spec.Name
	if name == "" {
		name = proc.ContextID
	}
	probe := proc.Spec.Probes.probeFor(kind)
	if probe == nil {
		return r.probeContextState(kind, name, proc.ContextID)
	}
	if probe.InitialDelaySeconds > 0 && time.Since(proc.StartedAt) < time.Duration(probe.InitialDelaySeconds)*time.Second {
		return sandboxprobe.CheckResult{Name: name, Target: proc.ContextID, Status: sandboxprobe.StatusFailed, Reason: "InitialDelay", Message: "probe initial delay has not elapsed"}
	}
	if probe.Process != nil {
		return r.probeContextState(kind, name, proc.ContextID)
	}
	if probe.Exec != nil {
		return runExecProbe(name, proc, probe)
	}
	if probe.HTTPGet != nil {
		return runHTTPProbe(name, probe)
	}
	if probe.TCPSocket != nil {
		return runTCPProbe(name, probe)
	}
	return r.probeContextState(kind, name, proc.ContextID)
}

func (r *warmProcessProber) probeContextState(kind sandboxprobe.Kind, name, contextID string) sandboxprobe.CheckResult {
	if r == nil || r.manager == nil {
		return sandboxprobe.CheckResult{Name: name, Target: contextID, Status: sandboxprobe.StatusFailed, Reason: "ContextManagerUnavailable", Message: "context manager is unavailable"}
	}
	ctx, err := r.manager.GetContext(contextID)
	if err != nil {
		return sandboxprobe.CheckResult{Name: name, Target: contextID, Status: sandboxprobe.StatusFailed, Reason: "WarmProcessMissing", Message: err.Error()}
	}
	if ctx.IsRunning() {
		return sandboxprobe.CheckResult{Name: name, Target: contextID, Status: sandboxprobe.StatusPassed, Reason: "WarmProcessRunning"}
	}
	if kind == sandboxprobe.KindLiveness && ctx.IsPaused() {
		return sandboxprobe.CheckResult{Name: name, Target: contextID, Status: sandboxprobe.StatusSuspended, Reason: "WarmProcessPaused", Message: "warm process is paused"}
	}
	return sandboxprobe.CheckResult{Name: name, Target: contextID, Status: sandboxprobe.StatusFailed, Reason: "WarmProcessNotRunning", Message: "warm process is not running"}
}

func (r *warmProcessProber) exitAfterFailedLiveness(result sandboxprobe.Response) {
	if r == nil || !r.exitOnFailedLiveness {
		return
	}
	r.exitOnce.Do(func() {
		if r.logger != nil {
			r.logger.Error("Warm process liveness failed; terminating procd for Kubernetes restart",
				zap.String("reason", result.Reason),
				zap.String("message", result.Message),
			)
		}
		go func() {
			time.Sleep(100 * time.Millisecond)
			warmProcessExit(1)
		}()
	})
}

func (s *warmProbeSet) probeFor(kind sandboxprobe.Kind) *warmProbeSpec {
	if s == nil {
		return nil
	}
	switch kind {
	case sandboxprobe.KindStartup:
		return s.Startup
	case sandboxprobe.KindReadiness:
		return s.Readiness
	case sandboxprobe.KindLiveness:
		return s.Liveness
	default:
		return nil
	}
}

func aggregateProbeResults(kind sandboxprobe.Kind, checks []sandboxprobe.CheckResult) sandboxprobe.Response {
	for _, check := range checks {
		if check.Status == sandboxprobe.StatusFailed {
			return sandboxprobe.Failed(kind, check.Reason, check.Message, checks)
		}
	}
	for _, check := range checks {
		if check.Status == sandboxprobe.StatusSuspended {
			return sandboxprobe.Suspended(kind, check.Reason, check.Message, checks)
		}
	}
	return sandboxprobe.Passed(kind, "SandboxProbePassed", "sandbox probe passed", checks)
}

func runExecProbe(name string, proc warmProcessRuntime, probe *warmProbeSpec) sandboxprobe.CheckResult {
	if len(probe.Exec.Command) == 0 || strings.TrimSpace(probe.Exec.Command[0]) == "" {
		return sandboxprobe.CheckResult{Name: name, Target: "exec", Status: sandboxprobe.StatusFailed, Reason: "InvalidProbe", Message: "exec command is required"}
	}
	ctx, cancel := context.WithTimeout(context.Background(), probeTimeout(probe))
	defer cancel()
	cmd := exec.CommandContext(ctx, probe.Exec.Command[0], probe.Exec.Command[1:]...)
	if proc.Spec.CWD != "" {
		cmd.Dir = proc.Spec.CWD
	}
	cmd.Env = os.Environ()
	for key, value := range proc.Spec.EnvVars {
		cmd.Env = append(cmd.Env, key+"="+value)
	}
	output, err := cmd.CombinedOutput()
	if ctx.Err() == context.DeadlineExceeded {
		return sandboxprobe.CheckResult{Name: name, Target: "exec", Status: sandboxprobe.StatusFailed, Reason: "ProbeTimeout", Message: "exec probe timed out"}
	}
	if err != nil {
		message := strings.TrimSpace(string(output))
		if message == "" {
			message = err.Error()
		}
		return sandboxprobe.CheckResult{Name: name, Target: "exec", Status: sandboxprobe.StatusFailed, Reason: "ExecProbeFailed", Message: message}
	}
	return sandboxprobe.CheckResult{Name: name, Target: "exec", Status: sandboxprobe.StatusPassed, Reason: "ExecProbePassed"}
}

func runHTTPProbe(name string, probe *warmProbeSpec) sandboxprobe.CheckResult {
	port, err := probePort(probe.HTTPGet.Port)
	if err != nil {
		return sandboxprobe.CheckResult{Name: name, Target: "http", Status: sandboxprobe.StatusFailed, Reason: "InvalidProbe", Message: err.Error()}
	}
	scheme := strings.ToLower(strings.TrimSpace(probe.HTTPGet.Scheme))
	if scheme == "" {
		scheme = "http"
	}
	host := strings.TrimSpace(probe.HTTPGet.Host)
	if host == "" {
		host = "127.0.0.1"
	}
	path := probe.HTTPGet.Path
	if path == "" {
		path = "/"
	}
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	url := fmt.Sprintf("%s://%s%s", scheme, net.JoinHostPort(host, strconv.Itoa(port)), path)
	ctx, cancel := context.WithTimeout(context.Background(), probeTimeout(probe))
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return sandboxprobe.CheckResult{Name: name, Target: url, Status: sandboxprobe.StatusFailed, Reason: "InvalidProbe", Message: err.Error()}
	}
	for _, header := range probe.HTTPGet.HTTPHeaders {
		req.Header.Add(header.Name, header.Value)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return sandboxprobe.CheckResult{Name: name, Target: url, Status: sandboxprobe.StatusFailed, Reason: "HTTPProbeFailed", Message: err.Error()}
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 400 {
		return sandboxprobe.CheckResult{Name: name, Target: url, Status: sandboxprobe.StatusFailed, Reason: "HTTPProbeFailed", Message: fmt.Sprintf("status code %d", resp.StatusCode)}
	}
	return sandboxprobe.CheckResult{Name: name, Target: url, Status: sandboxprobe.StatusPassed, Reason: "HTTPProbePassed"}
}

func runTCPProbe(name string, probe *warmProbeSpec) sandboxprobe.CheckResult {
	port, err := probePort(probe.TCPSocket.Port)
	if err != nil {
		return sandboxprobe.CheckResult{Name: name, Target: "tcp", Status: sandboxprobe.StatusFailed, Reason: "InvalidProbe", Message: err.Error()}
	}
	host := strings.TrimSpace(probe.TCPSocket.Host)
	if host == "" {
		host = "127.0.0.1"
	}
	addr := net.JoinHostPort(host, strconv.Itoa(port))
	conn, err := net.DialTimeout("tcp", addr, probeTimeout(probe))
	if err != nil {
		return sandboxprobe.CheckResult{Name: name, Target: addr, Status: sandboxprobe.StatusFailed, Reason: "TCPProbeFailed", Message: err.Error()}
	}
	_ = conn.Close()
	return sandboxprobe.CheckResult{Name: name, Target: addr, Status: sandboxprobe.StatusPassed, Reason: "TCPProbePassed"}
}

func probePort(port intstr.IntOrString) (int, error) {
	if port.Type == intstr.Int {
		if port.IntVal <= 0 {
			return 0, fmt.Errorf("probe port must be positive")
		}
		return int(port.IntVal), nil
	}
	value := strings.TrimSpace(port.StrVal)
	if value == "" {
		return 0, fmt.Errorf("probe port is required")
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed <= 0 {
		return 0, fmt.Errorf("probe named ports are not supported by procd: %q", value)
	}
	return parsed, nil
}

func probeTimeout(probe *warmProbeSpec) time.Duration {
	if probe != nil && probe.TimeoutSeconds > 0 {
		return time.Duration(probe.TimeoutSeconds) * time.Second
	}
	return time.Second
}

func cloneStringMap(src map[string]string) map[string]string {
	if len(src) == 0 {
		return nil
	}
	cloned := make(map[string]string, len(src))
	for key, value := range src {
		cloned[key] = value
	}
	return cloned
}
