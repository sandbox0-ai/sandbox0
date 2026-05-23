package http

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/functions"
	meteringpkg "github.com/sandbox0-ai/sandbox0/pkg/metering"
	"go.uber.org/zap"
)

const functionUsageProducer = "function_gateway.requests"

type functionUsageMeter struct {
	repo      *meteringpkg.Repository
	regionID  string
	clusterID string
	logger    *zap.Logger
	now       func() time.Time

	mu     sync.Mutex
	active map[string]*functionActiveWindow
}

type functionActiveWindow struct {
	start             time.Time
	count             int
	teamID            string
	userID            string
	functionID        string
	revisionID        string
	runtimeID         string
	sandboxID         string
	routeID           string
	resourceMillicpu  int64
	resourceMemoryMiB int64
}

type functionRequestWindow struct {
	requestID         string
	start             time.Time
	teamID            string
	userID            string
	functionID        string
	revisionID        string
	runtimeID         string
	sandboxID         string
	routeID           string
	resourceMillicpu  int64
	resourceMemoryMiB int64
}

func newFunctionUsageMeter(repo *meteringpkg.Repository, regionID, clusterID string, logger *zap.Logger) *functionUsageMeter {
	if repo == nil {
		return nil
	}
	if logger == nil {
		logger = zap.NewNop()
	}
	return &functionUsageMeter{
		repo:      repo,
		regionID:  strings.TrimSpace(regionID),
		clusterID: strings.TrimSpace(clusterID),
		logger:    logger,
		now: func() time.Time {
			return time.Now().UTC()
		},
		active: make(map[string]*functionActiveWindow),
	}
}

func (m *functionUsageMeter) Begin(ctx context.Context, fn *functions.Function, rev *functions.Revision, lease *functionRuntimeLease, routeID string, requestStarted time.Time) func(int) {
	if m == nil || m.repo == nil || fn == nil || rev == nil || lease == nil || lease.Sandbox == nil {
		return func(int) {}
	}
	start := m.now()
	if !requestStarted.IsZero() {
		start = requestStarted.UTC()
	}
	req := functionRequestWindow{
		requestID:  uuid.NewString(),
		start:      start,
		teamID:     strings.TrimSpace(fn.TeamID),
		userID:     strings.TrimSpace(rev.CreatedBy),
		functionID: strings.TrimSpace(fn.ID),
		revisionID: strings.TrimSpace(rev.ID),
		sandboxID:  strings.TrimSpace(lease.Sandbox.ID),
		routeID:    strings.TrimSpace(routeID),
	}
	if lease.Instance != nil {
		req.runtimeID = strings.TrimSpace(lease.Instance.ID)
	}
	if req.runtimeID == "" {
		req.runtimeID = strings.TrimSpace(req.sandboxID)
	}
	req.resourceMillicpu, req.resourceMemoryMiB = m.lookupSandboxResources(ctx, req.sandboxID)

	key := functionRuntimeUsageKey(req.functionID, req.revisionID, req.runtimeID, req.sandboxID)
	m.mu.Lock()
	active := m.active[key]
	if active == nil {
		active = &functionActiveWindow{
			start:             start,
			teamID:            req.teamID,
			userID:            req.userID,
			functionID:        req.functionID,
			revisionID:        req.revisionID,
			runtimeID:         req.runtimeID,
			sandboxID:         req.sandboxID,
			routeID:           req.routeID,
			resourceMillicpu:  req.resourceMillicpu,
			resourceMemoryMiB: req.resourceMemoryMiB,
		}
		m.active[key] = active
	}
	active.count++
	m.mu.Unlock()

	return func(status int) {
		m.finish(key, req, status)
	}
}

func (m *functionUsageMeter) lookupSandboxResources(ctx context.Context, sandboxID string) (int64, int64) {
	if strings.TrimSpace(sandboxID) == "" {
		return 0, 0
	}
	state, err := m.repo.GetSandboxProjectionState(ctx, sandboxID)
	if err != nil {
		m.logger.Warn("Failed to load function runtime metering resources",
			zap.String("sandbox_id", sandboxID),
			zap.Error(err),
		)
		return 0, 0
	}
	if state == nil {
		return 0, 0
	}
	return state.ResourceMillicpu, state.ResourceMemoryMiB
}

func (m *functionUsageMeter) finish(key string, req functionRequestWindow, status int) {
	end := m.now()
	var active *functionActiveWindow
	m.mu.Lock()
	current := m.active[key]
	if current != nil {
		current.count--
		if current.count <= 0 {
			active = current
			delete(m.active, key)
		}
	}
	m.mu.Unlock()

	windows := []*meteringpkg.Window{
		m.requestCountWindow(req, end, status),
		m.requestDurationWindow(req, end, status),
	}
	windows = append(windows, m.activeWindows(active, end)...)
	m.appendWindows(windows, end)
}

func (m *functionUsageMeter) appendWindows(windows []*meteringpkg.Window, completeBefore time.Time) {
	if len(windows) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := m.repo.InTx(ctx, func(tx pgx.Tx) error {
		for _, window := range windows {
			if window == nil {
				continue
			}
			if err := m.repo.AppendWindowTx(ctx, tx, window); err != nil {
				return err
			}
		}
		return m.repo.UpsertProducerWatermarkTx(ctx, tx, functionUsageProducer, m.regionID, completeBefore)
	}); err != nil {
		m.logger.Warn("Failed to append function usage windows", zap.Error(err))
	}
}

func (m *functionUsageMeter) requestCountWindow(req functionRequestWindow, end time.Time, status int) *meteringpkg.Window {
	return m.functionWindow(
		fmt.Sprintf("function/%s/request/%s/count", req.functionID, req.requestID),
		meteringpkg.WindowTypeFunctionRequestCount,
		req.start,
		end,
		1,
		meteringpkg.WindowUnitCount,
		req,
		status,
	)
}

func (m *functionUsageMeter) requestDurationWindow(req functionRequestWindow, end time.Time, status int) *meteringpkg.Window {
	value := end.Sub(req.start).Milliseconds()
	if value < 0 {
		value = 0
	}
	return m.functionWindow(
		fmt.Sprintf("function/%s/request/%s/duration", req.functionID, req.requestID),
		meteringpkg.WindowTypeFunctionRequestDurationMilliseconds,
		req.start,
		end,
		value,
		meteringpkg.WindowUnitMilliseconds,
		req,
		status,
	)
}

func (m *functionUsageMeter) activeWindows(active *functionActiveWindow, end time.Time) []*meteringpkg.Window {
	if active == nil || active.start.IsZero() || !end.After(active.start) {
		return nil
	}
	durationMS := end.Sub(active.start).Milliseconds()
	if durationMS <= 0 {
		return nil
	}
	req := functionRequestWindow{
		start:             active.start,
		teamID:            active.teamID,
		userID:            active.userID,
		functionID:        active.functionID,
		revisionID:        active.revisionID,
		runtimeID:         active.runtimeID,
		sandboxID:         active.sandboxID,
		routeID:           active.routeID,
		resourceMillicpu:  active.resourceMillicpu,
		resourceMemoryMiB: active.resourceMemoryMiB,
	}
	prefix := fmt.Sprintf("function/%s/runtime/%s/active/%d/%d", active.functionID, active.runtimeID, active.start.UTC().UnixNano(), end.UTC().UnixNano())
	windows := []*meteringpkg.Window{
		m.functionWindow(prefix+"/runtime", meteringpkg.WindowTypeFunctionActiveRuntimeMilliseconds, active.start, end, durationMS, meteringpkg.WindowUnitMilliseconds, req, 0),
	}
	if active.resourceMillicpu > 0 {
		windows = append(windows, m.functionWindow(prefix+"/cpu", meteringpkg.WindowTypeFunctionActiveMillicpuMilliseconds, active.start, end, active.resourceMillicpu*durationMS, meteringpkg.WindowUnitMillicpuMilliseconds, req, 0))
	}
	if active.resourceMemoryMiB > 0 {
		windows = append(windows, m.functionWindow(prefix+"/memory", meteringpkg.WindowTypeFunctionActiveMiBMilliseconds, active.start, end, active.resourceMemoryMiB*durationMS, meteringpkg.WindowUnitMiBMilliseconds, req, 0))
	}
	return windows
}

func (m *functionUsageMeter) functionWindow(windowID, windowType string, start, end time.Time, value int64, unit string, req functionRequestWindow, status int) *meteringpkg.Window {
	data := map[string]any{
		"product":                      meteringpkg.ProductFunction,
		"function_id":                  req.functionID,
		"function_revision_id":         req.revisionID,
		"function_runtime_instance_id": req.runtimeID,
		"runtime_sandbox_id":           req.sandboxID,
		"route_id":                     req.routeID,
		"resource_millicpu":            req.resourceMillicpu,
		"resource_memory_mib":          req.resourceMemoryMiB,
	}
	if status > 0 {
		data["http_status"] = status
	}
	return &meteringpkg.Window{
		WindowID:    windowID,
		Producer:    functionUsageProducer,
		RegionID:    m.regionID,
		WindowType:  windowType,
		SubjectType: meteringpkg.SubjectTypeFunction,
		SubjectID:   req.functionID,
		TeamID:      req.teamID,
		UserID:      req.userID,
		SandboxID:   req.sandboxID,
		ClusterID:   m.clusterID,
		WindowStart: start,
		WindowEnd:   end,
		Value:       value,
		Unit:        unit,
		Data:        mustJSON(data),
	}
}

func functionRuntimeUsageKey(functionID, revisionID, runtimeID, sandboxID string) string {
	return strings.Join([]string{
		strings.TrimSpace(functionID),
		strings.TrimSpace(revisionID),
		strings.TrimSpace(runtimeID),
		strings.TrimSpace(sandboxID),
	}, "/")
}

func mustJSON(value any) json.RawMessage {
	if value == nil {
		return json.RawMessage(`{}`)
	}
	payload, err := json.Marshal(value)
	if err != nil {
		return json.RawMessage(`{}`)
	}
	return payload
}
