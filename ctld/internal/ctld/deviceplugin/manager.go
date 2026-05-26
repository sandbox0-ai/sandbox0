package deviceplugin

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/sandbox0-ai/sandbox0/pkg/sandboxdevices"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	pluginapi "k8s.io/kubelet/pkg/apis/deviceplugin/v1beta1"
)

const (
	defaultCapacity      = 256
	defaultHealthPoll    = 10 * time.Second
	defaultRegisterRetry = 5 * time.Second
)

type Config struct {
	PluginDir string
	Capacity  int
	Logger    *zap.Logger
}

type Manager struct {
	plugins []*Plugin
	logger  *zap.Logger
}

func NewManager(cfg Config) *Manager {
	logger := cfg.Logger
	if logger == nil {
		logger = zap.NewNop()
	}
	pluginDir := strings.TrimSpace(cfg.PluginDir)
	if pluginDir == "" {
		pluginDir = pluginapi.DevicePluginPath
	}
	capacity := cfg.Capacity
	if capacity <= 0 {
		capacity = defaultCapacity
	}

	resources := []string{
		sandboxdevices.ResourceFuse,
		sandboxdevices.ResourceNetTun,
	}
	plugins := make([]*Plugin, 0, len(resources))
	for _, resourceName := range resources {
		nodes, _ := sandboxdevices.DeviceNodesForResource(resourceName)
		plugins = append(plugins, NewPlugin(PluginConfig{
			ResourceName: resourceName,
			DeviceNodes:  nodes,
			PluginDir:    pluginDir,
			Capacity:     capacity,
			Logger:       logger,
		}))
	}
	return &Manager{plugins: plugins, logger: logger}
}

func (m *Manager) Run(ctx context.Context) {
	if m == nil {
		return
	}
	for _, plugin := range m.plugins {
		plugin := plugin
		go plugin.Run(ctx)
	}
	<-ctx.Done()
	for _, plugin := range m.plugins {
		plugin.Stop()
	}
}

type PluginConfig struct {
	ResourceName string
	DeviceNodes  []sandboxdevices.DeviceNode
	PluginDir    string
	Capacity     int
	Logger       *zap.Logger
}

type Plugin struct {
	pluginapi.UnimplementedDevicePluginServer

	resourceName string
	deviceNodes  []sandboxdevices.DeviceNode
	pluginDir    string
	socketPath   string
	deviceIDBase string
	capacity     int
	logger       *zap.Logger
	mu           sync.Mutex
	server       *grpc.Server
}

func NewPlugin(cfg PluginConfig) *Plugin {
	logger := cfg.Logger
	if logger == nil {
		logger = zap.NewNop()
	}
	capacity := cfg.Capacity
	if capacity <= 0 {
		capacity = defaultCapacity
	}
	pluginDir := strings.TrimSpace(cfg.PluginDir)
	if pluginDir == "" {
		pluginDir = pluginapi.DevicePluginPath
	}
	socketName := strings.NewReplacer("/", "-", ".", "-").Replace(cfg.ResourceName) + ".sock"
	deviceIDBase := strings.TrimSuffix(socketName, ".sock")
	return &Plugin{
		resourceName: cfg.ResourceName,
		deviceNodes:  append([]sandboxdevices.DeviceNode(nil), cfg.DeviceNodes...),
		pluginDir:    pluginDir,
		socketPath:   filepath.Join(pluginDir, socketName),
		deviceIDBase: deviceIDBase,
		capacity:     capacity,
		logger:       logger.With(zap.String("resource", cfg.ResourceName)),
	}
}

func (p *Plugin) Run(ctx context.Context) {
	for ctx.Err() == nil {
		if err := p.startAndRegister(ctx); err != nil && ctx.Err() == nil {
			p.logger.Warn("device plugin registration failed", zap.Error(err))
			timer := time.NewTimer(defaultRegisterRetry)
			select {
			case <-ctx.Done():
				timer.Stop()
			case <-timer.C:
			}
			continue
		}
		return
	}
}

func (p *Plugin) Stop() {
	if p == nil {
		return
	}
	p.mu.Lock()
	server := p.server
	p.server = nil
	p.mu.Unlock()
	if server != nil {
		server.Stop()
	}
	_ = os.Remove(p.socketPath)
}

func (p *Plugin) startAndRegister(ctx context.Context) error {
	if err := p.startServer(); err != nil {
		return err
	}
	if err := p.register(ctx); err != nil {
		p.Stop()
		return err
	}
	p.logger.Info("registered sandbox device plugin")
	if err := p.waitForKubeletRestart(ctx); err != nil && ctx.Err() == nil {
		p.Stop()
		return err
	}
	p.Stop()
	return nil
}

func (p *Plugin) startServer() error {
	if err := os.MkdirAll(p.pluginDir, 0o755); err != nil {
		return fmt.Errorf("create device plugin directory: %w", err)
	}
	_ = os.Remove(p.socketPath)
	listener, err := net.Listen("unix", p.socketPath)
	if err != nil {
		return fmt.Errorf("listen on device plugin socket: %w", err)
	}
	server := grpc.NewServer()
	pluginapi.RegisterDevicePluginServer(server, p)
	p.mu.Lock()
	p.server = server
	p.mu.Unlock()
	go func() {
		if err := server.Serve(listener); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			p.logger.Warn("device plugin grpc server stopped", zap.Error(err))
		}
	}()
	return nil
}

func (p *Plugin) register(ctx context.Context) error {
	kubeletSocket := filepath.Join(p.pluginDir, "kubelet.sock")
	dialCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(
		dialCtx,
		kubeletSocket,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, addr string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", addr)
		}),
		grpc.WithBlock(),
	)
	if err != nil {
		return fmt.Errorf("connect to kubelet device plugin socket: %w", err)
	}
	defer conn.Close()

	client := pluginapi.NewRegistrationClient(conn)
	_, err = client.Register(ctx, &pluginapi.RegisterRequest{
		Version:      pluginapi.Version,
		Endpoint:     filepath.Base(p.socketPath),
		ResourceName: p.resourceName,
		Options:      &pluginapi.DevicePluginOptions{},
	})
	if err != nil {
		return fmt.Errorf("register with kubelet: %w", err)
	}
	return nil
}

func (p *Plugin) waitForKubeletRestart(ctx context.Context) error {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		p.logger.Warn("device plugin kubelet socket watcher disabled", zap.Error(err))
		<-ctx.Done()
		return nil
	}
	defer watcher.Close()
	if err := watcher.Add(p.pluginDir); err != nil {
		p.logger.Warn("device plugin directory watcher disabled", zap.Error(err))
		<-ctx.Done()
		return nil
	}

	kubeletSocket := filepath.Join(p.pluginDir, "kubelet.sock")
	for {
		select {
		case <-ctx.Done():
			return nil
		case err := <-watcher.Errors:
			if err != nil {
				p.logger.Warn("device plugin directory watcher error", zap.Error(err))
			}
		case event := <-watcher.Events:
			if event.Name != kubeletSocket {
				continue
			}
			if event.Has(fsnotify.Remove) || event.Has(fsnotify.Rename) || event.Has(fsnotify.Create) {
				return fmt.Errorf("kubelet device plugin socket changed")
			}
		}
	}
}

func (p *Plugin) GetDevicePluginOptions(context.Context, *pluginapi.Empty) (*pluginapi.DevicePluginOptions, error) {
	return &pluginapi.DevicePluginOptions{}, nil
}

func (p *Plugin) ListAndWatch(_ *pluginapi.Empty, stream pluginapi.DevicePlugin_ListAndWatchServer) error {
	if err := stream.Send(&pluginapi.ListAndWatchResponse{Devices: p.devices()}); err != nil {
		return err
	}
	ticker := time.NewTicker(defaultHealthPoll)
	defer ticker.Stop()
	for {
		select {
		case <-stream.Context().Done():
			return nil
		case <-ticker.C:
			if err := stream.Send(&pluginapi.ListAndWatchResponse{Devices: p.devices()}); err != nil {
				return err
			}
		}
	}
}

func (p *Plugin) Allocate(_ context.Context, req *pluginapi.AllocateRequest) (*pluginapi.AllocateResponse, error) {
	if !p.available() {
		return nil, fmt.Errorf("resource %s is unavailable on this node", p.resourceName)
	}
	response := &pluginapi.AllocateResponse{
		ContainerResponses: make([]*pluginapi.ContainerAllocateResponse, 0, len(req.ContainerRequests)),
	}
	for _, containerReq := range req.ContainerRequests {
		if len(containerReq.DevicesIds) == 0 {
			return nil, fmt.Errorf("resource %s allocation requires at least one device ID", p.resourceName)
		}
		response.ContainerResponses = append(response.ContainerResponses, &pluginapi.ContainerAllocateResponse{
			Devices: p.deviceSpecs(),
		})
	}
	return response, nil
}

func (p *Plugin) PreStartContainer(context.Context, *pluginapi.PreStartContainerRequest) (*pluginapi.PreStartContainerResponse, error) {
	return &pluginapi.PreStartContainerResponse{}, nil
}

func (p *Plugin) GetPreferredAllocation(context.Context, *pluginapi.PreferredAllocationRequest) (*pluginapi.PreferredAllocationResponse, error) {
	return &pluginapi.PreferredAllocationResponse{}, nil
}

func (p *Plugin) devices() []*pluginapi.Device {
	health := pluginapi.Healthy
	if !p.available() {
		health = pluginapi.Unhealthy
	}
	devices := make([]*pluginapi.Device, 0, p.capacity)
	for i := 0; i < p.capacity; i++ {
		devices = append(devices, &pluginapi.Device{
			ID:     fmt.Sprintf("%s-%d", p.deviceIDBase, i),
			Health: health,
		})
	}
	return devices
}

func (p *Plugin) available() bool {
	if len(p.deviceNodes) == 0 {
		return false
	}
	for _, node := range p.deviceNodes {
		if _, err := os.Stat(node.HostPath); err != nil {
			return false
		}
	}
	return true
}

func (p *Plugin) deviceSpecs() []*pluginapi.DeviceSpec {
	specs := make([]*pluginapi.DeviceSpec, 0, len(p.deviceNodes))
	for _, node := range p.deviceNodes {
		specs = append(specs, &pluginapi.DeviceSpec{
			HostPath:      node.HostPath,
			ContainerPath: node.ContainerPath,
			Permissions:   node.Permissions,
		})
	}
	return specs
}
