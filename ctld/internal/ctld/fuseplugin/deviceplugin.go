package fuseplugin

import (
	context2 "context"
	"fmt"
	"log"
	"net"
	"os"
	"path"
	"time"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
	pluginapi "k8s.io/kubelet/pkg/apis/deviceplugin/v1beta1"
)

const (
	ResourceName = "sandbox0.ai/fuse"
	ServerSock   = pluginapi.DevicePluginPath + "fuse.sock"
)

// DevicePlugin implements the Kubernetes device plugin API for /dev/fuse.
type DevicePlugin struct {
	devs   []*pluginapi.Device
	socket string

	stop   chan any
	health chan *pluginapi.Device

	server *grpc.Server
}

func NewDevicePlugin(number int) *DevicePlugin {
	return &DevicePlugin{
		devs:   getDevices(number),
		socket: ServerSock,

		stop:   make(chan any),
		health: make(chan *pluginapi.Device),
	}
}

func (m *DevicePlugin) GetPreferredAllocation(_ context2.Context, _ *pluginapi.PreferredAllocationRequest) (*pluginapi.PreferredAllocationResponse, error) {
	return &pluginapi.PreferredAllocationResponse{}, nil
}

func (m *DevicePlugin) GetDevicePluginOptions(context.Context, *pluginapi.Empty) (*pluginapi.DevicePluginOptions, error) {
	return &pluginapi.DevicePluginOptions{}, nil
}

func (m *DevicePlugin) PreStartContainer(context.Context, *pluginapi.PreStartContainerRequest) (*pluginapi.PreStartContainerResponse, error) {
	return &pluginapi.PreStartContainerResponse{}, nil
}

func dial(unixSocketPath string, timeout time.Duration) (*grpc.ClientConn, error) {
	c, err := grpc.Dial(unixSocketPath, grpc.WithInsecure(), grpc.WithBlock(),
		grpc.WithTimeout(timeout),
		grpc.WithDialer(func(addr string, timeout time.Duration) (net.Conn, error) {
			return net.DialTimeout("unix", addr, timeout)
		}),
	)
	if err != nil {
		return nil, err
	}
	return c, nil
}

func (m *DevicePlugin) Start() error {
	if err := m.cleanup(); err != nil {
		return err
	}

	sock, err := net.Listen("unix", m.socket)
	if err != nil {
		return err
	}

	m.server = grpc.NewServer([]grpc.ServerOption{}...)
	pluginapi.RegisterDevicePluginServer(m.server, m)

	go m.server.Serve(sock)

	conn, err := dial(m.socket, 5*time.Second)
	if err != nil {
		return err
	}
	conn.Close()

	go m.healthcheck()

	return nil
}

func (m *DevicePlugin) Stop() error {
	if m.server == nil {
		return nil
	}

	m.server.Stop()
	m.server = nil
	close(m.stop)

	return m.cleanup()
}

func (m *DevicePlugin) Register(kubeletEndpoint, resourceName string) error {
	conn, err := dial(kubeletEndpoint, 5*time.Second)
	if err != nil {
		return err
	}
	defer conn.Close()

	client := pluginapi.NewRegistrationClient(conn)
	reqt := &pluginapi.RegisterRequest{
		Version:      pluginapi.Version,
		Endpoint:     path.Base(m.socket),
		ResourceName: resourceName,
	}

	_, err = client.Register(context.Background(), reqt)
	return err
}

func (m *DevicePlugin) ListAndWatch(e *pluginapi.Empty, s pluginapi.DevicePlugin_ListAndWatchServer) error {
	s.Send(&pluginapi.ListAndWatchResponse{Devices: m.devs})

	for {
		select {
		case <-m.stop:
			return nil
		case d := <-m.health:
			d.Health = pluginapi.Unhealthy
			s.Send(&pluginapi.ListAndWatchResponse{Devices: m.devs})
		}
	}
}

func (m *DevicePlugin) Allocate(ctx context.Context, reqs *pluginapi.AllocateRequest) (*pluginapi.AllocateResponse, error) {
	devs := m.devs
	var responses pluginapi.AllocateResponse

	for _, req := range reqs.ContainerRequests {
		for _, id := range req.DevicesIDs {
			if !deviceExists(devs, id) {
				return nil, fmt.Errorf("invalid allocation request: unknown device: %s", id)
			}

			response := new(pluginapi.ContainerAllocateResponse)
			response.Devices = []*pluginapi.DeviceSpec{{
				ContainerPath: "/dev/fuse",
				HostPath:      "/dev/fuse",
				Permissions:   "rwm",
			}}

			responses.ContainerResponses = append(responses.ContainerResponses, response)
		}
	}

	return &responses, nil
}

func (m *DevicePlugin) cleanup() error {
	if err := os.Remove(m.socket); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

func (m *DevicePlugin) healthcheck() {
	for range m.stop {
		return
	}
}

func (m *DevicePlugin) Serve() error {
	if err := m.Start(); err != nil {
		log.Printf("Could not start device plugin: %s", err)
		return err
	}
	log.Println("Starting to serve on", m.socket)

	if err := m.Register(pluginapi.KubeletSocket, ResourceName); err != nil {
		log.Printf("Could not register device plugin: %s", err)
		m.Stop()
		return err
	}
	log.Println("Registered device plugin with Kubelet")

	return nil
}

func getDevices(number int) []*pluginapi.Device {
	hostname, _ := os.Hostname()
	devs := []*pluginapi.Device{}
	for i := 0; i < number; i++ {
		devs = append(devs, &pluginapi.Device{ID: fmt.Sprintf("fuse-%s-%d", hostname, i), Health: pluginapi.Healthy})
	}
	return devs
}

func deviceExists(devs []*pluginapi.Device, id string) bool {
	for _, d := range devs {
		if d.ID == id {
			return true
		}
	}
	return false
}

var _ pluginapi.DevicePluginServer = &DevicePlugin{}
