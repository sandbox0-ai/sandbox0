//go:build linux

package ha

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"
)

type ProbeServer struct {
	coordinator  *Coordinator
	socket       string
	listener     *net.UnixListener
	serviceReady atomic.Bool
}

type probeResponse struct {
	State        State `json:"state"`
	ServiceReady bool  `json:"service_ready"`
}

func StartProbeServer(ctx context.Context, socket string, coordinator *Coordinator) (*ProbeServer, error) {
	if strings.TrimSpace(socket) == "" {
		return nil, fmt.Errorf("ctld HA probe socket is required")
	}
	if coordinator == nil {
		return nil, fmt.Errorf("ctld HA coordinator is required")
	}
	if err := os.MkdirAll(filepath.Dir(socket), 0o755); err != nil {
		return nil, fmt.Errorf("create ctld HA probe directory: %w", err)
	}
	if err := os.Remove(socket); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("remove stale ctld HA probe socket: %w", err)
	}
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: socket, Net: "unix"})
	if err != nil {
		return nil, fmt.Errorf("listen on ctld HA probe socket: %w", err)
	}
	if err := os.Chmod(socket, 0o600); err != nil {
		_ = listener.Close()
		return nil, err
	}
	server := &ProbeServer{coordinator: coordinator, socket: socket, listener: listener}
	go server.serve(ctx)
	return server, nil
}

func (s *ProbeServer) SetServiceReady(ready bool) {
	if s != nil {
		s.serviceReady.Store(ready)
	}
}

func (s *ProbeServer) Close() error {
	if s == nil {
		return nil
	}
	err := s.listener.Close()
	_ = os.Remove(s.socket)
	return err
}

func (s *ProbeServer) serve(ctx context.Context) {
	go func() {
		<-ctx.Done()
		_ = s.listener.Close()
	}()
	for {
		conn, err := s.listener.AcceptUnix()
		if err != nil {
			return
		}
		response := probeResponse{State: s.coordinator.State(), ServiceReady: s.serviceReady.Load()}
		_ = json.NewEncoder(conn).Encode(response)
		_ = conn.Close()
	}
}

func RunProbe(ctx context.Context, socket, kind, activeHTTPAddr string) error {
	kind = strings.TrimSpace(kind)
	if kind != "live" && kind != "ready" {
		return fmt.Errorf("unsupported ctld HA probe %q", kind)
	}
	dialer := net.Dialer{Timeout: 2 * time.Second}
	conn, err := dialer.DialContext(ctx, "unix", socket)
	if err != nil {
		return fmt.Errorf("dial ctld HA probe: %w", err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
	var response probeResponse
	if err := json.NewDecoder(bufio.NewReader(conn)).Decode(&response); err != nil {
		return fmt.Errorf("read ctld HA probe: %w", err)
	}
	if kind == "ready" {
		switch response.State.Role {
		case RolePrimary:
			if !response.ServiceReady || !response.State.Synchronized {
				return fmt.Errorf("ctld primary is not ready")
			}
		case RoleStandby:
			if !response.State.Synchronized {
				return fmt.Errorf("ctld standby is not synchronized")
			}
		default:
			return fmt.Errorf("ctld role is %q", response.State.Role)
		}
		return nil
	}
	if response.State.Role != RolePrimary {
		return nil
	}
	if !response.ServiceReady {
		return nil
	}
	url := activeProbeURL(activeHTTPAddr)
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	client := &http.Client{Timeout: 2 * time.Second}
	httpResponse, err := client.Do(request)
	if err != nil {
		return fmt.Errorf("probe active ctld HTTP server: %w", err)
	}
	defer httpResponse.Body.Close()
	if httpResponse.StatusCode < 200 || httpResponse.StatusCode >= 300 {
		return fmt.Errorf("active ctld health returned %s", httpResponse.Status)
	}
	return nil
}

func activeProbeURL(addr string) string {
	addr = strings.TrimSpace(addr)
	if strings.HasPrefix(addr, ":") {
		addr = "127.0.0.1" + addr
	}
	if host, port, err := net.SplitHostPort(addr); err == nil && (host == "" || host == "0.0.0.0" || host == "::") {
		addr = net.JoinHostPort("127.0.0.1", port)
	}
	return "http://" + addr + "/healthz"
}
