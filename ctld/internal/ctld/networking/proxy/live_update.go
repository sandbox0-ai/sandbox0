package proxy

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"time"

	"github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/networking/policy"
	"github.com/sandbox0-ai/sandbox0/pkg/config"
)

func (s *Server) PolicyStore() *policy.Store { return s.store }

// ValidateLiveListeners duplicates and inspects descriptors without accepting
// traffic or closing the source's references. Reject before ownership commits.
func ValidateLiveListeners(cfg *config.NetworkRuntimeConfig, files []*os.File) error {
	if cfg == nil || (len(files) != 3 && len(files) != 4) ||
		(cfg.ProxyHTTPPort == cfg.ProxyHTTPSPort) != (len(files) == 3) {
		return fmt.Errorf("inherited proxy listener topology changed")
	}
	ports := []int{cfg.ProxyHTTPPort, cfg.ProxyHTTPSPort, cfg.ProxyHTTPSPort}
	if len(files) == 4 {
		ports = append(ports, cfg.ProxyHTTPPort)
	}
	for i, file := range files {
		address := net.JoinHostPort(cfg.ProxyListenAddr, fmt.Sprintf("%d", ports[i]))
		if i < 2 {
			listener, err := net.FileListener(file)
			if err != nil {
				return err
			}
			_, tcp := listener.(*net.TCPListener)
			valid := tcp && listener.Addr().String() == address
			_ = listener.Close()
			if !valid {
				return fmt.Errorf("inherited TCP listener changed")
			}
		} else {
			connection, err := net.FilePacketConn(file)
			if err != nil {
				return err
			}
			_, udp := connection.(*net.UDPConn)
			valid := udp && connection.LocalAddr().String() == address
			_ = connection.Close()
			if !valid {
				return fmt.Errorf("inherited UDP listener changed")
			}
		}
	}
	return nil
}

func (s *Server) ExportListeners() (files []*os.File, result error) {
	defer func() {
		if result != nil {
			for _, file := range files {
				_ = file.Close()
			}
		}
	}()
	if s.closing.Load() || s.draining.Load() {
		return nil, fmt.Errorf("proxy is already closing")
	}
	for _, listener := range []net.Listener{s.httpListener, s.httpsListener} {
		exporter, ok := listener.(interface{ File() (*os.File, error) })
		if !ok {
			return files, fmt.Errorf("proxy listener cannot transfer")
		}
		file, err := exporter.File()
		if err != nil {
			return files, err
		}
		files = append(files, file)
	}
	for _, connection := range []*net.UDPConn{s.udpHTTPSConn, s.udpHTTPConn} {
		if connection == s.udpHTTPConn && connection == s.udpHTTPSConn && len(files) == 3 {
			break
		}
		file, err := connection.File()
		if err != nil {
			return files, err
		}
		files = append(files, file)
	}
	return files, nil
}

// Drain stops new admission while keeping established TCP/TLS connections,
// their policy checks, credential revocation, audit and metering alive. UDP
// sessions are recreated by the successor using the same bound socket.
func (s *Server) Drain() error {
	s.draining.Store(true)
	s.closing.Store(true)
	result := errors.Join(s.httpListener.Close(), s.httpsListener.Close())
	s.acceptWG.Wait()
	s.closeUDPSessions()
	result = errors.Join(result, s.udpHTTPSConn.Close())
	if s.udpHTTPConn != s.udpHTTPSConn {
		result = errors.Join(result, s.udpHTTPConn.Close())
	}
	return result
}

func (s *Server) WaitDrained(ctx context.Context) error {
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		if s.activeConnections.Load() == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
