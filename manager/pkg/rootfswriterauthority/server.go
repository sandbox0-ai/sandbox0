package rootfswriterauthority

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"
)

type ServerConfig struct {
	Address  string
	CertFile string
	KeyFile  string
	Handler  http.Handler
}

type Server struct {
	address string
	config  *tls.Config
	http    *http.Server
}

func NewServer(config ServerConfig) (*Server, error) {
	if strings.TrimSpace(config.Address) == "" || strings.TrimSpace(config.CertFile) == "" ||
		strings.TrimSpace(config.KeyFile) == "" || config.Handler == nil {
		return nil, fmt.Errorf("writer authority address, TLS certificate, key, and handler are required")
	}
	certificate, err := tls.LoadX509KeyPair(config.CertFile, config.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("load writer authority TLS identity: %w", err)
	}
	return &Server{
		address: strings.TrimSpace(config.Address),
		config:  &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{certificate}},
		http: &http.Server{
			Handler: config.Handler, ReadHeaderTimeout: 2 * time.Second,
			ReadTimeout: 5 * time.Second, WriteTimeout: 5 * time.Second,
			IdleTimeout: 15 * time.Second, MaxHeaderBytes: 16 << 10,
		},
	}, nil
}

func (s *Server) Start(ctx context.Context) error {
	listener, err := net.Listen("tcp", s.address)
	if err != nil {
		return fmt.Errorf("listen for writer authority: %w", err)
	}
	tlsListener := tls.NewListener(listener, s.config)
	errorsCh := make(chan error, 1)
	go func() { errorsCh <- s.http.Serve(tlsListener) }()
	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return s.http.Shutdown(shutdownCtx)
	case err := <-errorsCh:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}
