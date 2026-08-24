package rootfswriterauthority

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

type ServerConfig struct {
	Address  string
	CertFile string
	KeyFile  string
	// ClientCAFile is the trust root for node client certificates. The server
	// never supports optional client authentication.
	ClientCAFile string
	Handler      http.Handler
}

type Server struct {
	address string
	config  *tls.Config
	http    *http.Server
	ready   chan struct{}
	readyMu sync.Once
}

func NewServer(config ServerConfig) (*Server, error) {
	if strings.TrimSpace(config.Address) == "" || strings.TrimSpace(config.CertFile) == "" ||
		strings.TrimSpace(config.KeyFile) == "" || strings.TrimSpace(config.ClientCAFile) == "" || config.Handler == nil {
		return nil, fmt.Errorf("writer authority address, TLS certificate, key, client CA, and handler are required")
	}
	certificate, err := tls.LoadX509KeyPair(config.CertFile, config.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("load writer authority TLS identity: %w", err)
	}
	clientCAPEM, err := os.ReadFile(strings.TrimSpace(config.ClientCAFile))
	if err != nil {
		return nil, fmt.Errorf("read writer authority client CA: %w", err)
	}
	clientRoots := x509.NewCertPool()
	if !clientRoots.AppendCertsFromPEM(clientCAPEM) {
		return nil, fmt.Errorf("writer authority client CA contains no certificates")
	}
	return &Server{
		address: strings.TrimSpace(config.Address),
		config: &tls.Config{
			MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{certificate},
			ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: clientRoots,
		},
		http: &http.Server{
			Handler: config.Handler, ReadHeaderTimeout: 2 * time.Second,
			ReadTimeout: 5 * time.Second, WriteTimeout: 5 * time.Second,
			IdleTimeout: 15 * time.Second, MaxHeaderBytes: 16 << 10,
		},
		ready: make(chan struct{}),
	}, nil
}

// Ready closes after the TCP listener has bound successfully. Callers that
// perform destructive reconciliation can wait for this barrier.
func (s *Server) Ready() <-chan struct{} {
	if s == nil {
		return nil
	}
	return s.ready
}

func (s *Server) Start(ctx context.Context) error {
	listener, err := net.Listen("tcp", s.address)
	if err != nil {
		return fmt.Errorf("listen for writer authority: %w", err)
	}
	s.readyMu.Do(func() { close(s.ready) })
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
