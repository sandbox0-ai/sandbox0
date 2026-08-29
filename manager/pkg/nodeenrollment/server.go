package nodeenrollment

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"
)

type Server struct {
	address  string
	certFile string
	keyFile  string
	http     *http.Server
	ready    chan struct{}
	once     sync.Once
}

func NewServer(address, certFile, keyFile string, handler http.Handler) (*Server, error) {
	if address == "" || certFile == "" || keyFile == "" || handler == nil {
		return nil, errors.New("node enrollment TLS server config is incomplete")
	}
	return &Server{address: address, certFile: certFile, keyFile: keyFile, ready: make(chan struct{}),
		http: &http.Server{Handler: handler, ReadHeaderTimeout: 5 * time.Second}}, nil
}

func (s *Server) Ready() <-chan struct{} { return s.ready }

func (s *Server) Run(ctx context.Context) error {
	certificate, err := tls.LoadX509KeyPair(s.certFile, s.keyFile)
	if err != nil {
		return err
	}
	listener, err := net.Listen("tcp", s.address)
	if err != nil {
		return err
	}
	s.once.Do(func() { close(s.ready) })
	tlsListener := tls.NewListener(listener, &tls.Config{
		MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{certificate},
	})
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = s.http.Shutdown(shutdown)
	}()
	err = s.http.Serve(tlsListener)
	if errors.Is(err, http.ErrServerClosed) && ctx.Err() != nil {
		return ctx.Err()
	}
	return fmt.Errorf("serve node enrollment: %w", err)
}
