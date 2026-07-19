package proxy

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"

	"github.com/sandbox0-ai/sandbox0/netd/pkg/policy"
)

// upstreamOnWireConn accounts traffic at the external network boundary. The
// sandbox-to-ctld connection is intentionally excluded because it is an
// internal hop.
type upstreamOnWireConn struct {
	*countingConn
	server    *Server
	compiled  *policy.CompiledPolicy
	audit     *flowAudit
	closeOnce sync.Once
	ioMu      sync.Mutex
	ioWG      sync.WaitGroup
	closing   bool
	closeErr  error
	cancel    context.CancelFunc
}

// wrapUpstreamOnWireConn places quota enforcement below protocol stacks so
// generated handshake, authentication, control, and payload bytes are charged.
func (s *Server) wrapUpstreamOnWireConn(ctx context.Context, conn net.Conn, compiled *policy.CompiledPolicy, audit *flowAudit) *upstreamOnWireConn {
	if conn == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	quotaCtx, cancel := context.WithCancel(ctx)
	var limiter *bandwidthLimiter
	if s != nil {
		limiter = s.bandwidthLimiter
	}
	return &upstreamOnWireConn{
		countingConn: &countingConn{
			Conn:           conn,
			ctx:            quotaCtx,
			limiter:        limiter,
			compiled:       compiled,
			readDirection:  bandwidthIngress,
			writeDirection: bandwidthEgress,
		},
		server:   s,
		compiled: compiled,
		audit:    audit,
		cancel:   cancel,
	}
}

func (c *upstreamOnWireConn) Read(p []byte) (int, error) {
	if !c.beginIO() {
		return 0, net.ErrClosed
	}
	defer c.ioWG.Done()
	return c.countingConn.Read(p)
}

func (c *upstreamOnWireConn) Write(p []byte) (int, error) {
	if !c.beginIO() {
		return 0, net.ErrClosed
	}
	defer c.ioWG.Done()
	return c.countingConn.Write(p)
}

func (c *upstreamOnWireConn) Close() error {
	if c == nil || c.countingConn == nil {
		return nil
	}
	c.closeOnce.Do(func() {
		c.ioMu.Lock()
		c.closing = true
		c.ioMu.Unlock()
		c.cancel()
		c.closeErr = c.countingConn.Close()
		c.ioWG.Wait()
		if c.server != nil {
			c.server.recordEgressBytes(c.compiled, c.WrittenBytes(), c.audit)
			c.server.recordIngressBytes(c.compiled, c.ReadBytes(), c.audit)
		}
	})
	return c.closeErr
}

func (c *upstreamOnWireConn) beginIO() bool {
	if c == nil || c.countingConn == nil {
		return false
	}
	c.ioMu.Lock()
	defer c.ioMu.Unlock()
	if c.closing {
		return false
	}
	c.ioWG.Add(1)
	return true
}

// pipeTerminatedProtocol relays decrypted payload without charging it again.
func pipeTerminatedProtocol(client net.Conn, upstream net.Conn, clientReader io.Reader) error {
	errCh := make(chan error, 2)
	go func() {
		_, err := io.Copy(upstream, clientReader)
		closeConnWrite(upstream)
		errCh <- err
	}()
	go func() {
		_, err := io.Copy(client, upstream)
		closeConnWrite(client)
		errCh <- err
	}()
	errs := make([]error, 0, 2)
	for range 2 {
		if err := normalizeRelayError(<-errCh); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
