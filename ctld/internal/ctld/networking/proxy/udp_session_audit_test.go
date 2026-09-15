package proxy

import (
	"bytes"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/ctld/internal/ctld/networking/policy"
	"github.com/sandbox0-ai/sandbox0/pkg/sandboxobservability"
	v1alpha1 "github.com/sandbox0-ai/sandbox0/pkg/sandboxspec"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type pendingUDPReply struct {
	entered chan struct{}
	closed  chan struct{}
	release chan struct{}
	err     error
}

func (c *pendingUDPReply) Read([]byte) (int, error) {
	<-c.closed
	return 0, net.ErrClosed
}

func (c *pendingUDPReply) Write(payload []byte) (int, error) {
	close(c.entered)
	<-c.release
	if c.err != nil {
		return len(payload) / 2, c.err
	}
	return len(payload), c.err
}

func (c *pendingUDPReply) Close() error {
	close(c.closed)
	return nil
}

func TestUDPCloseWaitsForDeliveredReplyAccounting(t *testing.T) {
	for _, writeErr := range []error{nil, net.ErrClosed} {
		t.Run(errorsLabel(writeErr), func(t *testing.T) {
			var output bytes.Buffer
			server := &Server{logger: zap.NewNop(), auditor: newTestAuditLogger(nopWriteCloser{Writer: &output})}
			compiled := &policy.CompiledPolicy{Mode: v1alpha1.NetworkModeAllowAll}
			req := &adapterRequest{Server: server, Compiled: compiled, SrcIP: "127.0.0.1", DestIP: net.ParseIP("127.0.0.1"), DestPort: 1234}
			session := newUDPSession(server, udpSessionKey{}, req)
			req.Audit = session.Audit()
			decision := decideTraffic(compiled, classifyUnknownTraffic("udp", "udp", req.DestIP, req.DestPort, "missing_sni"))
			session.bindAudit(req, decision, &udpPassThroughAdapter{})
			require.NoError(t, server.recordAuditAttempt(req, decision, &udpPassThroughAdapter{}))
			reply := &pendingUDPReply{entered: make(chan struct{}), closed: make(chan struct{}), release: make(chan struct{}), err: writeErr}
			session.downstream = reply
			writeDone := make(chan error, 1)
			go func() {
				_, err := session.forwardReply([]byte("delivered"), compiled, req.Audit)
				writeDone <- err
			}()
			select {
			case <-reply.entered:
			case <-time.After(time.Second):
				t.Fatal("reply write did not start")
			}
			closed := make(chan struct{})
			go func() { session.close(); close(closed) }()
			select {
			case <-reply.closed:
			case <-time.After(time.Second):
				t.Fatal("session did not interrupt its socket")
			}
			// The client can receive the datagram before Write returns to the
			// accounting code. Closing must retain that in-flight operation.
			select {
			case <-closed:
				t.Fatal("final audit published before delivered bytes were counted")
			default:
			}
			select {
			case <-session.ctx.Done():
			default:
				t.Fatal("session closure did not cancel pending bandwidth waits")
			}
			close(reply.release)
			require.True(t, errors.Is(<-writeDone, writeErr))
			select {
			case <-closed:
			case <-time.After(time.Second):
				t.Fatal("session did not finish after accounting")
			}
			session.close()
			events := decodeAuditEvents(t, output.Bytes())
			require.Len(t, events, 2)
			require.Equal(t, string(sandboxobservability.EventPhaseResult), events[1].Phase)
			wantBytes := int64(len("delivered"))
			if writeErr != nil {
				wantBytes /= 2
			}
			require.Equal(t, wantBytes, events[1].IngressBytes)
			require.False(t, session.beginIO(), "a retired session cannot admit another write")
		})
	}
}

func errorsLabel(err error) string {
	if err != nil {
		return "partial_write_with_error"
	}
	return "successful_write"
}
