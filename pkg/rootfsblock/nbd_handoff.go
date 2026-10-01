package rootfsblock

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"time"
)

// ErrNBDHandoff is a planned transfer, not a transport failure. The caller
// must keep a duplicate of the connected socket alive until its successor
// adopts it; neither side may disconnect the kernel device during transfer.
var ErrNBDHandoff = errors.New("NBD transmission ownership transferred")

// NBDTransmissionControl pauses only at a request boundary after all accepted
// operations and replies complete. It never discards a partial request.
type NBDTransmissionControl struct {
	mu         sync.Mutex
	connection net.Conn
	requested  bool
	detached   bool
	headerRead bool
	paused     chan struct{}
	resume     chan struct{}
	bound      chan struct{}
	stopped    chan struct{}
	ended      bool
}

func (c *NBDTransmissionControl) initialize() {
	if c.bound == nil {
		c.bound, c.stopped = make(chan struct{}), make(chan struct{})
	}
}

func (c *NBDTransmissionControl) bind(connection net.Conn) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.initialize()
	if c.connection != nil {
		return fmt.Errorf("NBD handoff control already bound")
	}
	c.connection = connection
	close(c.bound)
	return nil
}

func (c *NBDTransmissionControl) stop() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.initialize()
	if !c.ended {
		c.ended = true
		close(c.stopped)
	}
}

// Pause leaves the source fully usable when preparation times out. A blocked
// partial header or write body must finish before a transferable boundary.
func (c *NBDTransmissionControl) Pause(ctx context.Context) error {
	c.mu.Lock()
	c.initialize()
	bound, stopped := c.bound, c.stopped
	c.mu.Unlock()
	select {
	case <-bound:
	case <-stopped:
		return fmt.Errorf("NBD transmission ended before handoff")
	case <-ctx.Done():
		return ctx.Err()
	}
	c.mu.Lock()
	if c.ended || c.requested || c.detached {
		c.mu.Unlock()
		return fmt.Errorf("NBD transmission cannot prepare another handoff")
	}
	c.requested = true
	c.paused, c.resume = make(chan struct{}), make(chan struct{})
	paused := c.paused
	if c.headerRead {
		_ = c.connection.SetReadDeadline(time.Now())
	}
	c.mu.Unlock()
	select {
	case <-paused:
		return nil
	case <-ctx.Done():
		c.Resume()
		return ctx.Err()
	case <-stopped:
		c.Resume()
		return fmt.Errorf("NBD transmission ended during handoff")
	}
}

func (c *NBDTransmissionControl) Resume() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.requested || c.detached {
		return
	}
	c.requested = false
	_ = c.connection.SetReadDeadline(time.Time{})
	close(c.resume)
}

// File exports a duplicate only after the stream parser and reply writers
// have quiesced. Owning this descriptor does not authorize backend writes.
func (c *NBDTransmissionControl) File() (*os.File, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.requested || c.detached || c.ended {
		return nil, fmt.Errorf("NBD transmission is not prepared")
	}
	select {
	case <-c.paused:
	default:
		return nil, fmt.Errorf("NBD requests have not drained")
	}
	connection, ok := c.connection.(interface{ File() (*os.File, error) })
	if !ok {
		return nil, fmt.Errorf("NBD connection cannot export descriptors")
	}
	return connection.File()
}

func (c *NBDTransmissionControl) Detach() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.requested || c.detached || c.ended {
		return fmt.Errorf("NBD transmission is not prepared")
	}
	select {
	case <-c.paused:
	default:
		return fmt.Errorf("NBD requests have not drained")
	}
	c.detached = true
	close(c.resume)
	return nil
}

func (c *NBDTransmissionControl) boundary(ctx context.Context, drain func()) error {
	c.mu.Lock()
	if !c.requested {
		c.mu.Unlock()
		return nil
	}
	paused, resume := c.paused, c.resume
	c.mu.Unlock()
	drain()
	c.mu.Lock()
	if !c.requested || c.paused != paused {
		c.mu.Unlock()
		return nil
	}
	close(paused)
	c.mu.Unlock()
	select {
	case <-resume:
	case <-ctx.Done():
		return ctx.Err()
	}
	c.mu.Lock()
	detached := c.detached
	c.mu.Unlock()
	if detached {
		return ErrNBDHandoff
	}
	return nil
}

// readHeader remembers consumed bytes while a pause wakes an idle parser.
// Partial headers are completed by the old owner, never handed off implicitly.
func (c *NBDTransmissionControl) readHeader(ctx context.Context, header []byte) error {
	read := 0
	for read < len(header) {
		c.mu.Lock()
		c.headerRead = true
		requested := c.requested
		if requested && read == 0 {
			_ = c.connection.SetReadDeadline(time.Now())
		}
		c.mu.Unlock()
		n, err := c.connection.Read(header[read:])
		read += n
		c.mu.Lock()
		c.headerRead = false
		requested = c.requested
		_ = c.connection.SetReadDeadline(time.Time{})
		c.mu.Unlock()
		if timeout, ok := err.(net.Error); ok && timeout.Timeout() {
			if requested && read == 0 {
				return errNBDPauseBoundary
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
			continue
		}
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrNoProgress
		}
	}
	return nil
}

var errNBDPauseBoundary = errors.New("NBD idle parser reached handoff boundary")

type nbdHandoffHeaderReader struct {
	ctx     context.Context
	control *NBDTransmissionControl
}

func (r nbdHandoffHeaderReader) Read(header []byte) (int, error) {
	if err := r.control.readHeader(r.ctx, header); err != nil {
		return 0, err
	}
	return len(header), nil
}
