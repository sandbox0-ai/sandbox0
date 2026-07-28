//go:build linux

package fuseportal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sys/unix"
)

const (
	epollBatchSize                    = 128
	multiplexerWatchdogInterval       = 5 * time.Second
	multiplexerWatchdogTimeout        = 30 * time.Second
	multiplexerWatchdogControlToken   = uint64(0)
	multiplexerWatchdogEventValueSize = 8
)

type multiplexerConfig struct {
	watchdogInterval time.Duration
	watchdogTimeout  time.Duration
}

type epollRegistration struct {
	server *Server
	active sync.WaitGroup
}

// epollMultiplexer keeps all idle FUSE channels on one kernel wait queue. The
// request handlers remain concurrent, while the number of blocking OS threads
// no longer grows with the number of mounted portals.
type epollMultiplexer struct {
	fd            int
	controlFD     int
	mu            sync.Mutex
	nextToken     uint64
	registrations map[uint64]*epollRegistration
	runErr        error
	probeSequence atomic.Uint64
	probeAck      chan uint64
	stop          chan struct{}
	done          chan struct{}
	closeOnce     sync.Once
}

var (
	sharedMuxOnce sync.Once
	sharedMux     *epollMultiplexer
	sharedMuxErr  error
)

func sharedEpollMultiplexer() (*epollMultiplexer, error) {
	sharedMuxOnce.Do(func() {
		sharedMux, sharedMuxErr = newEpollMultiplexer(multiplexerConfig{
			watchdogInterval: multiplexerWatchdogInterval,
			watchdogTimeout:  multiplexerWatchdogTimeout,
		})
		if sharedMuxErr != nil {
			sharedMuxErr = fmt.Errorf("create shared FUSE epoll multiplexer: %w", sharedMuxErr)
		}
	})
	return sharedMux, sharedMuxErr
}

func newEpollMultiplexer(cfg multiplexerConfig) (*epollMultiplexer, error) {
	if cfg.watchdogInterval <= 0 {
		cfg.watchdogInterval = multiplexerWatchdogInterval
	}
	if cfg.watchdogTimeout <= cfg.watchdogInterval {
		cfg.watchdogTimeout = multiplexerWatchdogTimeout
	}
	fd, err := unix.EpollCreate1(unix.EPOLL_CLOEXEC)
	if err != nil {
		return nil, fmt.Errorf("create epoll descriptor: %w", err)
	}
	controlFD, err := unix.Eventfd(0, unix.EFD_CLOEXEC|unix.EFD_NONBLOCK)
	if err != nil {
		_ = unix.Close(fd)
		return nil, fmt.Errorf("create watchdog event descriptor: %w", err)
	}
	controlEvent := unix.EpollEvent{Events: uint32(unix.EPOLLIN | unix.EPOLLERR | unix.EPOLLHUP)}
	setEpollToken(&controlEvent, multiplexerWatchdogControlToken)
	if err := unix.EpollCtl(fd, unix.EPOLL_CTL_ADD, controlFD, &controlEvent); err != nil {
		_ = unix.Close(controlFD)
		_ = unix.Close(fd)
		return nil, fmt.Errorf("register watchdog event descriptor: %w", err)
	}
	m := &epollMultiplexer{
		fd:            fd,
		controlFD:     controlFD,
		registrations: make(map[uint64]*epollRegistration),
		probeAck:      make(chan uint64, 1),
		stop:          make(chan struct{}),
		done:          make(chan struct{}),
	}
	go m.run()
	go m.watch(cfg.watchdogInterval, cfg.watchdogTimeout)
	return m, nil
}

// SharedMultiplexerHealth reports whether the shared FUSE event loop is still
// making progress. Calling it also initializes the process-wide multiplexer.
func SharedMultiplexerHealth() error {
	mux, err := sharedEpollMultiplexer()
	if err != nil {
		return err
	}
	return mux.health()
}

func (m *epollMultiplexer) add(server *Server) error {
	server.fdMu.Lock()
	fd := server.fd
	if fd >= 0 {
		if err := unix.SetNonblock(fd, true); err != nil {
			server.fdMu.Unlock()
			return fmt.Errorf("set FUSE channel nonblocking: %w", err)
		}
	}
	server.fdMu.Unlock()
	if fd < 0 {
		return fmt.Errorf("FUSE channel is unavailable")
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if m.runErr != nil {
		return m.runErr
	}
	m.nextToken++
	if m.nextToken == 0 {
		m.nextToken++
	}
	token := m.nextToken
	event := unix.EpollEvent{Events: uint32(unix.EPOLLIN | unix.EPOLLERR | unix.EPOLLHUP)}
	setEpollToken(&event, token)
	if err := unix.EpollCtl(m.fd, unix.EPOLL_CTL_ADD, fd, &event); err != nil {
		return fmt.Errorf("register FUSE channel with shared epoll: %w", err)
	}
	m.registrations[token] = &epollRegistration{server: server}
	server.muxToken = token
	return nil
}

func (m *epollMultiplexer) remove(server *Server) {
	if m == nil || server == nil {
		return
	}
	m.mu.Lock()
	token := server.muxToken
	registration := m.registrations[token]
	if registration != nil && registration.server == server {
		delete(m.registrations, token)
		server.muxToken = 0
	}
	m.mu.Unlock()
	if registration == nil || registration.server != server {
		return
	}

	server.fdMu.Lock()
	fd := server.fd
	server.fdMu.Unlock()
	if fd >= 0 {
		_ = unix.EpollCtl(m.fd, unix.EPOLL_CTL_DEL, fd, nil)
	}
	registration.active.Wait()
}

func (m *epollMultiplexer) run() {
	defer close(m.done)
	events := make([]unix.EpollEvent, epollBatchSize)
	for {
		count, err := unix.EpollWait(m.fd, events, -1)
		if errors.Is(err, unix.EINTR) {
			continue
		}
		if err != nil {
			m.fail(fmt.Errorf("wait for shared FUSE epoll events: %w", err))
			return
		}
		for i := 0; i < count; i++ {
			if epollToken(events[i]) == multiplexerWatchdogControlToken {
				if m.handleControlEvent() {
					return
				}
				continue
			}
			m.dispatch(events[i])
		}
	}
}

func (m *epollMultiplexer) handleControlEvent() bool {
	var payload [multiplexerWatchdogEventValueSize]byte
	for {
		_, err := unix.Read(m.controlFD, payload[:])
		if errors.Is(err, unix.EINTR) {
			continue
		}
		if err != nil && !errors.Is(err, unix.EAGAIN) && !errors.Is(err, unix.EWOULDBLOCK) {
			m.markUnhealthy(fmt.Errorf("read shared FUSE watchdog event: %w", err))
		}
		break
	}
	select {
	case <-m.stop:
		return true
	default:
	}
	sequence := m.probeSequence.Load()
	select {
	case m.probeAck <- sequence:
	default:
		select {
		case <-m.probeAck:
		default:
		}
		select {
		case m.probeAck <- sequence:
		default:
		}
	}
	return false
}

func (m *epollMultiplexer) watch(interval, timeout time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-m.stop:
			return
		case <-ticker.C:
		}
		sequence := m.probeSequence.Add(1)
		if err := writeEventFD(m.controlFD); err != nil {
			m.markUnhealthy(fmt.Errorf("signal shared FUSE watchdog: %w", err))
			return
		}
		timer := time.NewTimer(timeout)
		acked := false
		for !acked {
			select {
			case <-m.stop:
				if !timer.Stop() {
					<-timer.C
				}
				return
			case ack := <-m.probeAck:
				acked = ack >= sequence
			case <-timer.C:
				m.markUnhealthy(fmt.Errorf("shared FUSE epoll multiplexer made no progress for %s", timeout))
				return
			}
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
	}
}

func writeEventFD(fd int) error {
	var payload [multiplexerWatchdogEventValueSize]byte
	binary.NativeEndian.PutUint64(payload[:], 1)
	for {
		_, err := unix.Write(fd, payload[:])
		if errors.Is(err, unix.EINTR) {
			continue
		}
		return err
	}
}

func (m *epollMultiplexer) dispatch(event unix.EpollEvent) {
	token := epollToken(event)
	m.mu.Lock()
	registration := m.registrations[token]
	if registration != nil {
		registration.active.Add(1)
	}
	m.mu.Unlock()
	if registration == nil {
		return
	}

	stop, err := registration.server.handleReady(event.Events)
	registration.active.Done()
	if stop {
		registration.server.requestStop(err)
	}
}

func (m *epollMultiplexer) fail(err error) {
	m.markUnhealthy(err)
	m.mu.Lock()
	servers := make([]*Server, 0, len(m.registrations))
	for _, registration := range m.registrations {
		servers = append(servers, registration.server)
	}
	m.mu.Unlock()
	for _, server := range servers {
		server.requestStop(err)
	}
}

func (m *epollMultiplexer) markUnhealthy(err error) {
	if m == nil || err == nil {
		return
	}
	m.mu.Lock()
	if m.runErr == nil {
		m.runErr = err
	}
	m.mu.Unlock()
}

func (m *epollMultiplexer) health() error {
	if m == nil {
		return fmt.Errorf("shared FUSE epoll multiplexer is unavailable")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.runErr
}

func (m *epollMultiplexer) close() {
	if m == nil {
		return
	}
	m.closeOnce.Do(func() {
		close(m.stop)
		_ = writeEventFD(m.controlFD)
		<-m.done
		_ = unix.Close(m.controlFD)
		_ = unix.Close(m.fd)
	})
}

func (m *epollMultiplexer) activeCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.registrations)
}

func setEpollToken(event *unix.EpollEvent, token uint64) {
	event.Fd = int32(uint32(token))
	event.Pad = int32(uint32(token >> 32))
}

func epollToken(event unix.EpollEvent) uint64 {
	return uint64(uint32(event.Fd)) | uint64(uint32(event.Pad))<<32
}
