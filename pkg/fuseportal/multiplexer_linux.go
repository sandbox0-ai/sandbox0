//go:build linux

package fuseportal

import (
	"errors"
	"fmt"
	"sync"

	"golang.org/x/sys/unix"
)

const epollBatchSize = 128

type epollRegistration struct {
	server *Server
	active sync.WaitGroup
}

// epollMultiplexer keeps all idle FUSE channels on one kernel wait queue. The
// request handlers remain concurrent, while the number of blocking OS threads
// no longer grows with the number of mounted portals.
type epollMultiplexer struct {
	fd            int
	mu            sync.Mutex
	nextToken     uint64
	registrations map[uint64]*epollRegistration
	runErr        error
}

var (
	sharedMuxOnce sync.Once
	sharedMux     *epollMultiplexer
	sharedMuxErr  error
)

func sharedEpollMultiplexer() (*epollMultiplexer, error) {
	sharedMuxOnce.Do(func() {
		fd, err := unix.EpollCreate1(unix.EPOLL_CLOEXEC)
		if err != nil {
			sharedMuxErr = fmt.Errorf("create shared FUSE epoll multiplexer: %w", err)
			return
		}
		sharedMux = &epollMultiplexer{
			fd:            fd,
			registrations: make(map[uint64]*epollRegistration),
		}
		go sharedMux.run()
	})
	return sharedMux, sharedMuxErr
}

func (m *epollMultiplexer) add(server *Server) error {
	server.fdMu.Lock()
	fd := server.fd
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
			m.dispatch(events[i])
		}
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
	m.mu.Lock()
	if m.runErr == nil {
		m.runErr = err
	}
	servers := make([]*Server, 0, len(m.registrations))
	for _, registration := range m.registrations {
		servers = append(servers, registration.server)
	}
	m.mu.Unlock()
	for _, server := range servers {
		server.requestStop(err)
	}
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
