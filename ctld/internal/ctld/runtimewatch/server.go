package runtimewatch

import (
	"errors"
	"net"
	"net/http"
	"strings"

	"github.com/gorilla/websocket"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
)

const maxObservationBytes = 64 << 10

type Server struct {
	hub      *Hub
	upgrader websocket.Upgrader
}

func NewServer(hub *Hub) *Server {
	return &Server{
		hub: hub,
		upgrader: websocket.Upgrader{
			CheckOrigin: func(*http.Request) bool { return true },
		},
	}
}

// ServeHTTP intentionally exposes only the runtime watch path. It runs on a
// dedicated node-local port so the network bypass cannot reach CTLD control
// APIs.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r == nil || r.URL == nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	if r.URL.Path != runtimecontrol.WatchPath {
		http.NotFound(w, r)
		return
	}
	s.WatchRuntime(w, r)
}

func (s *Server) WatchRuntime(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	namespace := strings.TrimSpace(r.URL.Query().Get("namespace"))
	name := strings.TrimSpace(r.URL.Query().Get("name"))
	uid := strings.TrimSpace(r.URL.Query().Get("uid"))
	if namespace == "" || name == "" || uid == "" {
		http.Error(w, "namespace, name and uid are required", http.StatusBadRequest)
		return
	}
	pod, err := s.hub.Resolve(namespace, name, uid)
	if err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, ErrPodNotFound) {
			status = http.StatusNotFound
		}
		http.Error(w, err.Error(), status)
		return
	}
	if !requestMatchesPodIP(r, pod.Status.PodIP) {
		http.Error(w, "runtime watch source does not match pod", http.StatusForbidden)
		return
	}

	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer conn.Close()
	conn.SetReadLimit(maxObservationBytes)

	subscriberID, updates, unsubscribe, err := s.hub.Subscribe(namespace, name, uid)
	if err != nil {
		_ = conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.ClosePolicyViolation, err.Error()), deadlineNow())
		return
	}
	defer unsubscribe()

	readErrors := make(chan error, 1)
	go func() {
		for {
			var observation runtimecontrol.Observation
			if err := conn.ReadJSON(&observation); err != nil {
				readErrors <- err
				return
			}
			if err := s.hub.Observe(r.Context(), uid, subscriberID, observation); err != nil {
				readErrors <- err
				return
			}
		}
	}()

	for {
		select {
		case <-r.Context().Done():
			return
		case <-s.hub.Done():
			return
		case <-readErrors:
			return
		case snapshot, ok := <-updates:
			if !ok {
				return
			}
			if err := conn.WriteJSON(snapshot); err != nil {
				return
			}
		}
	}
}

func requestMatchesPodIP(r *http.Request, podIP string) bool {
	podIP = strings.TrimSpace(podIP)
	if podIP == "" {
		return false
	}
	host, _, err := net.SplitHostPort(strings.TrimSpace(r.RemoteAddr))
	if err != nil {
		host = strings.TrimSpace(r.RemoteAddr)
	}
	remoteIP := net.ParseIP(strings.TrimSpace(host))
	expectedIP := net.ParseIP(podIP)
	return remoteIP != nil && expectedIP != nil && remoteIP.Equal(expectedIP)
}
