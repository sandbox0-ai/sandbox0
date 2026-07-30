package runtimewatch

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
)

func TestRuntimeWatchRejectsSourceOutsidePodNetworkIdentity(t *testing.T) {
	pod := testPod()
	pod.Status.PodIP = "10.0.0.5"
	hub := NewHub(nil)
	hub.UpdatePod(pod)
	server := NewServer(hub)

	request := httptest.NewRequest(
		http.MethodGet,
		"/api/v1/runtime/watch?namespace="+pod.Namespace+"&name="+pod.Name+"&uid="+string(pod.UID),
		nil,
	)
	request.RemoteAddr = "10.0.0.6:4321"
	recorder := httptest.NewRecorder()

	server.WatchRuntime(recorder, request)

	if recorder.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusForbidden)
	}
}

func TestRuntimeWatchServerDoesNotExposeCtldControlPaths(t *testing.T) {
	server := NewServer(NewHub(nil))
	for _, path := range []string{"/readyz", "/api/v1/rootfs/save", "/api/v1/volume-portals/bind"} {
		request := httptest.NewRequest(http.MethodPost, path, nil)
		recorder := httptest.NewRecorder()

		server.ServeHTTP(recorder, request)

		if recorder.Code != http.StatusNotFound {
			t.Fatalf("%s status = %d, want %d", path, recorder.Code, http.StatusNotFound)
		}
	}
}

func TestRequestMatchesPodIP(t *testing.T) {
	tests := []struct {
		name       string
		remoteAddr string
		podIP      string
		want       bool
	}{
		{name: "ipv4", remoteAddr: "10.0.0.5:4321", podIP: "10.0.0.5", want: true},
		{name: "ipv4 mapped", remoteAddr: "[::ffff:10.0.0.5]:4321", podIP: "10.0.0.5", want: true},
		{name: "ipv6", remoteAddr: "[2001:db8::5]:4321", podIP: "2001:db8::5", want: true},
		{name: "mismatch", remoteAddr: "10.0.0.6:4321", podIP: "10.0.0.5"},
		{name: "missing pod ip", remoteAddr: "10.0.0.5:4321"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodGet, "/", nil)
			request.RemoteAddr = tt.remoteAddr
			if got := requestMatchesPodIP(request, tt.podIP); got != tt.want {
				t.Fatalf("requestMatchesPodIP() = %t, want %t", got, tt.want)
			}
		})
	}
}

func TestRuntimeWatchClosesWhenPrimaryHubStops(t *testing.T) {
	sink := &recordingSink{}
	hub := NewHub(sink)
	pod := testPod()
	hub.UpdatePod(pod)

	server := httptest.NewServer(NewServer(hub))
	defer server.Close()

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	go func() {
		hub.Run(ctx, 1)
		close(runDone)
	}()

	endpoint := "ws" + strings.TrimPrefix(server.URL, "http") +
		"/api/v1/runtime/watch?namespace=" + pod.Namespace +
		"&name=" + pod.Name +
		"&uid=" + string(pod.UID)
	conn, _, err := websocket.DefaultDialer.Dial(endpoint, nil)
	if err != nil {
		cancel()
		<-runDone
		t.Fatalf("Dial() error = %v", err)
	}
	defer conn.Close()

	var snapshot runtimecontrol.Snapshot
	if err := conn.ReadJSON(&snapshot); err != nil {
		cancel()
		<-runDone
		t.Fatalf("ReadJSON() initial snapshot error = %v", err)
	}

	cancel()
	<-runDone
	if err := conn.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := conn.ReadJSON(&snapshot); err == nil {
		t.Fatal("runtime watch remained open after hub stopped")
	}

	deadline := time.Now().Add(time.Second)
	for {
		sink.mu.Lock()
		disconnections := sink.disconnections
		sink.mu.Unlock()
		if disconnections >= 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("disconnect calls = %d, want subscription disconnect after hub stop", disconnections)
		}
		time.Sleep(time.Millisecond)
	}
}
