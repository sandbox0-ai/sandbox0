package runtimecontroller

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
	"go.uber.org/zap"
)

func TestClientIdleConnectionDoesNotPoll(t *testing.T) {
	var connections atomic.Int32
	observed := make(chan runtimecontrol.Observation, 1)
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		connections.Add(1)
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		if err := conn.WriteJSON(runtimecontrol.Snapshot{State: runtimecontrol.DesiredStandby}); err != nil {
			return
		}
		var observation runtimecontrol.Observation
		if err := conn.ReadJSON(&observation); err != nil {
			return
		}
		observed <- observation
		var next runtimecontrol.Observation
		_ = conn.ReadJSON(&next)
	}))
	defer server.Close()

	host, portRaw, err := net.SplitHostPort(server.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	port, err := strconv.Atoi(portRaw)
	if err != nil {
		t.Fatal(err)
	}
	controller := New(nil, nil, nil, nil, 49983, zap.NewNop())
	client, err := NewClient(Identity{
		Namespace: "sandbox-system",
		PodName:   "sandbox-pod",
		PodUID:    "pod-uid",
		NodeHost:  host,
		WatchPort: port,
	}, controller, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		client.Run(ctx)
	}()

	select {
	case observation := <-observed:
		if observation.State != runtimecontrol.ObservedStandby {
			t.Fatalf("observation = %#v", observation)
		}
	case <-time.After(time.Second):
		t.Fatal("runtime observation was not received")
	}
	time.Sleep(100 * time.Millisecond)
	if got := connections.Load(); got != 1 {
		t.Fatalf("idle connection count = %d, want 1", got)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Client.Run() did not stop after cancellation")
	}
}

func TestClientReconnectsAndReplaysCurrentAssignment(t *testing.T) {
	assignment := runtimecontrol.Assignment{
		SandboxID:         "sandbox-1",
		RuntimeGeneration: 3,
	}
	revision, err := assignment.Revision()
	if err != nil {
		t.Fatal(err)
	}
	snapshot := runtimecontrol.Snapshot{
		State:      runtimecontrol.DesiredActive,
		Revision:   revision,
		Assignment: &assignment,
	}

	var connections atomic.Int32
	readyConnections := make(chan int32, 2)
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		connectionNumber := connections.Add(1)
		conn, upgradeErr := upgrader.Upgrade(w, r, nil)
		if upgradeErr != nil {
			return
		}
		defer conn.Close()
		if writeErr := conn.WriteJSON(snapshot); writeErr != nil {
			return
		}
		for {
			var observation runtimecontrol.Observation
			if readErr := conn.ReadJSON(&observation); readErr != nil {
				return
			}
			if observation.State == runtimecontrol.ObservedReady {
				readyConnections <- connectionNumber
				break
			}
		}
		if connectionNumber == 1 {
			return
		}
		var next runtimecontrol.Observation
		_ = conn.ReadJSON(&next)
	}))
	defer server.Close()

	host, portRaw, err := net.SplitHostPort(server.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	port, err := strconv.Atoi(portRaw)
	if err != nil {
		t.Fatal(err)
	}
	controller := New(nil, nil, nil, nil, 49983, zap.NewNop())
	client, err := NewClient(Identity{
		Namespace: "sandbox-system",
		PodName:   "sandbox-pod",
		PodUID:    "pod-uid",
		NodeHost:  host,
		WatchPort: port,
	}, controller, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		client.Run(ctx)
	}()

	for want := int32(1); want <= 2; want++ {
		select {
		case got := <-readyConnections:
			if got != want {
				t.Fatalf("ready connection = %d, want %d", got, want)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("connection %d did not recover the current assignment", want)
		}
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Client.Run() did not stop after cancellation")
	}
}
