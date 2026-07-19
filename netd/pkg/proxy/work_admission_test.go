package proxy

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"go.uber.org/zap"
)

func TestAcceptLoopRejectsBeforeSpawningWhenTCPAdmissionIsFull(t *testing.T) {
	dropped := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "test_netd_proxy_admission_dropped_total",
		Help: "Test counter.",
	}, []string{"transport", "reason"})
	server := &Server{
		logger:       zap.NewNop(),
		tcpAdmission: make(chan struct{}, 1),
		metrics: &proxyMetricsRegistry{
			proxyAdmissionDropped: dropped,
		},
	}
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	acceptDone := make(chan struct{})
	handlerStarted := make(chan struct{}, 2)
	releaseHandler := make(chan struct{})
	go func() {
		defer close(acceptDone)
		server.acceptLoop(ctx, "http", listener, func(_ context.Context, conn net.Conn) {
			defer conn.Close()
			handlerStarted <- struct{}{}
			<-releaseHandler
		})
	}()

	first, err := net.Dial("tcp4", listener.Addr().String())
	if err != nil {
		t.Fatalf("dial first: %v", err)
	}
	defer first.Close()
	select {
	case <-handlerStarted:
	case <-time.After(time.Second):
		t.Fatal("first handler did not start")
	}

	second, err := net.Dial("tcp4", listener.Addr().String())
	if err != nil {
		t.Fatalf("dial second: %v", err)
	}
	defer second.Close()
	_ = second.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := second.Read(make([]byte, 1)); err == nil {
		t.Fatal("second connection remained open while local admission was full")
	}
	select {
	case <-handlerStarted:
		t.Fatal("second connection spawned a handler")
	default:
	}
	if got := testutil.ToFloat64(
		dropped.WithLabelValues("tcp", "active_limit"),
	); got != 1 {
		t.Fatalf("TCP admission drops = %v, want 1", got)
	}

	close(releaseHandler)
	_ = listener.Close()
	select {
	case <-acceptDone:
	case <-time.After(time.Second):
		t.Fatal("accept loop did not stop")
	}
}

func TestUDPWorkerPoolBoundsOutstandingDatagramsAndConcurrency(t *testing.T) {
	const (
		workers  = 2
		capacity = 3
	)
	server := &Server{
		udpAdmission: make(chan struct{}, capacity),
		udpDatagrams: make(chan udpDatagram, capacity),
	}
	var active atomic.Int32
	var maximum atomic.Int32
	started := make(chan struct{}, capacity)
	release := make(chan struct{})
	server.udpHandler = func(context.Context, udpDatagram) {
		current := active.Add(1)
		defer active.Add(-1)
		for {
			seen := maximum.Load()
			if current <= seen || maximum.CompareAndSwap(seen, current) {
				break
			}
		}
		started <- struct{}{}
		<-release
	}

	ctx, cancel := context.WithCancel(context.Background())
	var workersDone sync.WaitGroup
	workersDone.Add(workers)
	for range workers {
		go func() {
			defer workersDone.Done()
			server.udpWorker(ctx)
		}()
	}

	for index := 0; index < capacity; index++ {
		if !server.tryAcquireUDPAdmission() {
			t.Fatalf("datagram %d rejected before capacity", index)
		}
		server.udpDatagrams <- udpDatagram{payload: []byte{byte(index)}}
	}
	for index := 0; index < workers; index++ {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("UDP worker did not start")
		}
	}
	if server.tryAcquireUDPAdmission() {
		t.Fatal("datagram admitted above outstanding-work capacity")
	}
	if got := maximum.Load(); got != workers {
		t.Fatalf("maximum UDP handler concurrency = %d, want %d", got, workers)
	}

	close(release)
	deadline := time.Now().Add(time.Second)
	for len(server.udpAdmission) != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := len(server.udpAdmission); got != 0 {
		t.Fatalf("outstanding UDP admissions = %d, want 0", got)
	}
	cancel()
	workersDone.Wait()
}
