package process

import (
	"sync"
	"testing"
	"time"
)

// TestMultiplexedChannel_BasicFork tests basic fork functionality.
func TestMultiplexedChannel_BasicFork(t *testing.T) {
	mc := NewMultiplexedChannel[ProcessOutput](10)
	defer mc.Close()

	ch1, cancel1 := mc.Fork()
	defer cancel1()

	ch2, cancel2 := mc.Fork()
	defer cancel2()

	if mc.SubscriberCount() != 2 {
		t.Errorf("SubscriberCount() = %d, want 2", mc.SubscriberCount())
	}

	// Publish an event
	event := ProcessOutput{
		Source: OutputSourceStdout,
		Data:   []byte("test data"),
	}
	mc.Publish(event)

	// Both subscribers should receive the event
	select {
	case <-ch1:
		// OK
	case <-time.After(time.Second):
		t.Error("ch1 did not receive event")
	}

	select {
	case <-ch2:
		// OK
	case <-time.After(time.Second):
		t.Error("ch2 did not receive event")
	}
}

// TestMultiplexedChannel_Unsubscribe tests unsubscribe functionality.
func TestMultiplexedChannel_Unsubscribe(t *testing.T) {
	mc := NewMultiplexedChannel[ProcessOutput](10)
	defer mc.Close()

	ch, cancel := mc.Fork()

	// Initial subscriber count
	if mc.SubscriberCount() != 1 {
		t.Errorf("SubscriberCount() = %d, want 1", mc.SubscriberCount())
	}

	// Unsubscribe
	cancel()

	// Wait for dispatch goroutine to process
	time.Sleep(100 * time.Millisecond)

	// Subscriber count should be 0
	if mc.SubscriberCount() != 0 {
		t.Errorf("SubscriberCount() = %d, want 0", mc.SubscriberCount())
	}

	// Channel should be closed
	select {
	case _, ok := <-ch:
		if ok {
			t.Error("expected channel to be closed")
		}
	case <-time.After(time.Second):
		t.Error("timeout waiting for channel close")
	}
}

// TestMultiplexedChannel_MultipleSubscribers tests many subscribers.
func TestMultiplexedChannel_MultipleSubscribers(t *testing.T) {
	mc := NewMultiplexedChannel[ProcessOutput](10)
	defer mc.Close()

	const numSubscribers = 100
	var subs []<-chan ProcessOutput
	var cancels []func()

	for i := 0; i < numSubscribers; i++ {
		ch, cancel := mc.Fork()
		subs = append(subs, ch)
		cancels = append(cancels, cancel)
	}

	if mc.SubscriberCount() != numSubscribers {
		t.Errorf("SubscriberCount() = %d, want %d", mc.SubscriberCount(), numSubscribers)
	}

	// Publish multiple events
	const numEvents = 10
	for i := 0; i < numEvents; i++ {
		mc.Publish(ProcessOutput{
			Source: OutputSourceStdout,
			Data:   []byte("test"),
		})
	}

	// Verify all subscribers received all events
	var wg sync.WaitGroup
	for _, ch := range subs {
		wg.Add(1)
		go func(c <-chan ProcessOutput) {
			defer wg.Done()
			count := 0
			timeout := time.After(5 * time.Second)
			for {
				select {
				case <-c:
					count++
					if count >= numEvents {
						return
					}
				case <-timeout:
					t.Errorf("subscriber only received %d/%d events", count, numEvents)
					return
				}
			}
		}(ch)
	}

	wg.Wait()

	// Clean up
	for _, cancel := range cancels {
		cancel()
	}
}

// TestMultiplexedChannel_ConcurrentForkUnsubscribe tests concurrent fork/unsubscribe.
func TestMultiplexedChannel_ConcurrentForkUnsubscribe(t *testing.T) {
	mc := NewMultiplexedChannel[ProcessOutput](10)
	defer mc.Close()

	const numGoroutines = 50
	var wg sync.WaitGroup

	// Concurrently fork and unsubscribe
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ch, cancel := mc.Fork()
			time.Sleep(time.Duration(i) * time.Microsecond)
			cancel()
			// Drain channel to allow goroutine to exit
			for range ch {
			}
		}()
	}

	wg.Wait()
}

// TestMultiplexedChannel_BufferFull tests behavior when buffer is full.
func TestMultiplexedChannel_BufferFull(t *testing.T) {
	mc := NewMultiplexedChannel[ProcessOutput](2) // Small buffer
	defer mc.Close()

	_, cancel := mc.Fork()
	defer cancel()

	// Fill the channel buffer
	for i := 0; i < 2; i++ {
		mc.Publish(ProcessOutput{Source: OutputSourceStdout})
	}

	// Publish more events - should be dropped, not block
	// This test verifies the dispatch doesn't block when subscriber buffer is full
	done := make(chan bool)
	go func() {
		for i := 0; i < 10; i++ {
			mc.Publish(ProcessOutput{Source: OutputSourceStdout})
		}
		close(done)
	}()

	select {
	case <-done:
		// OK - didn't block
	case <-time.After(time.Second):
		t.Error("Publish blocked when subscriber buffer was full")
	}
}

// TestMultiplexedChannel_ForkAfterClose tests fork after source is closed.
func TestMultiplexedChannel_ForkAfterClose(t *testing.T) {
	mc := NewMultiplexedChannel[ProcessOutput](10)

	// Close immediately
	mc.Close()

	// Fork after close should return a closed channel
	ch, cancel := mc.Fork()
	defer cancel()

	select {
	case _, ok := <-ch:
		if ok {
			t.Error("expected closed channel")
		}
	case <-time.After(time.Second):
		t.Error("timeout waiting for closed channel")
	}
}

// TestMultiplexedChannel_CloseAllSubscribers tests that all subscribers are closed when source closes.
func TestMultiplexedChannel_CloseAllSubscribers(t *testing.T) {
	mc := NewMultiplexedChannel[ProcessOutput](10)

	const numSubscribers = 10
	var chs []<-chan ProcessOutput
	var cancels []func()

	for i := 0; i < numSubscribers; i++ {
		ch, cancel := mc.Fork()
		chs = append(chs, ch)
		cancels = append(cancels, cancel)
	}

	// Publish an event before closing
	mc.Publish(ProcessOutput{Source: OutputSourceStdout})

	// Close the source
	mc.Close()

	// All subscribers should receive the event and then be closed
	for i, ch := range chs {
		select {
		case _, ok := <-ch:
			if !ok {
				t.Errorf("subscriber %d: expected to receive event before close", i)
			}
		case <-time.After(time.Second):
			t.Errorf("subscriber %d: timeout waiting for event", i)
		}

		// Channel should now be closed
		select {
		case _, ok := <-ch:
			if ok {
				t.Errorf("subscriber %d: expected channel to be closed", i)
			}
		case <-time.After(time.Second):
			t.Errorf("subscriber %d: timeout waiting for close", i)
		}
	}

	// Clean up cancels
	for _, cancel := range cancels {
		cancel()
	}
}

// TestMultiplexedChannel_UnsubscribeNonExistent tests unsubscribing a channel that was never subscribed.
func TestMultiplexedChannel_UnsubscribeNonExistent(t *testing.T) {
	mc := NewMultiplexedChannel[ProcessOutput](10)
	defer mc.Close()

	// Create a channel that was never subscribed
	ch := make(chan ProcessOutput, 10)

	// Unsubscribing should not panic
	mc.Unsubscribe(ch)

	if mc.SubscriberCount() != 0 {
		t.Errorf("SubscriberCount() = %d, want 0", mc.SubscriberCount())
	}
}

// BenchmarkMultiplexedChannel_Publish benchmarks publish performance.
func BenchmarkMultiplexedChannel_Publish(b *testing.B) {
	mc := NewMultiplexedChannel[ProcessOutput](1000)
	defer mc.Close()

	// Add some subscribers
	for i := 0; i < 10; i++ {
		ch, cancel := mc.Fork()
		defer cancel()
		go func(c <-chan ProcessOutput) {
			for range c {
			}
		}(ch)
	}

	event := ProcessOutput{
		Source: OutputSourceStdout,
		Data:   make([]byte, 100),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mc.Publish(event)
	}
}

// BenchmarkMultiplexedChannel_Fork benchmarks fork performance.
func BenchmarkMultiplexedChannel_Fork(b *testing.B) {
	mc := NewMultiplexedChannel[ProcessOutput](100)
	defer mc.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ch, cancel := mc.Fork()
		// Don't defer cancel in benchmark - would overflow
		_ = ch
		_ = cancel
	}
}
