package server

import (
	"context"
	"io"
	"sync"

	"github.com/sandbox0-ai/sandbox0/pkg/teamquota"
	"github.com/sandbox0-ai/sandbox0/pkg/teamquota/activeconnections"
)

func permissiveTeamQuotaOptions() []Option {
	return []Option{
		WithActiveConnectionQuota(newFakeActiveConnectionQuota(-1)),
		WithNetworkByteQuota(passthroughNetworkByteQuota{}),
	}
}

type fakeActiveConnectionQuota struct {
	mu       sync.Mutex
	limit    int
	active   int
	max      int
	acquires int
	leases   []*fakeActiveConnectionLease
	err      error
}

func newFakeActiveConnectionQuota(limit int) *fakeActiveConnectionQuota {
	return &fakeActiveConnectionQuota{limit: limit}
}

func (q *fakeActiveConnectionQuota) Acquire(
	_ context.Context,
	teamID string,
) (activeconnections.Lease, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.acquires++
	if q.err != nil {
		return nil, q.err
	}
	if q.limit >= 0 && q.active >= q.limit {
		return nil, &teamquota.ConcurrencyExceededError{
			TeamID: teamID,
			Key:    teamquota.KeyActiveConnectionCount,
			Limit:  int64(q.limit),
			Used:   int64(q.active),
		}
	}
	lease := &fakeActiveConnectionLease{
		owner: q,
		done:  make(chan struct{}),
	}
	q.active++
	if q.active > q.max {
		q.max = q.active
	}
	q.leases = append(q.leases, lease)
	return lease, nil
}

func (*fakeActiveConnectionQuota) Close() error {
	return nil
}

func (q *fakeActiveConnectionQuota) snapshot() (active, max, acquires int) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.active, q.max, q.acquires
}

func (q *fakeActiveConnectionQuota) leaseAt(index int) *fakeActiveConnectionLease {
	q.mu.Lock()
	defer q.mu.Unlock()
	if index < 0 || index >= len(q.leases) {
		return nil
	}
	return q.leases[index]
}

type fakeActiveConnectionLease struct {
	owner *fakeActiveConnectionQuota
	done  chan struct{}

	once sync.Once
	mu   sync.RWMutex
	err  error
}

func (l *fakeActiveConnectionLease) Done() <-chan struct{} {
	return l.done
}

func (l *fakeActiveConnectionLease) Err() error {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.err
}

func (l *fakeActiveConnectionLease) Release(context.Context) error {
	l.finish(nil)
	return nil
}

func (l *fakeActiveConnectionLease) lose(err error) {
	l.finish(err)
}

func (l *fakeActiveConnectionLease) finish(err error) {
	l.once.Do(func() {
		l.mu.Lock()
		l.err = err
		l.mu.Unlock()
		if l.owner != nil {
			l.owner.mu.Lock()
			l.owner.active--
			l.owner.mu.Unlock()
		}
		close(l.done)
	})
}

type passthroughNetworkByteQuota struct{}

func (passthroughNetworkByteQuota) Reader(
	_ context.Context,
	_ string,
	_ teamquota.Key,
	reader io.Reader,
) io.Reader {
	return reader
}

func (passthroughNetworkByteQuota) Writer(
	_ context.Context,
	_ string,
	_ teamquota.Key,
	writer io.Writer,
) io.Writer {
	return writer
}

func (passthroughNetworkByteQuota) Close() error {
	return nil
}

type recordingNetworkByteQuota struct {
	mu     sync.Mutex
	totals map[teamquota.Key]int64
}

func newRecordingNetworkByteQuota() *recordingNetworkByteQuota {
	return &recordingNetworkByteQuota{
		totals: make(map[teamquota.Key]int64),
	}
}

func (q *recordingNetworkByteQuota) Reader(
	_ context.Context,
	_ string,
	key teamquota.Key,
	reader io.Reader,
) io.Reader {
	return &recordingQuotaReader{
		reader: reader,
		record: func(bytes int) {
			q.add(key, bytes)
		},
	}
}

func (q *recordingNetworkByteQuota) Writer(
	_ context.Context,
	_ string,
	key teamquota.Key,
	writer io.Writer,
) io.Writer {
	return &recordingQuotaWriter{
		writer: writer,
		record: func(bytes int) {
			q.add(key, bytes)
		},
	}
}

func (*recordingNetworkByteQuota) Close() error {
	return nil
}

func (q *recordingNetworkByteQuota) add(key teamquota.Key, bytes int) {
	q.mu.Lock()
	q.totals[key] += int64(bytes)
	q.mu.Unlock()
}

func (q *recordingNetworkByteQuota) bytes(key teamquota.Key) int64 {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.totals[key]
}

type recordingQuotaReader struct {
	reader io.Reader
	record func(int)
}

func (r *recordingQuotaReader) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	if n > 0 {
		r.record(n)
	}
	return n, err
}

type recordingQuotaWriter struct {
	writer io.Writer
	record func(int)
}

func (w *recordingQuotaWriter) Write(p []byte) (int, error) {
	n, err := w.writer.Write(p)
	if n > 0 {
		w.record(n)
	}
	return n, err
}
