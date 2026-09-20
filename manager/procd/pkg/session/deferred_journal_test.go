package session

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestStoppedSessionDoesNotReadHistoryDuringActivation(t *testing.T) {
	store, err := NewFileStore(t.TempDir())
	require.NoError(t, err)
	record := Session{ID: "ses-stopped", Phase: PhaseStopped, RuntimeGeneration: 1,
		Spec: normalizeSpec(SessionSpec{Command: []string{"/bin/true"}, Lifecycle: LifecycleSpec{DesiredState: DesiredStateStopped}})}
	require.NoError(t, store.Save(record))
	path, err := store.JournalPath(record.ID)
	require.NoError(t, err)
	j, err := OpenJournal(path, record.Spec.EventRetention, EventCursor{})
	require.NoError(t, err)
	_, err = j.Append(Event{Type: "session.stopped"})
	require.NoError(t, err)
	record.Cursor = j.Cursor()
	require.NoError(t, store.Save(record))
	require.NoError(t, j.Close())
	// A corrupt manifest proves activation never opened the journal. Access must
	// still detect the corruption instead of inventing a fresh empty history.
	manifest := filepath.Join(filepath.Dir(path), "journal-v2", "manifest.json")
	require.NoError(t, os.WriteFile(manifest, []byte("corrupt"), 0600))
	s, err := NewSupervisor(store, zap.NewNop())
	require.NoError(t, err)
	defer s.Close()
	before, err := os.ReadFile(filepath.Join(filepath.Dir(path), "state.json"))
	require.NoError(t, err)
	require.NoError(t, s.Activate(Activation{SandboxID: "sandbox-1", RuntimeGeneration: 2}))
	after, err := os.ReadFile(filepath.Join(filepath.Dir(path), "state.json"))
	require.NoError(t, err)
	require.Equal(t, before, after, "activation must not fsync unchanged stopped state")
	require.Len(t, s.List(), 1)
	d, ok := s.sessions[record.ID].journal.(*deferredJournal)
	require.True(t, ok)
	require.Nil(t, d.journal)
	_, err = d.Read(0, 10)
	require.Error(t, err)
	require.NoError(t, d.Close())
	require.Nil(t, d.journal)
	require.NoError(t, s.Close())
	record.Spec.Lifecycle.DesiredState = DesiredStateRunning
	record.Phase = PhaseSuspended
	require.NoError(t, store.Save(record))
	resumed, err := NewSupervisor(store, zap.NewNop())
	require.NoError(t, err)
	defer resumed.Close()
	require.Error(t, resumed.Activate(Activation{SandboxID: "sandbox-1", RuntimeGeneration: 3}), "running session history must be verified before command readiness")
}

func TestDeferredJournalRecoversOnceAndPreservesSequence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.jsonl")
	retention := normalizeSpec(SessionSpec{}).EventRetention
	j, err := OpenJournal(path, retention, EventCursor{})
	require.NoError(t, err)
	for range 20 {
		_, err = j.Append(Event{Type: "output"})
		require.NoError(t, err)
	}
	cursor := j.Cursor()
	require.NoError(t, j.Close())
	d := &deferredJournal{path: path, retention: retention, cursor: cursor}
	require.Equal(t, cursor, d.Cursor())
	require.NoError(t, d.Flush())
	require.Nil(t, d.journal)
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p, err := d.Read(0, 100)
			require.NoError(t, err)
			require.Len(t, p.Events, 20)
		}()
	}
	wg.Wait()
	next, err := d.Append(Event{Type: "session.started"})
	require.NoError(t, err)
	require.Equal(t, cursor.Latest+1, next.Seq)
	require.NoError(t, d.Prune(time.Now()))
	require.NoError(t, d.Close())
	_, err = d.Append(Event{Type: "output"})
	require.Error(t, err)
}

func TestSessionStateRejectsUnknownFormatBeforeActivation(t *testing.T) {
	store, err := NewFileStore(t.TempDir())
	require.NoError(t, err)
	dir, err := store.sessionDir("ses-future")
	require.NoError(t, err)
	require.NoError(t, os.MkdirAll(dir, 0700))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "state.json"), []byte(`{"id":"ses-future","format_version":99}`), 0600))
	_, err = store.Load()
	require.ErrorContains(t, err, "unsupported state format 99")
}
