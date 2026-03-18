package service

import (
	"context"
	"testing"

	"github.com/sandbox0-ai/sandbox0/pkg/egressauth"
	"go.uber.org/zap"
)

type memorySourceStore struct {
	records map[string]*egressauth.CredentialSourceRecord
}

func newMemorySourceStore() *memorySourceStore {
	return &memorySourceStore{records: make(map[string]*egressauth.CredentialSourceRecord)}
}

func (s *memorySourceStore) ListSourceRecords(_ context.Context, teamID string) ([]egressauth.CredentialSourceRecord, error) {
	out := make([]egressauth.CredentialSourceRecord, 0)
	for key, value := range s.records {
		if len(key) >= len(teamID)+1 && key[:len(teamID)+1] == teamID+"/" {
			out = append(out, *cloneSourceRecord(value))
		}
	}
	return out, nil
}

func (s *memorySourceStore) GetSourceRecord(_ context.Context, teamID, name string) (*egressauth.CredentialSourceRecord, error) {
	return cloneSourceRecord(s.records[teamID+"/"+name]), nil
}

func (s *memorySourceStore) PutSourceRecord(_ context.Context, teamID string, record *egressauth.CredentialSourceRecord) (*egressauth.CredentialSourceRecord, error) {
	cloned := cloneSourceRecord(record)
	if cloned.CurrentVersion == 0 {
		cloned.CurrentVersion = 1
	} else {
		cloned.CurrentVersion++
	}
	s.records[teamID+"/"+record.Name] = cloned
	return cloneSourceRecord(cloned), nil
}

func (s *memorySourceStore) DeleteSourceRecord(_ context.Context, teamID, name string) error {
	delete(s.records, teamID+"/"+name)
	return nil
}

func TestCredentialSourceServicePutSource(t *testing.T) {
	store := newMemorySourceStore()
	svc := NewCredentialSourceService(store, zap.NewNop())

	record, err := svc.PutSource(context.Background(), "team-1", &egressauth.CredentialSourceRecord{
		Name:         "github-api",
		ResolverKind: "static_headers",
		Spec: egressauth.CredentialSourceSpec{
			StaticHeaders: &egressauth.StaticHeadersSourceSpec{
				Values: map[string]string{"token": "abc"},
			},
		},
	})
	if err != nil {
		t.Fatalf("put source: %v", err)
	}
	if record.CurrentVersion != 1 {
		t.Fatalf("current version = %d, want 1", record.CurrentVersion)
	}
}

func cloneSourceRecord(in *egressauth.CredentialSourceRecord) *egressauth.CredentialSourceRecord {
	if in == nil {
		return nil
	}
	cloned := *in
	if in.Spec.StaticHeaders != nil {
		cloned.Spec.StaticHeaders = &egressauth.StaticHeadersSourceSpec{
			Values: cloneSourceValues(in.Spec.StaticHeaders.Values),
		}
	}
	return &cloned
}

func cloneSourceValues(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}
