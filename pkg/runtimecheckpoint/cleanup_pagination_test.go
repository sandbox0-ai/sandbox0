package runtimecheckpoint

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/sandbox0-ai/sandbox0/pkg/objectstore"
	"github.com/stretchr/testify/require"
)

// OSS can count deleted versions toward its scan budget, returning no current
// objects with IsTruncated=true. A continuation page, not an ownership bypass,
// is required to establish whether a capture scope is actually empty.
func TestCheckpointCollectorEmptyVersionedPages(t *testing.T) {
	for _, retained := range []bool{false, true} {
		t.Run(fmt.Sprint(retained), func(t *testing.T) {
			binding := testBinding()
			scope, err := captureScopeForBinding(binding)
			require.NoError(t, err)
			capture, err := capturePrefix(scope)
			require.NoError(t, err)
			var cursors []string
			deletes := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodDelete {
					deletes++
					w.WriteHeader(http.StatusNoContent)
					return
				}
				if r.URL.Query().Get("list-type") != "2" {
					w.WriteHeader(http.StatusNotFound)
					_, _ = fmt.Fprint(w, `<Error><Code>NoSuchKey</Code></Error>`)
					return
				}
				w.Header().Set("Content-Type", "application/xml")
				if r.URL.Query().Get("prefix") != capture {
					_, _ = fmt.Fprint(w, `<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>`)
					return
				}
				cursor := r.URL.Query().Get("continuation-token")
				cursors = append(cursors, cursor)
				switch cursor {
				case "":
					_, _ = fmt.Fprint(w, `<ListBucketResult><IsTruncated>true</IsTruncated><NextContinuationToken>first</NextContinuationToken></ListBucketResult>`)
				case "first":
					_, _ = fmt.Fprint(w, `<ListBucketResult><IsTruncated>true</IsTruncated><NextContinuationToken>second</NextContinuationToken></ListBucketResult>`)
				case "second":
					if retained {
						_, _ = fmt.Fprintf(w, `<ListBucketResult><IsTruncated>false</IsTruncated><Contents><Key>%sreservation.json</Key><Size>1</Size></Contents></ListBucketResult>`, capture)
					} else {
						_, _ = fmt.Fprint(w, `<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>`)
					}
				default:
					t.Errorf("unexpected cursor %q", cursor)
					w.WriteHeader(http.StatusBadRequest)
				}
			}))
			defer server.Close()
			objects, err := objectstore.Create(objectstore.Config{Type: objectstore.TypeS3, Bucket: "checkpoint-pages", Region: "us-east-1", Endpoint: server.URL, AccessKey: "isolated-test", SecretKey: "isolated-test"})
			require.NoError(t, err)
			collector, err := NewCollector(objects)
			require.NoError(t, err)
			for i := range 3 {
				before := len(cursors)
				done, err := collector.Collect(t.Context(), binding)
				require.LessOrEqual(t, len(cursors)-before, 1, "each pass must stay bounded")
				if i == 2 && retained {
					require.ErrorContains(t, err, "terminal capture authority")
					require.False(t, done)
				} else {
					require.NoError(t, err)
					require.Equal(t, i == 2, done)
				}
			}
			require.Equal(t, []string{"", "first", "second"}, cursors)
			require.Zero(t, deletes, "pagination must never delete an unbound capture")
		})
	}
}

func TestCaptureCollectorEmptyPagesRestartAndDeletion(t *testing.T) {
	raw := objectstore.NewMemoryStore("")
	scope, err := captureScopeForBinding(testBinding())
	require.NoError(t, err)
	prefix, err := capturePrefix(scope)
	require.NoError(t, err)
	require.NoError(t, raw.Put(prefix+"reservation.json", strings.NewReader("reservation")))
	var cursors []string
	deleted := false
	fault := &cleanupFaultStore{ContextCleanupStore: raw.(objectstore.ContextCleanupStore)}
	fault.listPage = func(ctx context.Context, p, token string, limit int64) ([]objectstore.Info, bool, string, error) {
		cursors = append(cursors, token)
		if deleted {
			require.Empty(t, token, "a deletion must reset the traversal")
			return fault.ContextCleanupStore.ListContext(ctx, p, "", "", "", limit)
		}
		if token == "" {
			return nil, true, "past-delete-markers", nil
		}
		require.Equal(t, "past-delete-markers", token)
		deleted = true
		return fault.ContextCleanupStore.ListContext(ctx, p, "", "", "", limit)
	}
	collector, err := NewCollector(fault)
	require.NoError(t, err)
	done, err := collector.CollectCapture(t.Context(), scope)
	require.NoError(t, err)
	require.False(t, done)
	collector, err = NewCollector(fault) // A worker restart safely repeats the empty page.
	require.NoError(t, err)
	for range 2 {
		done, err = collector.CollectCapture(t.Context(), scope)
		require.NoError(t, err)
		require.False(t, done)
	}
	done, err = collector.CollectCapture(t.Context(), scope)
	require.NoError(t, err)
	require.True(t, done)
	require.Equal(t, []string{"", "", "past-delete-markers", ""}, cursors)
	require.Equal(t, 1, fault.deletes)
}

func TestCheckpointCollectorRejectsNonadvancingEmptyPages(t *testing.T) {
	for _, next := range []string{"", "same-token"} {
		t.Run(next, func(t *testing.T) {
			raw := objectstore.NewMemoryStore("")
			fault := &cleanupFaultStore{ContextCleanupStore: raw.(objectstore.ContextCleanupStore)}
			fault.listPage = func(context.Context, string, string, int64) ([]objectstore.Info, bool, string, error) {
				return nil, true, next, nil
			}
			collector, err := NewCollector(fault)
			require.NoError(t, err)
			if next != "" {
				done, err := collector.Collect(t.Context(), testBinding())
				require.NoError(t, err)
				require.False(t, done)
			}
			done, err := collector.Collect(t.Context(), testBinding())
			require.ErrorContains(t, err, "advancing continuation")
			require.False(t, done)
			require.Zero(t, fault.deletes)
		})
	}
}
