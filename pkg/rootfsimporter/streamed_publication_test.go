package rootfsimporter

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	"github.com/stretchr/testify/require"
)

func TestStreamedMappingPublicationRecoversThroughExistingJournal(t *testing.T) {
	for _, version := range []int{0, rootfsblock.CompressedFormatVersion} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			data := make([]byte, 128*rootfsblock.LogicalBlockSize)
			for index := range data {
				data[index] = byte(index/rootfsblock.LogicalBlockSize%251 + 1)
			}
			input := &trackedImportReader{Reader: bytes.NewReader(data)}
			journal := &failMappingCompletionJournal{recordingPublicationJournal: newRecordingPublicationJournal(), fail: true}
			objects := &recordingImmutablePublisher{objects: make(map[string][]byte), events: &journal.events}
			publisher := JournaledPublisher{OperationID: "rootfs-stream-replay", Journal: journal, Publisher: objects}
			options := rootfsblock.BuildOptions{FormatVersion: version, DataRangeBytes: rootfsblock.LogicalBlockSize, PackBytes: 4 * rootfsblock.LogicalBlockSize, PageEntries: 2}
			failed, err := rootfsblock.BuildMaterializedGeneration(t.Context(), input, int64(len(data)), publisher, options)
			require.ErrorContains(t, err, "mapping completion unavailable")
			require.Empty(t, failed.Payload)
			require.Less(t, input.readBytes, int64(len(data)))
			require.Equal(t, "prepared", journal.states[journal.failedKey])
			require.Contains(t, objects.objects, journal.failedKey, "the durable object exists before completion acknowledgement")
			result, err := rootfsblock.BuildMaterializedGeneration(t.Context(), bytes.NewReader(data), int64(len(data)), publisher, options)
			require.NoError(t, err)
			require.Len(t, journal.prepared, len(result.References))
			require.Len(t, objects.objects, len(result.References))
			for _, reference := range result.References {
				require.Equal(t, reference, journal.prepared[reference.Key])
				require.Equal(t, "published", journal.states[reference.Key])
			}
			require.GreaterOrEqual(t, journal.prepareCalls[journal.failedKey], 2)
			for index, event := range journal.events {
				if len(event) > 4 && event[:4] == "put:" {
					require.Greater(t, index, 0)
					require.Equal(t, "prepare:"+event[4:], journal.events[index-1])
				}
			}
		})
	}
}

type trackedImportReader struct {
	*bytes.Reader
	readBytes int64
}

func (r *trackedImportReader) ReadAt(p []byte, off int64) (int, error) {
	n, err := r.Reader.ReadAt(p, off)
	r.readBytes += int64(n)
	return n, err
}

type failMappingCompletionJournal struct {
	*recordingPublicationJournal
	fail      bool
	failedKey string
}

func (j *failMappingCompletionJournal) MarkObjectPublished(ctx context.Context, operation string, reference rootfsblock.ObjectReference) error {
	if j.fail && reference.Kind == rootfsblock.ObjectKindMappingPage {
		j.fail = false
		j.failedKey = reference.Key
		return errors.New("mapping completion unavailable")
	}
	return j.recordingPublicationJournal.MarkObjectPublished(ctx, operation, reference)
}
