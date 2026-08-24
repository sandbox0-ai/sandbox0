// Copyright 2026 Sandbox0 Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package rootfsimporter

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/opencontainers/go-digest"
	"github.com/stretchr/testify/require"

	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
)

func TestJournaledPublisherPersistsIntentBeforePutAndCompletion(t *testing.T) {
	journal := newRecordingPublicationJournal()
	publisher := &recordingImmutablePublisher{objects: make(map[string][]byte), events: &journal.events}
	payload := []byte("base-object")
	key := publicationTestKey("packs", payload)
	err := (JournaledPublisher{
		OperationID: "rootfs-import-operation-1", Journal: journal, Publisher: publisher,
	}).PutImmutable(t.Context(), key, payload)
	require.NoError(t, err)
	require.Equal(t, []string{"prepare:" + key, "put:" + key, "published:" + key}, journal.events)
	require.Equal(t, payload, publisher.objects[key])
	require.Equal(t, "published", journal.states[key])
}

func TestJournaledPublisherReplaysAfterPutAndCompletionFailures(t *testing.T) {
	for _, stage := range []string{"put", "published"} {
		t.Run(stage, func(t *testing.T) {
			journal := newRecordingPublicationJournal()
			publisher := &recordingImmutablePublisher{objects: make(map[string][]byte), events: &journal.events}
			payload := []byte("retry-object-" + stage)
			key := publicationTestKey("maps", payload)
			wrapped := JournaledPublisher{OperationID: "rootfs-import-retry", Journal: journal, Publisher: publisher}
			if stage == "put" {
				publisher.failOnce = true
			} else {
				journal.failPublishedOnce = true
			}
			require.Error(t, wrapped.PutImmutable(t.Context(), key, payload))
			require.NoError(t, wrapped.PutImmutable(t.Context(), key, payload))
			require.Equal(t, payload, publisher.objects[key])
			require.Equal(t, "published", journal.states[key])
			require.Equal(t, 2, journal.prepareCalls[key])
		})
	}
}

func TestJournaledPublisherRejectsJournalConflictBeforePut(t *testing.T) {
	journal := newRecordingPublicationJournal()
	publisher := &recordingImmutablePublisher{objects: make(map[string][]byte), events: &journal.events}
	operationID := "rootfs-import-conflict"
	firstPayload := []byte("first")
	firstKey := publicationTestKey("packs", firstPayload)
	first, err := publicationReference(firstKey, firstPayload)
	require.NoError(t, err)
	require.NoError(t, journal.PrepareObject(t.Context(), operationID, first))
	conflict := first
	conflict.Size++
	journal.prepared[firstKey] = conflict

	err = (JournaledPublisher{OperationID: operationID, Journal: journal, Publisher: publisher}).
		PutImmutable(t.Context(), firstKey, firstPayload)
	require.ErrorContains(t, err, "conflict")
	require.Empty(t, publisher.objects)
}

func TestJournaledPublisherRejectsUnboundKeyAndOperation(t *testing.T) {
	journal := newRecordingPublicationJournal()
	publisher := &recordingImmutablePublisher{objects: make(map[string][]byte), events: &journal.events}
	payload := []byte("payload")
	for name, tc := range map[string]struct {
		operationID string
		key         string
	}{
		"operation": {operationID: "bad/operation", key: publicationTestKey("packs", payload)},
		"key":       {operationID: "valid-operation", key: "rootfs/packs/sha256/not-the-payload"},
	} {
		t.Run(name, func(t *testing.T) {
			err := (JournaledPublisher{OperationID: tc.operationID, Journal: journal, Publisher: publisher}).
				PutImmutable(t.Context(), tc.key, payload)
			require.Error(t, err)
		})
	}
	require.Empty(t, journal.prepared)
	require.Empty(t, publisher.objects)
}

func publicationTestKey(kind string, payload []byte) string {
	return "rootfs/import-test/" + kind + "/sha256/" + digest.FromBytes(payload).Encoded()
}

type recordingPublicationJournal struct {
	prepared          map[string]rootfsblock.ObjectReference
	states            map[string]string
	prepareCalls      map[string]int
	events            []string
	failPublishedOnce bool
}

func newRecordingPublicationJournal() *recordingPublicationJournal {
	return &recordingPublicationJournal{
		prepared: make(map[string]rootfsblock.ObjectReference),
		states:   make(map[string]string), prepareCalls: make(map[string]int),
	}
}

func (j *recordingPublicationJournal) PrepareObject(
	_ context.Context,
	_ string,
	reference rootfsblock.ObjectReference,
) error {
	j.events = append(j.events, "prepare:"+reference.Key)
	j.prepareCalls[reference.Key]++
	if current, found := j.prepared[reference.Key]; found && current != reference {
		return fmt.Errorf("journal object conflict")
	}
	j.prepared[reference.Key] = reference
	if j.states[reference.Key] == "" {
		j.states[reference.Key] = "prepared"
	}
	return nil
}

func (j *recordingPublicationJournal) MarkObjectPublished(
	_ context.Context,
	_ string,
	reference rootfsblock.ObjectReference,
) error {
	j.events = append(j.events, "published:"+reference.Key)
	if j.failPublishedOnce {
		j.failPublishedOnce = false
		return errors.New("publication completion unavailable")
	}
	if current, found := j.prepared[reference.Key]; !found || current != reference {
		return fmt.Errorf("object was not prepared exactly")
	}
	j.states[reference.Key] = "published"
	return nil
}

type recordingImmutablePublisher struct {
	objects  map[string][]byte
	events   *[]string
	failOnce bool
}

func (p *recordingImmutablePublisher) PutImmutable(_ context.Context, key string, payload []byte) error {
	*p.events = append(*p.events, "put:"+key)
	if p.failOnce {
		p.failOnce = false
		return errors.New("object store unavailable")
	}
	if current, found := p.objects[key]; found && !bytes.Equal(current, payload) {
		return fmt.Errorf("immutable object conflict")
	}
	p.objects[key] = append([]byte(nil), payload...)
	return nil
}
