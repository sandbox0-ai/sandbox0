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
	"context"
	"fmt"
	"strings"

	"github.com/opencontainers/go-digest"

	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
)

// ObjectPublicationJournal is the durable pre-PUT boundary for one import
// operation. Both methods must be idempotent for an exact operation/reference
// retry and must reject an existing key with different immutable fields.
type ObjectPublicationJournal interface {
	PrepareObject(context.Context, string, rootfsblock.ObjectReference) error
	MarkObjectPublished(context.Context, string, rootfsblock.ObjectReference) error
}

// JournaledPublisher records exact immutable object intent before allowing a
// network PUT. A crash after PUT but before MarkObjectPublished is recovered by
// replaying the same content-addressed PUT and completion transition.
type JournaledPublisher struct {
	OperationID string
	Journal     ObjectPublicationJournal
	Publisher   rootfsblock.ImmutableObjectPublisher
}

// PutImmutable implements rootfsblock.ImmutableObjectPublisher.
func (p JournaledPublisher) PutImmutable(ctx context.Context, key string, payload []byte) error {
	operationID, err := validateImportOperationID(p.OperationID)
	if err != nil {
		return err
	}
	if p.Journal == nil || p.Publisher == nil {
		return fmt.Errorf("RootFS import publication journal and immutable publisher are required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	reference, err := publicationReference(key, payload)
	if err != nil {
		return err
	}
	if err := p.Journal.PrepareObject(ctx, operationID, reference); err != nil {
		return fmt.Errorf("prepare RootFS import object %q: %w", key, err)
	}
	if err := p.Publisher.PutImmutable(ctx, key, payload); err != nil {
		return fmt.Errorf("publish journaled RootFS import object %q: %w", key, err)
	}
	if err := p.Journal.MarkObjectPublished(ctx, operationID, reference); err != nil {
		return fmt.Errorf("complete RootFS import object %q: %w", key, err)
	}
	return nil
}

func publicationReference(key string, payload []byte) (rootfsblock.ObjectReference, error) {
	checksum := digest.FromBytes(payload)
	kind := ""
	switch {
	case strings.HasSuffix(key, "/packs/sha256/"+checksum.Encoded()):
		kind = rootfsblock.ObjectKindDataPack
	case strings.HasSuffix(key, "/maps/sha256/"+checksum.Encoded()):
		kind = rootfsblock.ObjectKindMappingPage
	default:
		return rootfsblock.ObjectReference{}, fmt.Errorf("RootFS import object key does not bind its kind and payload digest")
	}
	reference := rootfsblock.ObjectReference{
		Key: key, Kind: kind, Size: int64(len(payload)), Checksum: checksum.String(),
	}
	if err := rootfsblock.ValidateObjectReference(reference); err != nil {
		return rootfsblock.ObjectReference{}, err
	}
	return reference, nil
}

func validateImportOperationID(value string) (string, error) {
	if value == "" || value != strings.TrimSpace(value) || len(value) > 128 {
		return "", fmt.Errorf("RootFS import operation ID must contain 1..128 canonical bytes")
	}
	for _, character := range value {
		if character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z' ||
			character >= '0' && character <= '9' || strings.ContainsRune("._:-", character) {
			continue
		}
		return "", fmt.Errorf("RootFS import operation ID contains an invalid character")
	}
	return value, nil
}
