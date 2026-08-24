package sandboxstore

import (
	"bytes"
	"context"
	"testing"

	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	"github.com/stretchr/testify/require"
)

func TestRootFSMaterializationBatchIDBindsOrderedExactMembership(t *testing.T) {
	objects := &rootFSMaterializationIDObjects{objects: make(map[string][]byte)}
	base, err := rootfsblock.BuildMaterializedGeneration(
		t.Context(), bytes.NewReader(make([]byte, rootfsblock.LogicalBlockSize)),
		rootfsblock.LogicalBlockSize, objects, rootfsblock.BuildOptions{},
	)
	require.NoError(t, err)
	_, composite, err := rootfsblock.BuildCompositeGeneration(base.Descriptor, []rootfsblock.BlockUpdate{{
		Sequence: 1, Block: 0, Data: bytes.Repeat([]byte{1}, rootfsblock.LogicalBlockSize),
	}})
	require.NoError(t, err)
	members := []RootFSGenerationMaterializationIdentity{
		{GenerationID: "generation-a", ExpectedLocatorVersion: 1, ExpectedDescriptor: composite},
		{GenerationID: "generation-b", ExpectedLocatorVersion: 2, ExpectedDescriptor: composite},
	}
	lane := RootFSMaterializationPackLane("team-a", 1)
	first, err := RootFSMaterializationBatchID(lane, members)
	require.NoError(t, err)
	second, err := RootFSMaterializationBatchID(lane, members)
	require.NoError(t, err)
	require.Equal(t, first, second)
	reversed := []RootFSGenerationMaterializationIdentity{members[1], members[0]}
	reversedID, err := RootFSMaterializationBatchID(lane, reversed)
	require.NoError(t, err)
	require.NotEqual(t, first, reversedID)
	changed := append([]RootFSGenerationMaterializationIdentity(nil), members...)
	changed[0].ExpectedLocatorVersion++
	changedID, err := RootFSMaterializationBatchID(lane, changed)
	require.NoError(t, err)
	require.NotEqual(t, first, changedID)
	otherLaneID, err := RootFSMaterializationBatchID(
		RootFSMaterializationPackLane("team-b", 1), members,
	)
	require.NoError(t, err)
	require.NotEqual(t, first, otherLaneID)
}

func TestRootFSMaterializationObjectReferenceValidation(t *testing.T) {
	valid := rootfsblock.ObjectReference{
		Key: "rootfs/v1/packs/sha256/abc", Kind: rootfsblock.ObjectKindDataPack,
		Size: rootfsblock.LogicalBlockSize, Checksum: digest.FromString("payload").String(),
	}
	require.NoError(t, validateRootFSMaterializationObjectReference(valid))
	for name, mutate := range map[string]func(*rootfsblock.ObjectReference){
		"traversal": func(value *rootfsblock.ObjectReference) { value.Key = "../object" },
		"oversize":  func(value *rootfsblock.ObjectReference) { value.Size = rootfsblock.DefaultPackBytes + 1 },
		"kind":      func(value *rootfsblock.ObjectReference) { value.Kind = "unknown" },
		"checksum":  func(value *rootfsblock.ObjectReference) { value.Checksum = "sha256:ABC" },
	} {
		t.Run(name, func(t *testing.T) {
			candidate := valid
			mutate(&candidate)
			require.Error(t, validateRootFSMaterializationObjectReference(candidate))
		})
	}
}

type rootFSMaterializationIDObjects struct{ objects map[string][]byte }

func (s *rootFSMaterializationIDObjects) PutImmutable(_ context.Context, key string, payload []byte) error {
	s.objects[key] = append([]byte(nil), payload...)
	return nil
}
