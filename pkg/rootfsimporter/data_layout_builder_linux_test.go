//go:build linux

package rootfsimporter

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/sandbox0-ai/sandbox0/pkg/rootfsartifact"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	"github.com/stretchr/testify/require"
)

type layoutFilesystemFixture struct {
	*fakeFilesystemImageBuilder
	fallback  string
	scanErr   error
	planCalls int
}

func (f *layoutFilesystemFixture) BuildWithBoundedDataRanges(ctx context.Context, source, destination string, size int64, unit int) (rootfsartifact.XFSDataRangePlan, error) {
	f.planCalls++
	if err := f.Build(ctx, source, destination, size); err != nil {
		return rootfsartifact.XFSDataRangePlan{}, err
	}
	if f.scanErr != nil {
		return rootfsartifact.XFSDataRangePlan{}, f.scanErr
	}
	plan, err := rootfsblock.NewDataRangeLayout(size, unit, []rootfsblock.DataRangeSpan{{Start: 4096, End: int64(4096 + unit)}})
	return rootfsartifact.XFSDataRangePlan{Layout: plan, Fallback: f.fallback}, err
}

func TestBlockBuilderLayoutPublicationAndWholeImageFallback(t *testing.T) {
	for _, fallback := range []string{"", rootfsartifact.XFSDataRangeBudgetFallback} {
		t.Run(fallback, func(t *testing.T) {
			f := newOCIBlockBuildFixture(t)
			f.request.BlockOptions.FormatVersion = 2
			baseline, err := f.builder().Build(t.Context(), f.request)
			require.NoError(t, err)
			baselineObjects := f.publisher.objects
			f.publisher = &fakeImmutablePublisher{objects: map[string][]byte{}}
			fs := &layoutFilesystemFixture{fakeFilesystemImageBuilder: f.filesystem, fallback: fallback}
			f.request.DataLayoutPolicy = XFSFileRangesV1
			journal := newRecordingPublicationJournal()
			builder := BlockBuilder{Unpacker: f.unpacker, Filesystem: fs, Publisher: JournaledPublisher{OperationID: "layout-import", Journal: journal, Publisher: f.publisher}}
			built, err := builder.Build(t.Context(), f.request)
			require.NoError(t, err)
			require.Equal(t, 1, fs.planCalls)
			require.Equal(t, XFSFileRangesV1, built.DataLayoutPolicy)
			require.Equal(t, 64<<10, built.DataLayoutRangeBytes)
			require.Equal(t, fallback, built.DataLayoutFallback)
			require.Len(t, journal.prepared, len(built.References))
			for _, ref := range built.References {
				require.Equal(t, "published", journal.states[ref.Key])
				require.Equal(t, ref, journal.prepared[ref.Key])
			}
			reader, err := rootfsblock.NewReader(f.publisher, built.Descriptor, 0)
			require.NoError(t, err)
			got := make([]byte, 128<<10)
			_, err = reader.ReadAt(got, 0)
			require.NoError(t, err)
			want := make([]byte, len(got))
			copy(want, bytes.Repeat([]byte{0x5a}, 4096))
			require.Equal(t, want, got)
			if fallback != "" {
				require.Equal(t, baseline.DescriptorBytes, built.DescriptorBytes)
				require.Equal(t, baselineObjects, f.publisher.objects, "fallback must ignore even a returned nonempty preferred plan")
			}
			proof, _, _, err := built.Attest(2, "procd-http-v1")
			require.NoError(t, err)
			require.Equal(t, fallback, proof.DataLayoutFallback)
			require.NoDirExists(t, f.unpacker.lastRoot)
			require.NoFileExists(t, f.unpacker.lastRoot+".xfs")
		})
	}
}

func TestBlockBuilderRejectsUnimplementedOrInvalidLayoutBeforeUnpacking(t *testing.T) {
	for _, policy := range []string{XFSFileRangesV1, "unknown"} {
		f := newOCIBlockBuildFixture(t)
		f.request.BlockOptions.FormatVersion = 2
		f.request.DataLayoutPolicy = policy
		_, err := f.builder().Build(t.Context(), f.request)
		require.Error(t, err)
		require.Zero(t, f.unpacker.calls)
		require.Empty(t, f.publisher.objects)
	}
}

func TestBlockBuilderDoesNotConvertUnsafeScanErrorsToFallback(t *testing.T) {
	for _, cause := range []error{errors.New("cross-device"), rootfsartifact.ErrXFSDataRangeLimit} {
		f := newOCIBlockBuildFixture(t)
		f.request.BlockOptions.FormatVersion = 2
		f.request.DataLayoutPolicy = XFSFileRangesV1
		fs := &layoutFilesystemFixture{fakeFilesystemImageBuilder: f.filesystem, scanErr: cause}
		_, err := (BlockBuilder{Unpacker: f.unpacker, Filesystem: fs, Publisher: f.publisher}).Build(t.Context(), f.request)
		require.ErrorIs(t, err, cause)
		require.Empty(t, f.publisher.objects)
		require.NoDirExists(t, f.unpacker.lastRoot)
		require.NoFileExists(t, f.unpacker.lastRoot+".xfs")
	}
}
