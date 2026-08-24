package rootfsblock

import (
	"math"
	"strings"
	"testing"

	"github.com/opencontainers/go-digest"
	"github.com/stretchr/testify/require"
)

func TestDescriptorRoundTrip(t *testing.T) {
	descriptor := validDescriptor()
	payload, err := EncodeDescriptor(descriptor)
	require.NoError(t, err)
	decoded, err := DecodeDescriptor(payload)
	require.NoError(t, err)
	require.Equal(t, descriptor, decoded)
}

func TestDescriptorRejectsUnsafeObjectKeyAndWrongRoot(t *testing.T) {
	descriptor := validDescriptor()
	descriptor.MappingRoot.Object.Key = "../mapping"
	require.ErrorContains(t, descriptor.Validate(), "canonical relative key")

	descriptor = validDescriptor()
	descriptor.MappingRoot.Object.Key = " rootfs/maps/root.page"
	require.ErrorContains(t, descriptor.Validate(), "canonical relative key")

	descriptor = validDescriptor()
	descriptor.MappingRoot.RootDigest = "sha256:" + strings.Repeat("z", 64)
	require.ErrorContains(t, descriptor.Validate(), "canonical sha256 digest")
}

func TestDescriptorRejectsObjectRangeOffsetOverflow(t *testing.T) {
	descriptor := validDescriptor()
	descriptor.MappingRoot.Object.Offset = math.MaxInt64
	descriptor.MappingRoot.Object.Length = 2
	require.ErrorContains(t, descriptor.Validate(), "offset or length")
}

func TestDescriptorRejectsUnknownFieldsAndTrailingValues(t *testing.T) {
	payload, err := EncodeDescriptor(validDescriptor())
	require.NoError(t, err)
	payload = append(payload[:len(payload)-1], []byte(`,"unknown":true}`)...)
	_, err = DecodeDescriptor(payload)
	require.ErrorContains(t, err, "unknown field")

	payload, err = EncodeDescriptor(validDescriptor())
	require.NoError(t, err)
	_, err = DecodeDescriptor(append(payload, []byte(` {}`)...))
	require.ErrorContains(t, err, "trailing data")
}

func TestCompositeTailChecksumIsExact(t *testing.T) {
	descriptor := validDescriptor()
	tail, err := EncodeCompositeTail([]BlockUpdate{{Block: 0, Data: make([]byte, LogicalBlockSize)}}, uint64(descriptor.LogicalSizeBytes/LogicalBlockSize))
	require.NoError(t, err)
	descriptor.CompositeTail = &tail
	require.NoError(t, descriptor.Validate())
	descriptor.CompositeTail.Payload[compositeTailHeaderBytes+7] ^= 0xff
	require.ErrorContains(t, descriptor.Validate(), "does not match")
}

func validDescriptor() Descriptor {
	return Descriptor{
		Version: DescriptorVersion, LogicalSizeBytes: 1 << 30, BlockSizeBytes: LogicalBlockSize,
		MappingRoot: MappingRootLocator{
			Version: MappingPageVersion, RootDigest: digest.FromString("root").String(),
			Object: ObjectRange{
				Key: "rootfs/maps/root.page", Offset: 4096, Length: 8192,
				Checksum: digest.FromString("mapping-page").String(),
			},
		},
	}
}
