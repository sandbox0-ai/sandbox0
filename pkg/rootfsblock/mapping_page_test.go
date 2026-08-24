package rootfsblock

import (
	"testing"

	"github.com/opencontainers/go-digest"
	"github.com/stretchr/testify/require"
)

func TestMappingPageRoundTrip(t *testing.T) {
	page := MappingPage{
		Level: 0, StartBlock: 10, BlockCount: 8,
		Entries: []MappingEntry{{
			LogicalStart: 12, BlockCount: 2, Kind: MappingEntryData,
			Object: ObjectRange{Key: "packs/base.pack", Offset: 4096, Length: 8192,
				Checksum: digest.FromString("base-range").String()},
		}},
	}
	payload, err := EncodeMappingPage(page)
	require.NoError(t, err)
	decoded, err := DecodeMappingPage(payload)
	require.NoError(t, err)
	require.Equal(t, page, decoded)
}

func TestMappingPageRejectsOverlapAndWrongLevel(t *testing.T) {
	page := validLeafPage()
	page.Entries = append(page.Entries, page.Entries[0])
	require.ErrorContains(t, page.Validate(), "overlaps")

	page = validLeafPage()
	page.Level = 1
	require.ErrorContains(t, page.Validate(), "data in an internal page")

	page = validLeafPage()
	page.Entries[0].Object.Length--
	require.ErrorContains(t, page.Validate(), "does not match")
}

func TestDecodeMappingPageRejectsTrailingAndReservedData(t *testing.T) {
	payload, err := EncodeMappingPage(validLeafPage())
	require.NoError(t, err)
	_, err = DecodeMappingPage(append(payload, 0))
	require.ErrorContains(t, err, "trailing")

	payload[11] = 1
	_, err = DecodeMappingPage(payload)
	require.ErrorContains(t, err, "reserved")
}

func validLeafPage() MappingPage {
	return MappingPage{
		StartBlock: 0, BlockCount: 4,
		Entries: []MappingEntry{{
			LogicalStart: 0, BlockCount: 2, Kind: MappingEntryData,
			Object: ObjectRange{Key: "packs/data.pack", Length: 8192,
				Checksum: digest.FromString("data-range").String()},
		}},
	}
}
