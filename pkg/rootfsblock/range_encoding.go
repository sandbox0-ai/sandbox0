package rootfsblock

import (
	"context"
	"fmt"

	"github.com/klauspost/compress/zstd"
	"github.com/opencontainers/go-digest"
)

const RangeEncodingZstd = "zstd"
const CompressedDataRangeBytes = 64 << 10

// encodeRangePayload publishes independently decodable ranges. Physical
// offsets belong in the authenticated mapping, not an out-of-band pack index.
// Incompressible bytes stay raw, so compressed storage never expands a range.
func encodeRangePayload(ctx context.Context, payload []byte) ([]byte, ObjectRange, error) {
	var encoder rangeEncoder
	defer encoder.close()
	return encoder.encode(ctx, payload)
}

// Each sequential publication owns one codec. Reusing it avoids rebuilding
// compression tables for every 64KiB range, without a process-global pool that
// retains memory in proportion to historical import/checkpoint concurrency.
type rangeEncoder struct {
	codec *zstd.Encoder
}

func (e *rangeEncoder) close() {
	if e.codec != nil {
		e.codec.Close()
		e.codec = nil
	}
}

func (e *rangeEncoder) encode(ctx context.Context, payload []byte) ([]byte, ObjectRange, error) {
	if err := ctx.Err(); err != nil {
		return nil, ObjectRange{}, err
	}
	if len(payload) == 0 || len(payload) > MaxMappingRootBytes {
		return nil, ObjectRange{}, fmt.Errorf("range payload exceeds encoding bound")
	}
	object := ObjectRange{Length: int64(len(payload)), Checksum: digest.FromBytes(payload).String()}
	if e.codec == nil {
		var err error
		e.codec, err = zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1), zstd.WithWindowSize(CompressedDataRangeBytes), zstd.WithEncoderLevel(zstd.SpeedDefault))
		if err != nil {
			return nil, ObjectRange{}, err
		}
	}
	encoded := e.codec.EncodeAll(payload, nil)
	if err := ctx.Err(); err != nil {
		return nil, ObjectRange{}, err
	}
	if len(encoded) >= len(payload) {
		return payload, object, nil
	}
	object.Encoding, object.EncodedLength = RangeEncodingZstd, int64(len(encoded))
	return encoded, object, nil
}

// decodeRangePayload verifies the mapping's decoded checksum even though the
// object store has already authenticated its encrypted transport frames. Both
// input and output allocations are bounded by the authenticated locator.
func decodeRangePayload(ctx context.Context, object ObjectRange, encoded []byte) ([]byte, error) {
	return decodeRangePayloadInto(ctx, object, encoded, nil, nil)
}

func newRangeDecoder(maxBytes int64) (*zstd.Decoder, error) {
	return zstd.NewReader(nil, zstd.WithDecoderConcurrency(1), zstd.WithDecoderLowmem(true),
		zstd.WithDecoderMaxWindow(CompressedDataRangeBytes), zstd.WithDecoderMaxMemory(uint64(max(maxBytes, CompressedDataRangeBytes))), zstd.WithDecodeAllCapLimit(true))
}

// Bulk reads reuse one decoder and give it an exactly capped output fragment;
// a corrupt neighbor cannot expand into another range's output or cache entry.
func decodeRangePayloadInto(ctx context.Context, object ObjectRange, encoded, target []byte, decoder *zstd.Decoder) ([]byte, error) {
	if err := object.Validate(MaxMappingRootBytes); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if int64(len(encoded)) != object.StoredLength() {
		return nil, fmt.Errorf("encoded range length mismatch")
	}
	decoded := encoded
	if object.Encoding != "" {
		var err error
		if decoder == nil {
			decoder, err = newRangeDecoder(object.Length)
			if err != nil {
				return nil, err
			}
			defer decoder.Close()
		}
		if target == nil {
			target = make([]byte, 0, int(object.Length))
		}
		if int64(cap(target)) < object.Length {
			return nil, fmt.Errorf("decoded range output capacity is too small")
		}
		decoded, err = decoder.DecodeAll(encoded, target[:0:int(object.Length)])
		if err != nil {
			return nil, fmt.Errorf("decode bounded object range: %w", err)
		}
	} else if target != nil {
		if int64(cap(target)) < object.Length {
			return nil, fmt.Errorf("raw range output capacity is too small")
		}
		decoded = target[:int(object.Length)]
		copy(decoded, encoded)
	}
	if int64(len(decoded)) != object.Length {
		return nil, fmt.Errorf("object range decoded length mismatch")
	}
	if digest.FromBytes(decoded).String() != object.Checksum {
		return nil, fmt.Errorf("object range checksum mismatch")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return decoded, nil
}
