package rootfsblock

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
)

// materializedDataReader is deliberately private: arbitrary ReaderAt values
// and tenant-provided extent hints cannot opt into omitting logical bytes.
type materializedDataReader interface {
	io.ReaderAt
	nextDataOffset(context.Context, int64) (int64, error)
}

var errMaterializedSeekUnsupported = errors.New("materialized file hole discovery unsupported")

// BuildMaterializedFileGeneration publishes an exclusively owned, immutable,
// read-only local file. On Linux it skips only filesystem-reported holes and
// preserves exactly the ordinary builder's object and descriptor bytes. A nil
// layout selects the ordinary global grid; a non-nil layout retains the same
// explicit format-two contract as BuildMaterializedGenerationWithLayout.
//
// The caller must exclude all writers for the entire call and still owns the
// file. Its current position is restored. Unsupported hole discovery falls back
// to complete reads, not guessed zero bytes. Detected mutation and I/O failures
// discard the result, even when immutable partial objects were already written.
func BuildMaterializedFileGeneration(ctx context.Context, file *os.File, logicalSize int64,
	publisher ImmutableObjectPublisher, options BuildOptions, layout *DataRangeLayout,
) (result BuildResult, resultErr error) {
	if file == nil {
		return BuildResult{}, fmt.Errorf("materialized image file is required")
	}
	before, err := file.Stat()
	if err != nil {
		return BuildResult{}, fmt.Errorf("stat materialized image: %w", err)
	}
	if !before.Mode().IsRegular() || before.Size() != logicalSize {
		return BuildResult{}, fmt.Errorf("materialized image must be a regular file of the exact logical size")
	}
	enabled, err := materializedFileSeekEnabled(file)
	if err != nil {
		return BuildResult{}, err
	}
	position, err := file.Seek(0, io.SeekCurrent)
	if err != nil {
		return BuildResult{}, fmt.Errorf("read materialized image position: %w", err)
	}
	defer func() {
		_, positionErr := file.Seek(position, io.SeekStart)
		after, statErr := file.Stat()
		if statErr == nil && !sameMaterializedFile(before, after) {
			statErr = fmt.Errorf("materialized image changed during publication")
		}
		if err := errors.Join(positionErr, statErr, ctx.Err()); err != nil {
			result = BuildResult{}
			resultErr = errors.Join(resultErr, err)
		}
	}()
	reader := &materializedFileReader{ReaderAt: file, size: logicalSize, disabled: !enabled,
		seek: func(offset int64, hole bool) (int64, error) { return seekMaterializedFile(file, offset, hole) }}
	return buildMaterializedGeneration(ctx, reader, logicalSize, publisher, options, layout)
}

type materializedFileReader struct {
	io.ReaderAt
	size      int64
	extentEnd int64
	disabled  bool
	seek      func(offset int64, hole bool) (int64, error)
}

func (r *materializedFileReader) nextDataOffset(ctx context.Context, offset int64) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if offset < 0 || offset >= r.size {
		return 0, fmt.Errorf("materialized seek offset is outside the image")
	}
	if r.disabled || offset < r.extentEnd {
		return offset, nil
	}
	data, err := r.seek(offset, false)
	if errors.Is(err, errMaterializedSeekUnsupported) {
		r.disabled = true
		return offset, nil
	}
	if errors.Is(err, io.EOF) {
		return r.size, ctx.Err()
	}
	if err != nil {
		return 0, err
	}
	if data < offset || data >= r.size {
		return 0, fmt.Errorf("materialized data extent starts outside the remaining image")
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	end, err := r.seek(data, true)
	if errors.Is(err, errMaterializedSeekUnsupported) {
		r.disabled = true
		return offset, nil
	}
	if err != nil {
		return 0, err
	}
	if end <= data || end > r.size {
		return 0, fmt.Errorf("materialized data extent ends outside the image")
	}
	r.extentEnd = end
	return data, ctx.Err()
}
