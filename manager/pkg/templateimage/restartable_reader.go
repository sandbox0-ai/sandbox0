package templateimage

import (
	"context"
	"fmt"
	"io"
)

type openAtFunc func(offset int64) (io.ReadCloser, error)

// restartableReader lets containerd restart or resume an upload while keeping
// large layer data outside manager memory.
type restartableReader struct {
	ctx    context.Context
	size   int64
	offset int64
	openAt openAtFunc
	reader io.ReadCloser
}

func newRestartableReader(ctx context.Context, size int64, openAt openAtFunc) (*restartableReader, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	r := &restartableReader{
		ctx:    ctx,
		size:   size,
		openAt: openAt,
	}
	if _, err := r.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	return r, nil
}

func (r *restartableReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	if r.reader == nil {
		return 0, io.ErrClosedPipe
	}
	n, err := r.reader.Read(p)
	r.offset += int64(n)
	return n, err
}

func (r *restartableReader) Seek(offset int64, whence int) (int64, error) {
	var absolute int64
	switch whence {
	case io.SeekStart:
		absolute = offset
	case io.SeekCurrent:
		absolute = r.offset + offset
	case io.SeekEnd:
		absolute = r.size + offset
	default:
		return r.offset, fmt.Errorf("invalid seek whence %d", whence)
	}
	if absolute < 0 || absolute > r.size {
		return r.offset, fmt.Errorf("seek offset %d is outside layer size %d", absolute, r.size)
	}
	if r.reader != nil {
		if err := r.reader.Close(); err != nil {
			return r.offset, fmt.Errorf("close layer reader: %w", err)
		}
		r.reader = nil
	}
	reader, err := r.openAt(absolute)
	if err != nil {
		return r.offset, err
	}
	r.reader = reader
	r.offset = absolute
	return absolute, nil
}

func (r *restartableReader) Close() error {
	if r.reader == nil {
		return nil
	}
	err := r.reader.Close()
	r.reader = nil
	return err
}
