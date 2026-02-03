package proxy

import (
	"io"
	"net"
	"net/http"
	"sync"
)

func estimateHeaderSize(req *http.Request) int64 {
	if req == nil {
		return 0
	}
	size := int64(0)
	if req.Method != "" {
		size += int64(len(req.Method))
	}
	if req.URL != nil {
		size += int64(len(req.URL.String()))
	}
	if req.Proto != "" {
		size += int64(len(req.Proto))
	}
	size += int64(len(req.Host))
	for k, vals := range req.Header {
		size += int64(len(k))
		for _, v := range vals {
			size += int64(len(v)) + 4
		}
	}
	return size
}

// relay copies data between two connections bidirectionally
func relay(clientReader io.Reader, clientConn, upstreamConn net.Conn) (bytesSent, bytesRecv int64) {
	var wg sync.WaitGroup
	wg.Add(2)

	// Client -> Upstream
	go func() {
		defer wg.Done()
		bytesSent, _ = io.Copy(upstreamConn, clientReader)
	}()

	// Upstream -> Client
	go func() {
		defer wg.Done()
		bytesRecv, _ = io.Copy(clientConn, upstreamConn)
	}()

	wg.Wait()
	return
}

// limitedReader wraps a reader with a size limit
type limitedReader struct {
	r io.Reader
	n int64
}

func newLimitedReader(r io.Reader, limit int64) *limitedReader {
	return &limitedReader{r: r, n: limit}
}

func (l *limitedReader) Read(p []byte) (n int, err error) {
	if l.n <= 0 {
		return 0, io.EOF
	}
	if int64(len(p)) > l.n {
		p = p[0:l.n]
	}
	n, err = l.r.Read(p)
	l.n -= int64(n)
	return
}
