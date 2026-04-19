package objectstore

import (
	"bytes"
	"io"
	"testing"
)

func TestCountingReaderForPreservesReadSeeker(t *testing.T) {
	reader := countingReaderFor(bytes.NewReader([]byte("hello")))

	readSeeker, ok := reader.(io.ReadSeeker)
	if !ok {
		t.Fatal("counting reader should preserve io.ReadSeeker")
	}

	buf := make([]byte, 2)
	n, err := readSeeker.Read(buf)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if n != 2 || string(buf) != "he" {
		t.Fatalf("Read() = %d, %q; want 2, he", n, string(buf))
	}
	if reader.BytesRead() != 2 {
		t.Fatalf("BytesRead() = %d, want 2", reader.BytesRead())
	}

	if _, err := readSeeker.Seek(0, io.SeekStart); err != nil {
		t.Fatalf("Seek() error = %v", err)
	}
	n, err = readSeeker.Read(buf)
	if err != nil {
		t.Fatalf("Read() after seek error = %v", err)
	}
	if n != 2 || string(buf) != "he" {
		t.Fatalf("Read() after seek = %d, %q; want 2, he", n, string(buf))
	}
	if reader.BytesRead() != 4 {
		t.Fatalf("BytesRead() after seek = %d, want 4", reader.BytesRead())
	}
}
