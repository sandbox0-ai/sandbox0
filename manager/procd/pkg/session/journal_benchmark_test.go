package session

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

var journalBenchmarkSizes = []struct {
	name string
	size int64
}{
	{name: "regular-1MiB", size: 1 << 20},
	{name: "large-64MiB", size: 64 << 20},
	{name: "huge-256MiB", size: 256 << 20},
}

func BenchmarkJournalResumeV2(b *testing.B) {
	for _, size := range journalBenchmarkSizes {
		b.Run(size.name, func(b *testing.B) {
			path, cursor, stats := buildV2BenchmarkJournal(b, size.size)
			b.ReportMetric(float64(stats.ActiveBytes), "active_bytes")
			b.ReportMetric(float64(stats.SealedSegments), "sealed_segments")
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				journal, err := OpenJournal(path, benchmarkRetention(size.size), cursor)
				if err != nil {
					b.Fatal(err)
				}
				b.StopTimer()
				if err := journal.Close(); err != nil {
					b.Fatal(err)
				}
				b.StartTimer()
			}
		})
	}
}

// BenchmarkLegacyJournalFullScan records the activation cost that v1 paid on
// every resume. V2 keeps this work off the activation path and performs it only
// when legacy history is first queried or pruned.
func BenchmarkLegacyJournalFullScan(b *testing.B) {
	for _, size := range journalBenchmarkSizes {
		b.Run(size.name, func(b *testing.B) {
			path, validBytes := buildLegacyBenchmarkJournal(b, size.size)
			b.ReportMetric(float64(validBytes), "journal_bytes")
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				entries, indexedBytes, err := loadLegacyJournal(path, validBytes)
				if err != nil {
					b.Fatal(err)
				}
				if indexedBytes != validBytes || len(entries) == 0 {
					b.Fatalf("indexed (%d, %d), want %d bytes", len(entries), indexedBytes, validBytes)
				}
			}
		})
	}
}

func buildV2BenchmarkJournal(b *testing.B, targetBytes int64) (string, EventCursor, JournalStats) {
	b.Helper()
	path := filepath.Join(b.TempDir(), "events.jsonl")
	journal, err := OpenJournal(path, benchmarkRetention(targetBytes), EventCursor{})
	if err != nil {
		b.Fatal(err)
	}
	payload := strings.Repeat("x", 32<<10)
	count := int(targetBytes/int64(len(payload))) + 1
	for i := 0; i < count; i++ {
		if _, err := journal.Append(Event{SessionID: "ses-benchmark", Type: "output", DataBase64: payload}); err != nil {
			b.Fatal(err)
		}
	}
	cursor := journal.Cursor()
	stats := journal.Stats()
	if err := journal.Close(); err != nil {
		b.Fatal(err)
	}
	return path, cursor, stats
}

func buildLegacyBenchmarkJournal(b *testing.B, targetBytes int64) (string, int64) {
	b.Helper()
	path := filepath.Join(b.TempDir(), "events.jsonl")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		b.Fatal(err)
	}
	writer := bufio.NewWriterSize(file, 1<<20)
	payload := strings.Repeat("x", 32<<10)
	written := int64(0)
	for seq := int64(1); written < targetBytes; seq++ {
		line, err := json.Marshal(Event{
			Seq:        seq,
			SessionID:  "ses-benchmark",
			Type:       "output",
			DataBase64: payload,
			OccurredAt: time.Now().UTC(),
		})
		if err != nil {
			b.Fatal(err)
		}
		line = append(line, '\n')
		if _, err := writer.Write(line); err != nil {
			b.Fatal(err)
		}
		written += int64(len(line))
	}
	if err := writer.Flush(); err != nil {
		b.Fatal(err)
	}
	if err := file.Sync(); err != nil {
		b.Fatal(err)
	}
	if err := file.Close(); err != nil {
		b.Fatal(err)
	}
	return path, written
}

func benchmarkRetention(size int64) EventRetentionSpec {
	return EventRetentionSpec{MaxBytes: size + 64<<20, MaxAgeSeconds: 24 * 60 * 60}
}
