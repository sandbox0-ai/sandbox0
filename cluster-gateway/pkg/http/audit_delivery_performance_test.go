package http

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/authn"
	"github.com/sandbox0-ai/sandbox0/pkg/sandboxobservability"
	ch "github.com/sandbox0-ai/sandbox0/pkg/sandboxobservability/clickhouse"
	"go.uber.org/zap"
)

// This opt-in test must only use disposable ClickHouse and object storage.
// It can also be copied unchanged into the base revision to measure both
// implementations with the actual HTTP audit middleware and identical traffic.
func TestAuditDeliveryPerformance(t *testing.T) {
	dsn := os.Getenv("SANDBOX0_AUDIT_PERFORMANCE_DSN")
	if dsn == "" {
		t.Skip("disposable performance backend is not configured")
	}
	ctx := context.Background()
	store := s3.NewFromConfig(aws.Config{
		Region: "us-east-1", Credentials: credentials.NewStaticCredentialsProvider("audit-test", "audit-test-only", ""),
	}, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(os.Getenv("SANDBOX0_AUDIT_PERFORMANCE_S3"))
		o.UsePathStyle = true
	})
	if _, err := store.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("audit-test")}); err != nil {
		var alreadyOwned *s3types.BucketAlreadyOwnedByYou
		if !errors.As(err, &alreadyOwned) {
			t.Fatal(err)
		}
	}
	for _, scenario := range []struct {
		name string
		rate int
	}{{"steady", 10}, {"burst", 100}, {"sustained", 400}} {
		t.Run(scenario.name, func(t *testing.T) {
			database := "audit_perf_" + strings.ReplaceAll(uuid.NewString(), "-", "")
			cfg := ch.Config{Database: database, RetentionDays: 90}
			db, _, err := ch.Open(ctx, ch.OpenConfig{DSN: dsn, Schema: cfg})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			defer func() { _, _ = db.ExecContext(ctx, "DROP DATABASE IF EXISTS "+database) }()
			statements, err := ch.SchemaStatements(cfg)
			if err != nil {
				t.Fatal(err)
			}
			for i, statement := range statements {
				if i == 1 {
					statement += ", storage_policy = 'audit_test'"
				}
				if _, err := db.ExecContext(ctx, statement); err != nil {
					t.Fatal(err)
				}
			}
			repo, err := ch.NewRepository(db, cfg)
			if err != nil {
				t.Fatal(err)
			}
			writer := &auditPerformanceWriter{writer: repo}
			d, err := newAuditDelivery(t.TempDir(), writer, zap.NewNop(), auditDeliveryTestSigningKey.Public().(ed25519.PublicKey))
			if err != nil {
				t.Fatal(err)
			}
			runCtx, cancel := context.WithCancel(ctx)
			defer cancel()
			d.Start(runCtx)
			cfgGateway := testAuditConfig()
			cfgGateway.SandboxObservability.AuditDeliveryMode = sandboxobservability.AuditDeliveryModeDurableAsync
			server := &Server{cfg: cfgGateway, auditDelivery: d, auditSigningKey: auditDeliveryTestSigningKey, logger: zap.NewNop()}
			gin.SetMode(gin.TestMode)
			router := gin.New()
			router.Use(func(c *gin.Context) {
				c.Request = c.Request.WithContext(authn.WithAuthContext(c.Request.Context(), &authn.AuthContext{TeamID: "team-1", UserID: "user-1"}))
				c.Next()
			})
			handler := func(c *gin.Context) { c.Status(http.StatusNoContent) }
			router.GET("/api/v1/sandboxes/:id", server.auditSandboxRequests(), handler)
			router.POST("/api/v1/sandboxes/:id/files", server.auditSandboxRequests(), handler)
			api := httptest.NewServer(router)
			defer api.Close()
			client := &http.Client{Timeout: 8 * time.Second}
			jobs := make(chan int, 32)
			var workers sync.WaitGroup
			var mu sync.Mutex
			var readLatencies, mutationLatencies []float64
			var failures int
			for range 16 {
				workers.Go(func() {
					for sequence := range jobs {
						mutation := sequence%20 == 0
						method, path := http.MethodGet, "/api/v1/sandboxes/sb-1"
						if mutation {
							method, path = http.MethodPost, path+"/files"
						}
						request, _ := http.NewRequestWithContext(ctx, method, api.URL+path, nil)
						started := time.Now()
						response, err := client.Do(request)
						ok := err == nil
						if ok {
							_, _ = io.Copy(io.Discard, response.Body)
							_ = response.Body.Close()
							ok = response.StatusCode == http.StatusNoContent
						}
						elapsed := time.Since(started).Seconds()
						mu.Lock()
						if !ok {
							failures++
						}
						if mutation {
							mutationLatencies = append(mutationLatencies, elapsed)
						} else {
							readLatencies = append(readLatencies, elapsed)
						}
						mu.Unlock()
					}
				})
			}
			started := time.Now()
			ticker := time.NewTicker(time.Second / time.Duration(scenario.rate))
			const seconds = 30
			requests := scenario.rate * seconds
			for sequence := range requests {
				<-ticker.C
				jobs <- sequence
			}
			ticker.Stop()
			close(jobs)
			workers.Wait()
			apiSeconds := time.Since(started).Seconds()
			deadline := time.Now().Add(30 * time.Second)
			for writer.count() < requests*2 && time.Now().Before(deadline) {
				time.Sleep(20 * time.Millisecond)
			}
			if failures != 0 || writer.count() != requests*2 {
				t.Fatalf("failures=%d canonical events=%d want=%d", failures, writer.count(), requests*2)
			}
			// All writers have ACKed; allow cleanup to release ownership before stopping.
			for d.pendingCalls.Load() != 0 && time.Now().Before(deadline) {
				time.Sleep(10 * time.Millisecond)
			}
			cancel()
			if _, err := db.ExecContext(ctx, "SYSTEM FLUSH LOGS"); err != nil {
				t.Fatal(err)
			}
			var rows, parts, merges, mergeBytes, uploads, uploadBytes, insertCPU, peakMemory uint64
			qualified := database + ".sandbox_audit_events"
			if err := db.QueryRowContext(ctx, "SELECT count() FROM "+qualified+" FINAL").Scan(&rows); err != nil {
				t.Fatal(err)
			}
			if rows != uint64(requests*2) {
				t.Fatalf("canonical rows=%d want=%d", rows, requests*2)
			}
			if err := db.QueryRowContext(ctx, "SELECT countIf(event_type='NewPart'),countIf(event_type='MergeParts'),sumIf(size_in_bytes,event_type='MergeParts') FROM system.part_log WHERE database=? AND table='sandbox_audit_events'", database).Scan(&parts, &merges, &mergeBytes); err != nil {
				t.Fatal(err)
			}
			var tableUUID string
			if err := db.QueryRowContext(ctx, "SELECT toString(uuid) FROM system.tables WHERE database=? AND name='sandbox_audit_events'", database).Scan(&tableUUID); err != nil {
				t.Fatal(err)
			}
			if err := db.QueryRowContext(ctx, "SELECT count(),sum(data_size) FROM system.blob_storage_log WHERE event_type='Upload' AND position(local_path,?)>0", tableUUID).Scan(&uploads, &uploadBytes); err != nil {
				t.Fatal(err)
			}
			if err := db.QueryRowContext(ctx, "SELECT sum(ProfileEvents['UserTimeMicroseconds']+ProfileEvents['SystemTimeMicroseconds']),max(memory_usage) FROM system.query_log WHERE type='QueryFinish' AND query_kind='Insert' AND has(tables,?)", qualified).Scan(&insertCPU, &peakMemory); err != nil {
				t.Fatal(err)
			}
			result := map[string]any{
				"scenario": scenario.name, "requests": requests, "events": rows, "api_seconds": apiSeconds, "requests_per_second": float64(requests) / apiSeconds,
				"read_p95": auditPerformanceQuantile(readLatencies, .95), "read_p99": auditPerformanceQuantile(readLatencies, .99),
				"mutation_p95": auditPerformanceQuantile(mutationLatencies, .95), "mutation_p99": auditPerformanceQuantile(mutationLatencies, .99),
				"visibility_p95": auditPerformanceQuantile(writer.delays(), .95), "visibility_p99": auditPerformanceQuantile(writer.delays(), .99),
				"new_parts": parts, "merges": merges, "merge_bytes": mergeBytes, "object_uploads": uploads, "object_upload_bytes": uploadBytes,
				"insert_cpu_microseconds": insertCPU, "insert_peak_memory_bytes": peakMemory,
			}
			encoded, err := json.Marshal(result)
			if err != nil {
				t.Fatal(err)
			}
			fmt.Println("AUDIT_PERFORMANCE_RESULT", string(encoded))
		})
	}
}

type auditPerformanceWriter struct {
	writer  auditEventInserter
	mu      sync.Mutex
	visible []float64
}

func (w *auditPerformanceWriter) InsertEvents(ctx context.Context, events []sandboxobservability.Event) error {
	if err := w.writer.InsertEvents(ctx, events); err != nil {
		return err
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, event := range events {
		w.visible = append(w.visible, time.Since(event.IngestedAt).Seconds())
	}
	return nil
}

func (w *auditPerformanceWriter) count() int { w.mu.Lock(); defer w.mu.Unlock(); return len(w.visible) }
func (w *auditPerformanceWriter) delays() []float64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]float64(nil), w.visible...)
}
func auditPerformanceQuantile(values []float64, q float64) float64 {
	copyValues := append([]float64(nil), values...)
	sort.Float64s(copyValues)
	if len(copyValues) == 0 {
		return 0
	}
	return copyValues[int(float64(len(copyValues)-1)*q)]
}
