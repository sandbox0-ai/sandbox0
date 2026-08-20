package dbpool

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const runPostgresHATestsEnv = "SANDBOX0_RUN_PG_HA_TESTS"

func TestPostgresPrimaryPromotionPreservesExactOperationReplay(t *testing.T) {
	if os.Getenv(runPostgresHATestsEnv) != "1" {
		t.Skipf("set %s=1 to run the local PostgreSQL promotion test", runPostgresHATestsEnv)
	}
	for _, binary := range []string{"initdb", "pg_ctl", "pg_basebackup"} {
		if _, err := exec.LookPath(binary); err != nil {
			t.Fatalf("required PostgreSQL binary %q is unavailable: %v", binary, err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	root, err := os.MkdirTemp("", "sandbox0-pg-ha-")
	if err != nil {
		t.Fatalf("create PostgreSQL HA test root: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	runner := newPostgresCommandRunner(t, root)
	primaryData := filepath.Join(root, "primary")
	standbyData := filepath.Join(root, "standby")
	primaryLog := filepath.Join(root, "primary.log")
	standbyLog := filepath.Join(root, "standby.log")
	primaryPort, standbyPort := reserveTCPPorts(t)

	runner.mustRun(ctx, "initdb", "-D", primaryData, "-A", "trust", "-U", "postgres", "--no-locale", "--encoding=UTF8")
	primaryOptions := postgresStartOptions(primaryPort, true)
	runner.mustRun(ctx, "pg_ctl", "-D", primaryData, "-l", primaryLog, "-o", primaryOptions, "-w", "start")
	primaryRunning := true
	defer func() {
		if primaryRunning {
			runner.stopIgnoringError(primaryData)
		}
	}()

	runner.mustRun(ctx,
		"pg_basebackup",
		"-h", "127.0.0.1",
		"-p", strconv.Itoa(primaryPort),
		"-U", "postgres",
		"-D", standbyData,
		"-R",
		"-X", "stream",
		"-c", "fast",
	)
	standbyOptions := postgresStartOptions(standbyPort, false)
	runner.mustRun(ctx, "pg_ctl", "-D", standbyData, "-l", standbyLog, "-o", standbyOptions, "-w", "start")
	standbyRunning := true
	defer func() {
		if standbyRunning {
			runner.stopIgnoringError(standbyData)
		}
	}()

	databaseURL := fmt.Sprintf(
		"postgres://postgres@127.0.0.1:%d,127.0.0.1:%d/postgres?sslmode=disable",
		primaryPort,
		standbyPort,
	)
	pool, err := New(ctx, Options{
		DatabaseURL:          databaseURL,
		MaxConns:             1,
		RequirePrimary:       true,
		ConnectTimeout:       time.Second,
		PrimaryCheckInterval: 10 * time.Millisecond,
		PrimaryCheckTimeout:  time.Second,
	})
	if err != nil {
		t.Fatalf("create HA pool: %v\nprimary log:\n%s\nstandby log:\n%s", err, readTestLog(primaryLog), readTestLog(standbyLog))
	}
	defer pool.Close()

	tableName := fmt.Sprintf("dbpool_ha_%d", time.Now().UnixNano())
	if _, err := pool.Exec(ctx, fmt.Sprintf("CREATE TABLE %s (operation_id text PRIMARY KEY)", tableName)); err != nil {
		t.Fatalf("create replay table: %v", err)
	}
	operationID := "operation-before-response-loss"
	insertSQL := fmt.Sprintf("INSERT INTO %s (operation_id) VALUES ($1) ON CONFLICT DO NOTHING", tableName)
	if _, err := pool.Exec(ctx, insertSQL, operationID); err != nil {
		t.Fatalf("commit operation on original primary: %v", err)
	}

	standbyURL := fmt.Sprintf("postgres://postgres@127.0.0.1:%d/postgres?sslmode=disable", standbyPort)
	standbyPool, err := pgxpool.New(ctx, standbyURL)
	if err != nil {
		t.Fatalf("connect standby observer: %v", err)
	}
	waitForReplicatedOperation(t, ctx, standbyPool, tableName, operationID)
	standbyPool.Close()

	failoverStarted := time.Now()
	runner.mustRun(ctx, "pg_ctl", "-D", primaryData, "-m", "immediate", "-w", "stop")
	primaryRunning = false
	runner.mustRun(ctx, "pg_ctl", "-D", standbyData, "-w", "promote")

	failoverCtx, failoverCancel := context.WithTimeout(ctx, 10*time.Second)
	defer failoverCancel()
	for {
		attemptCtx, attemptCancel := context.WithTimeout(failoverCtx, 2*time.Second)
		_, err = pool.Exec(attemptCtx, insertSQL, operationID)
		attemptCancel()
		if err == nil {
			break
		}
		if failoverCtx.Err() != nil {
			t.Fatalf("HA pool did not reach promoted primary: %v\nprimary log:\n%s\nstandby log:\n%s", err, readTestLog(primaryLog), readTestLog(standbyLog))
		}
		time.Sleep(25 * time.Millisecond)
	}
	if elapsed := time.Since(failoverStarted); elapsed > 10*time.Second {
		t.Fatalf("primary failover took %s, want at most 10s", elapsed)
	}

	var inRecovery bool
	var operationCount int
	if err := pool.QueryRow(ctx, "SELECT pg_is_in_recovery()").Scan(&inRecovery); err != nil {
		t.Fatalf("inspect promoted server: %v", err)
	}
	if inRecovery {
		t.Fatal("HA pool returned a standby after promotion")
	}
	if err := pool.QueryRow(ctx, fmt.Sprintf("SELECT count(*) FROM %s WHERE operation_id = $1", tableName), operationID).Scan(&operationCount); err != nil {
		t.Fatalf("count replayed operations: %v", err)
	}
	if operationCount != 1 {
		t.Fatalf("operation count = %d, want exactly 1", operationCount)
	}
	if _, err := pool.Exec(ctx, fmt.Sprintf("DROP TABLE %s", tableName)); err != nil {
		t.Fatalf("drop replay table: %v", err)
	}

	runner.mustRun(ctx, "pg_ctl", "-D", standbyData, "-m", "fast", "-w", "stop")
	standbyRunning = false
}

type postgresCommandRunner struct {
	t          *testing.T
	home       string
	credential *syscall.Credential
}

func newPostgresCommandRunner(t *testing.T, root string) *postgresCommandRunner {
	t.Helper()
	runner := &postgresCommandRunner{t: t, home: root}
	if os.Geteuid() != 0 {
		return runner
	}

	postgresUser, err := user.Lookup("postgres")
	if err != nil {
		t.Fatalf("look up postgres user required when tests run as root: %v", err)
	}
	uid, err := strconv.ParseUint(postgresUser.Uid, 10, 32)
	if err != nil {
		t.Fatalf("parse postgres uid: %v", err)
	}
	gid, err := strconv.ParseUint(postgresUser.Gid, 10, 32)
	if err != nil {
		t.Fatalf("parse postgres gid: %v", err)
	}
	if err := os.Chown(root, int(uid), int(gid)); err != nil {
		t.Fatalf("grant postgres ownership of test root: %v", err)
	}
	runner.credential = &syscall.Credential{Uid: uint32(uid), Gid: uint32(gid)}
	return runner
}

func (r *postgresCommandRunner) mustRun(ctx context.Context, name string, args ...string) {
	r.t.Helper()
	output, err := r.run(ctx, name, args...)
	if err != nil {
		r.t.Fatalf("%s %s: %v\n%s", name, strings.Join(args, " "), err, output)
	}
}

func (r *postgresCommandRunner) runIgnoringError(ctx context.Context, name string, args ...string) {
	r.t.Helper()
	_, _ = r.run(ctx, name, args...)
}

func (r *postgresCommandRunner) stopIgnoringError(dataDirectory string) {
	r.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	r.runIgnoringError(ctx, "pg_ctl", "-D", dataDirectory, "-m", "immediate", "-w", "stop")
}

func (r *postgresCommandRunner) run(ctx context.Context, name string, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Env = append(os.Environ(), "HOME="+r.home)
	if r.credential != nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{Credential: r.credential}
	}
	output, err := cmd.CombinedOutput()
	return string(output), err
}

func reserveTCPPorts(t *testing.T) (int, int) {
	t.Helper()
	first, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve first PostgreSQL port: %v", err)
	}
	defer first.Close()
	second, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve second PostgreSQL port: %v", err)
	}
	defer second.Close()
	return first.Addr().(*net.TCPAddr).Port, second.Addr().(*net.TCPAddr).Port
}

func postgresStartOptions(port int, primary bool) string {
	options := []string{
		"-c listen_addresses=127.0.0.1",
		"-c port=" + strconv.Itoa(port),
		"-c hot_standby=on",
	}
	if primary {
		options = append(options,
			"-c wal_level=replica",
			"-c max_wal_senders=5",
			"-c max_replication_slots=5",
		)
	}
	return strings.Join(options, " ")
}

func waitForReplicatedOperation(t *testing.T, ctx context.Context, pool *pgxpool.Pool, tableName, operationID string) {
	t.Helper()
	query := fmt.Sprintf("SELECT count(*) FROM %s WHERE operation_id = $1", tableName)
	for {
		var count int
		err := pool.QueryRow(ctx, query, operationID).Scan(&count)
		if err == nil && count == 1 {
			return
		}
		if ctx.Err() != nil {
			t.Fatalf("operation did not replicate to standby: %v", err)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func readTestLog(path string) string {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Sprintf("read %s: %v", path, err)
	}
	return string(data)
}
