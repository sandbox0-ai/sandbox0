package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/legacyackmigration"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
)

const (
	maxInternalPrivateKeyBytes = 64 << 10
	pausePollInterval          = time.Second
	pauseRetryInterval         = 2 * time.Second
)

type pauseSummary struct {
	StartedAt                  time.Time  `json:"started_at"`
	CompletedAt                *time.Time `json:"completed_at,omitempty"`
	InitialSourceCatalogDigest string     `json:"initial_source_catalog_digest"`
	FinalSourceCatalogDigest   string     `json:"final_source_catalog_digest,omitempty"`
	InitialSandboxCount        int        `json:"initial_sandbox_count"`
	InitiallyActiveCount       int        `json:"initially_active_count"`
	InitiallyPausedCount       int        `json:"initially_paused_count"`
	PauseCandidateCount        int        `json:"pause_candidate_count"`
	ManagerAccessVerified      bool       `json:"manager_access_verified"`
	PauseDispatchStartedAt     *time.Time `json:"pause_dispatch_started_at,omitempty"`
	FinalActiveCount           int        `json:"final_active_count"`
	FinalPausedCount           int        `json:"final_paused_count"`
	ActiveLifecycleTxns        int64      `json:"active_lifecycle_transactions"`
	PendingObjectDeletions     int64      `json:"pending_object_deletions"`
	PendingDeletionWebhooks    int64      `json:"pending_deletion_webhooks"`
}

func parseLoopbackManagerURL(value string) (*url.URL, error) {
	parsed, err := url.ParseRequestURI(strings.TrimSpace(value))
	if err != nil || parsed.Scheme != "http" || parsed.User != nil || parsed.RawQuery != "" ||
		parsed.Fragment != "" || (parsed.Path != "" && parsed.Path != "/") || parsed.Port() == "" {
		return nil, fmt.Errorf("source-manager-url must be an HTTP loopback origin with an explicit port")
	}
	host := net.ParseIP(parsed.Hostname())
	if host == nil || !host.IsLoopback() {
		return nil, fmt.Errorf("source-manager-url must be an HTTP loopback origin with an explicit port")
	}
	parsed.Path = ""
	return parsed, nil
}

func pauseSourceSandboxes(
	ctx context.Context,
	opts options,
	getenv func(string) string,
	normalizeOptions legacyackmigration.NormalizeOptions,
	accessOnly bool,
) (*pauseSummary, *legacyackmigration.Catalog, error) {
	summary := &pauseSummary{StartedAt: time.Now().UTC()}
	dsn, err := loadSourceDSN(opts.sourceDSNFile, strings.TrimSpace(getenv("SANDBOX0_LEGACY_SOURCE_DSN")))
	if err != nil {
		return summary, nil, err
	}
	pool, err := openDatabase(ctx, dsn, "legacy source pause")
	if err != nil {
		return summary, nil, err
	}
	defer pool.Close()
	initial, err := legacyackmigration.ReadCatalog(ctx, pool)
	if err != nil {
		return summary, nil, err
	}
	if _, err := initial.NormalizeForPreflight(normalizeOptions); err != nil {
		return summary, initial, fmt.Errorf("revalidate source before pause: %w", err)
	}
	summary.InitialSourceCatalogDigest, err = initial.Digest()
	if err != nil {
		return summary, initial, err
	}
	summary.InitialSandboxCount = len(initial.Sandboxes)
	for _, sandbox := range initial.Sandboxes {
		switch sandbox.DesiredState {
		case sandboxstore.SandboxDesiredStateActive:
			summary.InitiallyActiveCount++
		case sandboxstore.SandboxDesiredStatePaused:
			summary.InitiallyPausedCount++
		}
	}
	summary.PauseCandidateCount = summary.InitiallyActiveCount

	keyPayload, err := readOwnerOnlyFile(opts.sourceInternalKeyFile, maxInternalPrivateKeyBytes, "source internal private key")
	if err != nil {
		return summary, initial, err
	}
	privateKey, err := internalauth.LoadEd25519PrivateKey(keyPayload)
	if err != nil {
		return summary, initial, fmt.Errorf("load source internal private key: %w", err)
	}
	managerURL, err := parseLoopbackManagerURL(opts.sourceManagerURL)
	if err != nil {
		return summary, initial, err
	}
	client := &http.Client{
		Transport: &http.Transport{Proxy: nil},
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return fmt.Errorf("source manager redirect is forbidden")
		},
	}
	generator := internalauth.NewGenerator(internalauth.GeneratorConfig{
		Caller: "cluster-gateway", PrivateKey: privateKey, TTL: 30 * time.Second,
	})
	if err := verifyManagerSandboxAccess(ctx, client, generator, managerURL, initial.Sandboxes); err != nil {
		return summary, initial, err
	}
	summary.ManagerAccessVerified = true
	if accessOnly {
		summary.FinalActiveCount, summary.FinalPausedCount = sandboxStateCounts(initial)
		summary.ActiveLifecycleTxns = initial.ActiveLifecycleTxns
		summary.PendingObjectDeletions, summary.PendingDeletionWebhooks, err = readRetirementQueueCounts(ctx, pool)
		if err != nil {
			return summary, initial, err
		}
		summary.FinalSourceCatalogDigest = summary.InitialSourceCatalogDigest
		completedAt := time.Now().UTC()
		summary.CompletedAt = &completedAt
		return summary, initial, nil
	}
	if summary.PauseCandidateCount > 0 {
		dispatchStartedAt := time.Now().UTC()
		summary.PauseDispatchStartedAt = &dispatchStartedAt
	}
	for _, sandbox := range initial.Sandboxes {
		if sandbox.DesiredState != sandboxstore.SandboxDesiredStateActive {
			continue
		}
		if err := pauseOneSandbox(ctx, client, generator, managerURL, pool, sandbox); err != nil {
			return summary, nil, err
		}
	}

	for {
		final, readErr := legacyackmigration.ReadCatalog(ctx, pool)
		if readErr != nil {
			return summary, nil, readErr
		}
		active, paused := sandboxStateCounts(final)
		objectDeletions, deletionWebhooks, queueErr := readRetirementQueueCounts(ctx, pool)
		if queueErr != nil {
			return summary, final, queueErr
		}
		summary.FinalActiveCount = active
		summary.FinalPausedCount = paused
		summary.ActiveLifecycleTxns = final.ActiveLifecycleTxns
		summary.PendingObjectDeletions = objectDeletions
		summary.PendingDeletionWebhooks = deletionWebhooks
		if len(final.Sandboxes) != summary.InitialSandboxCount || !sameLiveSandboxSet(initial, final) {
			return summary, final, fmt.Errorf("legacy live sandbox set changed after ingress closure")
		}
		if active == 0 && paused == len(final.Sandboxes) && final.ActiveLifecycleTxns == 0 &&
			objectDeletions == 0 && deletionWebhooks == 0 {
			if _, normalizeErr := final.Normalize(normalizeOptions); normalizeErr != nil {
				return summary, final, fmt.Errorf("strict post-pause validation: %w", normalizeErr)
			}
			summary.FinalSourceCatalogDigest, err = final.Digest()
			if err != nil {
				return summary, final, err
			}
			completedAt := time.Now().UTC()
			summary.CompletedAt = &completedAt
			return summary, final, nil
		}
		if err := waitForPause(ctx, pausePollInterval); err != nil {
			return summary, final, fmt.Errorf("wait for legacy pause drain: %w", err)
		}
	}
}

func verifyManagerSandboxAccess(
	ctx context.Context,
	client *http.Client,
	generator *internalauth.Generator,
	managerURL *url.URL,
	sandboxes []legacyackmigration.Sandbox,
) error {
	for _, sandbox := range sandboxes {
		token, err := generateSandboxManagerToken(generator, sandbox)
		if err != nil {
			return err
		}
		status, err := requestManagerSandbox(ctx, client, managerURL, sandbox.ID, http.MethodGet, "", token)
		if err != nil {
			return fmt.Errorf("verify source manager sandbox access: %w", err)
		}
		if status < 200 || status >= 300 {
			return fmt.Errorf("source manager sandbox access verification failed with HTTP %d", status)
		}
	}
	return nil
}

func pauseOneSandbox(
	ctx context.Context,
	client *http.Client,
	generator *internalauth.Generator,
	managerURL *url.URL,
	pool *pgxpool.Pool,
	sandbox legacyackmigration.Sandbox,
) error {
	for {
		paused, err := sourceSandboxIsPaused(ctx, pool, sandbox.ID, sandbox.TeamID)
		if err != nil {
			return err
		}
		if paused {
			return nil
		}
		token, err := generateSandboxManagerToken(generator, sandbox)
		if err != nil {
			return err
		}
		status, err := requestManagerPause(ctx, client, managerURL, sandbox.ID, token)
		if err == nil && status >= 200 && status < 300 {
			return nil
		}
		if err == nil && status != http.StatusConflict && status != http.StatusTooManyRequests && status < 500 {
			return fmt.Errorf("source manager rejected a sandbox pause with HTTP %d", status)
		}
		if err := waitForPause(ctx, pauseRetryInterval); err != nil {
			return fmt.Errorf("wait to retry source manager pause: %w", err)
		}
	}
}

func requestManagerPause(
	ctx context.Context,
	client *http.Client,
	managerURL *url.URL,
	sandboxID, token string,
) (int, error) {
	return requestManagerSandbox(ctx, client, managerURL, sandboxID, http.MethodPost, "pause", token)
}

func requestManagerSandbox(
	ctx context.Context,
	client *http.Client,
	managerURL *url.URL,
	sandboxID, method, action, token string,
) (int, error) {
	endpoint := *managerURL
	endpoint.Path = "/api/v1/sandboxes/" + url.PathEscape(sandboxID)
	if action != "" {
		endpoint.Path += "/" + url.PathEscape(action)
	}
	request, err := http.NewRequestWithContext(ctx, method, endpoint.String(), nil)
	if err != nil {
		return 0, fmt.Errorf("build source manager sandbox request: %w", err)
	}
	request.Header.Set(internalauth.DefaultTokenHeader, token)
	response, err := client.Do(request)
	if err != nil {
		return 0, err
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 64<<10))
	return response.StatusCode, nil
}

func generateSandboxManagerToken(
	generator *internalauth.Generator,
	sandbox legacyackmigration.Sandbox,
) (string, error) {
	token, err := generator.Generate("manager", sandbox.TeamID, sandbox.UserID, internalauth.GenerateOptions{
		Permissions: []string{"*:*"}, SandboxID: sandbox.ID,
	})
	if err != nil {
		return "", fmt.Errorf("generate source manager sandbox token: %w", err)
	}
	return token, nil
}

func sourceSandboxIsPaused(ctx context.Context, pool *pgxpool.Pool, sandboxID, teamID string) (bool, error) {
	var desiredState string
	err := pool.QueryRow(ctx, `
		SELECT desired_state
		FROM manager.sandboxes
		WHERE sandbox_id = $1 AND team_id = $2 AND deleted_at IS NULL
	`, sandboxID, teamID).Scan(&desiredState)
	if err != nil {
		return false, fmt.Errorf("read source sandbox pause state: %w", err)
	}
	switch desiredState {
	case sandboxstore.SandboxDesiredStatePaused:
		return true, nil
	case sandboxstore.SandboxDesiredStateActive:
		return false, nil
	default:
		return false, fmt.Errorf("source sandbox entered unexpected desired state %s during pause", desiredState)
	}
}

func readRetirementQueueCounts(ctx context.Context, pool *pgxpool.Pool) (int64, int64, error) {
	var objectDeletions, deletionWebhooks int64
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM manager.rootfs_object_deletions`).Scan(&objectDeletions); err != nil {
		return 0, 0, fmt.Errorf("count pending source RootFS deletions: %w", err)
	}
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM manager.sandbox_deletion_webhook_outbox`).Scan(&deletionWebhooks); err != nil {
		return 0, 0, fmt.Errorf("count pending source deletion webhooks: %w", err)
	}
	return objectDeletions, deletionWebhooks, nil
}

func sandboxStateCounts(catalog *legacyackmigration.Catalog) (int, int) {
	var active, paused int
	for _, sandbox := range catalog.Sandboxes {
		switch sandbox.DesiredState {
		case sandboxstore.SandboxDesiredStateActive:
			active++
		case sandboxstore.SandboxDesiredStatePaused:
			paused++
		}
	}
	return active, paused
}

func sameLiveSandboxSet(left, right *legacyackmigration.Catalog) bool {
	if left == nil || right == nil || len(left.Sandboxes) != len(right.Sandboxes) {
		return false
	}
	identities := make(map[string]string, len(left.Sandboxes))
	for _, sandbox := range left.Sandboxes {
		identities[sandbox.ID] = sandbox.TeamID
	}
	for _, sandbox := range right.Sandboxes {
		if identities[sandbox.ID] != sandbox.TeamID {
			return false
		}
	}
	return true
}

func waitForPause(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
