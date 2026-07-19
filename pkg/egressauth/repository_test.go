package egressauth

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sandbox0-ai/sandbox0/pkg/dbpool"
	"github.com/sandbox0-ai/sandbox0/pkg/egressauth/migrations"
	"github.com/sandbox0-ai/sandbox0/pkg/migrate"
	"github.com/sandbox0-ai/sandbox0/pkg/pubsub"
	"github.com/sandbox0-ai/sandbox0/pkg/resourceguard"
	"github.com/sandbox0-ai/sandbox0/pkg/teamquota"
	teamquotatestutil "github.com/sandbox0-ai/sandbox0/pkg/teamquota/testutil"
)

type testMigrateLogger struct{}

func (testMigrateLogger) Printf(string, ...any) {}
func (testMigrateLogger) Fatalf(format string, args ...any) {
	panic(fmt.Sprintf(format, args...))
}

func TestRepositoryPutSourceAdvancesExistingBindings(t *testing.T) {
	ctx := context.Background()
	repo, _ := newRepositoryTestStore(t)

	first, err := repo.PutSource(ctx, "team-1", staticHeadersSourceWriteRequest("github-source", "old-token"))
	if err != nil {
		t.Fatalf("put first source: %v", err)
	}
	source, err := repo.GetSourceByRef(ctx, "team-1", "github-source")
	if err != nil {
		t.Fatalf("get source by ref: %v", err)
	}
	if source == nil {
		t.Fatal("source is nil")
	}
	if err := repo.UpsertBindings(ctx, &BindingRecord{
		TeamID:    "team-1",
		SandboxID: "sbx-1",
		Bindings: []CredentialBinding{{
			Ref:           "github-api",
			SourceRef:     "github-source",
			SourceID:      source.ID,
			SourceVersion: first.CurrentVersion,
			Projection: ProjectionSpec{
				Type: CredentialProjectionTypeHTTPHeaders,
				HTTPHeaders: &HTTPHeadersProjection{
					Headers: []ProjectedHeader{{
						Name:          "Authorization",
						ValueTemplate: "Bearer {{ .token }}",
					}},
				},
			},
		}},
	}); err != nil {
		t.Fatalf("upsert bindings: %v", err)
	}

	rotated, err := repo.PutSource(ctx, "team-1", staticHeadersSourceWriteRequest("github-source", "new-token"))
	if err != nil {
		t.Fatalf("rotate source: %v", err)
	}
	if rotated.CurrentVersion != 2 {
		t.Fatalf("rotated version = %d, want 2", rotated.CurrentVersion)
	}

	record, err := repo.GetBindings(ctx, "team-1", "sbx-1")
	if err != nil {
		t.Fatalf("get bindings: %v", err)
	}
	if record == nil || len(record.Bindings) != 1 {
		t.Fatalf("bindings = %#v, want one binding", record)
	}
	if got := record.Bindings[0].SourceVersion; got != rotated.CurrentVersion {
		t.Fatalf("binding source version = %d, want %d", got, rotated.CurrentVersion)
	}
	version, err := repo.GetSourceVersion(ctx, source.ID, record.Bindings[0].SourceVersion)
	if err != nil {
		t.Fatalf("get source version: %v", err)
	}
	if version == nil || version.Spec.StaticHeaders == nil {
		t.Fatalf("source version = %#v, want static headers", version)
	}
	if got := version.Spec.StaticHeaders.Values["token"]; got != "new-token" {
		t.Fatalf("resolved token = %q, want new-token", got)
	}
	var retainedVersions int
	if err := repo.pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM credential_source_versions
		WHERE source_id = $1
	`, source.ID).Scan(&retainedVersions); err != nil {
		t.Fatalf("count retained source versions: %v", err)
	}
	if retainedVersions != 1 {
		t.Fatalf("retained source versions = %d, want current-only retention", retainedVersions)
	}
	oldVersion, err := repo.GetSourceVersion(ctx, source.ID, first.CurrentVersion)
	if err != nil {
		t.Fatalf("get compacted source version: %v", err)
	}
	if oldVersion != nil {
		t.Fatalf("compacted source version = %#v, want nil", oldVersion)
	}
}

func TestRepositoryPutSourcePublishesRotationEvent(t *testing.T) {
	ctx := context.Background()
	repo, pool := newRepositoryTestStore(t)

	listenConn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire listen connection: %v", err)
	}
	defer listenConn.Release()
	if _, err := listenConn.Exec(ctx, "LISTEN "+pubsub.CredentialSourceRotationChannel); err != nil {
		t.Fatalf("listen: %v", err)
	}

	first, err := repo.PutSource(ctx, "team-1", staticHeadersSourceWriteRequest("github-source", "old-token"))
	if err != nil {
		t.Fatalf("put first source: %v", err)
	}
	source, err := repo.GetSourceByRef(ctx, "team-1", "github-source")
	if err != nil {
		t.Fatalf("get source by ref: %v", err)
	}
	if source == nil {
		t.Fatal("source is nil")
	}
	if first.CurrentVersion != 1 {
		t.Fatalf("first version = %d, want 1", first.CurrentVersion)
	}

	rotated, err := repo.PutSource(ctx, "team-1", staticHeadersSourceWriteRequest("github-source", "new-token"))
	if err != nil {
		t.Fatalf("rotate source: %v", err)
	}
	if rotated.CurrentVersion != 2 {
		t.Fatalf("rotated version = %d, want 2", rotated.CurrentVersion)
	}

	waitCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	notification, err := listenConn.Conn().WaitForNotification(waitCtx)
	if err != nil {
		t.Fatalf("wait for notification: %v", err)
	}
	if notification.Channel != pubsub.CredentialSourceRotationChannel {
		t.Fatalf("notification channel = %q, want %q", notification.Channel, pubsub.CredentialSourceRotationChannel)
	}
	var event pubsub.CredentialSourceRotatedEvent
	if err := json.Unmarshal([]byte(notification.Payload), &event); err != nil {
		t.Fatalf("unmarshal event: %v", err)
	}
	if event.TeamID != "team-1" || event.SourceID != source.ID || event.SourceRef != "github-source" || event.SourceVersion != 2 {
		t.Fatalf("rotation event = %#v", event)
	}
	if event.Timestamp.IsZero() {
		t.Fatal("rotation event timestamp is zero")
	}
}

func TestRepositoryPutSourceRejectsResolverKindChange(t *testing.T) {
	ctx := context.Background()
	repo, _ := newRepositoryTestStore(t)

	if _, err := repo.PutSource(ctx, "team-1", staticHeadersSourceWriteRequest("github-source", "token")); err != nil {
		t.Fatalf("put first source: %v", err)
	}
	_, err := repo.PutSource(ctx, "team-1", &CredentialSourceWriteRequest{
		Name:         "github-source",
		ResolverKind: "static_username_password",
		Spec: CredentialSourceSecretSpec{
			StaticUsernamePassword: &StaticUsernamePasswordSourceSpec{
				Username: "alice",
				Password: "secret",
			},
		},
	})
	if !errors.Is(err, ErrCredentialSourceResolverKindImmutable) {
		t.Fatalf("resolver kind change error = %v, want ErrCredentialSourceResolverKindImmutable", err)
	}
}

func TestRepositoryControlPlaneQuotaCountsSourceAndCurrentVersion(t *testing.T) {
	ctx := context.Background()
	repo, pool := newRepositoryTestStore(t)
	quotaRepo := teamquota.NewRepository(pool)
	repo.teamQuotaStore = quotaRepo
	teamID := "team-source-quota-" + uuid.NewString()
	if err := quotaRepo.UnsafePutTeamPolicyForTest(ctx, teamID, teamquota.Policy{
		Key:   teamquota.KeyControlPlaneObjectCount,
		Kind:  teamquota.KindCapacity,
		Limit: 2,
	}); err != nil {
		t.Fatalf("set team quota: %v", err)
	}

	if _, err := repo.PutSource(ctx, teamID, staticHeadersSourceWriteRequest("first-source", "first-token")); err != nil {
		t.Fatalf("put first source: %v", err)
	}
	if _, err := repo.PutSource(ctx, teamID, staticHeadersSourceWriteRequest("blocked-source", "blocked-token")); !teamquota.IsExceeded(err) {
		t.Fatalf("put second source error = %v, want quota exceeded", err)
	}
	if err := repo.DeleteSource(ctx, teamID, "first-source"); err != nil {
		t.Fatalf("delete first source: %v", err)
	}
	if _, err := repo.PutSource(ctx, teamID, staticHeadersSourceWriteRequest("replacement-source", "replacement-token")); err != nil {
		t.Fatalf("put replacement source: %v", err)
	}
}

func TestRepositoryManagedVaultSourceUsesBoundedPathAndDeletesMetadata(t *testing.T) {
	ctx := context.Background()
	repo, _ := newRepositoryTestStore(t)

	var metadataWrites, dataWrites, metadataDeletes int
	vaultServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && strings.Contains(r.URL.Path, "/metadata/"):
			metadataWrites++
			var body struct {
				MaxVersions int `json:"max_versions"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode vault metadata: %v", err)
			}
			if body.MaxVersions != 1 {
				t.Fatalf("vault max_versions = %d, want 1", body.MaxVersions)
			}
			w.WriteHeader(http.StatusNoContent)
		case r.Method == http.MethodPost && strings.Contains(r.URL.Path, "/data/"):
			dataWrites++
			_ = json.NewEncoder(w).Encode(map[string]any{})
		case r.Method == http.MethodDelete && strings.Contains(r.URL.Path, "/metadata/"):
			metadataDeletes++
			w.WriteHeader(http.StatusNoContent)
		default:
			t.Fatalf("unexpected vault request %s %s", r.Method, r.URL.Path)
		}
	}))
	defer vaultServer.Close()

	tokenFile := filepath.Join(t.TempDir(), "vault-token")
	if err := os.WriteFile(tokenFile, []byte("test-token"), 0o600); err != nil {
		t.Fatalf("write vault token: %v", err)
	}
	resolver, err := NewVaultResolver([]VaultConnectionConfig{{
		Name:                "default",
		Address:             vaultServer.URL,
		TokenFile:           tokenFile,
		DefaultMount:        "secret",
		AllowedPathPrefixes: []string{"sandbox0/credential-sources/{{teamID}}/"},
	}})
	if err != nil {
		t.Fatalf("new vault resolver: %v", err)
	}
	repo.defaultStorageKind = CredentialSourceStorageKindHashiCorpVault
	repo.vaultResolver = resolver

	request := staticHeadersSourceWriteRequest("github-source", "old-token")
	request.StorageKind = CredentialSourceStorageKindHashiCorpVault
	if _, err := repo.PutSource(ctx, "team-vault", request); err != nil {
		t.Fatalf("put managed vault source: %v", err)
	}
	request.Spec.StaticHeaders.Values["token"] = "new-token"
	if _, err := repo.PutSource(ctx, "team-vault", request); err != nil {
		t.Fatalf("rotate managed vault source: %v", err)
	}
	source, err := repo.GetSourceByRef(ctx, "team-vault", "github-source")
	if err != nil || source == nil {
		t.Fatalf("get managed source = (%#v, %v)", source, err)
	}
	var payload []byte
	if err := repo.pool.QueryRow(ctx, `
		SELECT storage_payload
		FROM credential_source_versions
		WHERE source_id = $1 AND version = $2
	`, source.ID, source.CurrentVersion).Scan(&payload); err != nil {
		t.Fatalf("read managed vault ref: %v", err)
	}
	ref, managed, err := decodeExternalRefStorage(payload)
	if err != nil {
		t.Fatalf("decode managed vault ref: %v", err)
	}
	if ref.Path != "sandbox0/credential-sources/team-vault/github-source" || !managed {
		t.Fatalf("managed vault ref = %#v, managed=%v", ref, managed)
	}
	if err := repo.DeleteSource(ctx, "team-vault", "github-source"); err != nil {
		t.Fatalf("delete managed vault source: %v", err)
	}
	if metadataWrites != 2 || dataWrites != 2 || metadataDeletes != 1 {
		t.Fatalf(
			"vault calls metadata writes=%d data writes=%d metadata deletes=%d",
			metadataWrites,
			dataWrites,
			metadataDeletes,
		)
	}
}

func TestRepositoryManagedVaultPutFailureRetainsProvisioningAndQuotaUntilRetry(t *testing.T) {
	ctx := context.Background()
	repo, pool := newRepositoryTestStore(t)
	quotaRepo := teamquota.NewRepository(pool)
	repo.teamQuotaStore = quotaRepo
	fakeVault := newRepositoryFakeVault(t)
	fakeVault.failNextDataWrite()
	repo.defaultStorageKind = CredentialSourceStorageKindHashiCorpVault
	repo.vaultResolver = fakeVault.resolver

	teamID := "team-vault-put-" + uuid.NewString()
	request := staticHeadersSourceWriteRequest("github-source", "token")
	request.StorageKind = CredentialSourceStorageKindHashiCorpVault
	if _, err := repo.PutSource(ctx, teamID, request); err == nil {
		t.Fatal("put managed vault source succeeded, want injected Vault failure")
	}

	sourceID, status, version := rawCredentialSourceState(t, pool, teamID, request.Name)
	if status != credentialSourceStatusProvisioning || version != 1 {
		t.Fatalf("staged source state = status %q version %d, want provisioning/1", status, version)
	}
	if got := rawCredentialSourceVersionCount(t, pool, sourceID); got != 1 {
		t.Fatalf("staged version count = %d, want 1", got)
	}
	if source, err := repo.GetSourceByRef(ctx, teamID, request.Name); err != nil || source != nil {
		t.Fatalf("runtime source during provisioning = (%#v, %v), want nil", source, err)
	}
	if got := committedControlPlaneObjects(t, quotaRepo, teamID); got != 2 {
		t.Fatalf("committed control-plane objects after failed put = %d, want 2", got)
	}

	retried, err := repo.PutSource(ctx, teamID, request)
	if err != nil {
		t.Fatalf("retry managed vault source put: %v", err)
	}
	if retried.Status != credentialSourceStatusActive || retried.CurrentVersion != 1 {
		t.Fatalf("retried source = %#v, want active version 1", retried)
	}
	_, status, version = rawCredentialSourceState(t, pool, teamID, request.Name)
	if status != credentialSourceStatusActive || version != 1 {
		t.Fatalf("retried source state = status %q version %d, want active/1", status, version)
	}
	if got := rawCredentialSourceVersionCount(t, pool, sourceID); got != 1 {
		t.Fatalf("retried version count = %d, want 1", got)
	}
	snapshot := fakeVault.snapshot()
	if len(snapshot.dataWritePaths) != 2 || snapshot.dataWritePaths[0] != snapshot.dataWritePaths[1] {
		t.Fatalf("managed Vault retry paths = %#v, want two writes to one stable path", snapshot.dataWritePaths)
	}
	if got := committedControlPlaneObjects(t, quotaRepo, teamID); got != 2 {
		t.Fatalf("committed control-plane objects after retry = %d, want 2", got)
	}
}

func TestRepositoryManagedVaultRotationFailureReusesVersionAndFailsClosed(t *testing.T) {
	ctx := context.Background()
	repo, _ := newRepositoryTestStore(t)
	fakeVault := newRepositoryFakeVault(t)
	repo.defaultStorageKind = CredentialSourceStorageKindHashiCorpVault
	repo.vaultResolver = fakeVault.resolver

	teamID := "team-vault-rotate-" + uuid.NewString()
	request := staticHeadersSourceWriteRequest("github-source", "old-token")
	request.StorageKind = CredentialSourceStorageKindHashiCorpVault
	first, err := repo.PutSource(ctx, teamID, request)
	if err != nil {
		t.Fatalf("put initial managed vault source: %v", err)
	}
	source, err := repo.GetSourceByRef(ctx, teamID, request.Name)
	if err != nil || source == nil {
		t.Fatalf("get initial managed source = (%#v, %v)", source, err)
	}
	if err := repo.UpsertBindings(ctx, &BindingRecord{
		TeamID:    teamID,
		SandboxID: "sandbox-1",
		Bindings: []CredentialBinding{{
			Ref:           "github-api",
			SourceRef:     request.Name,
			SourceID:      source.ID,
			SourceVersion: first.CurrentVersion,
		}},
	}); err != nil {
		t.Fatalf("bind initial source: %v", err)
	}

	request.Spec.StaticHeaders.Values["token"] = "new-token"
	fakeVault.failNextDataWrite()
	if _, err := repo.PutSource(ctx, teamID, request); err == nil {
		t.Fatal("rotate managed vault source succeeded, want injected Vault failure")
	}
	_, status, version := rawCredentialSourceState(t, repo.pool, teamID, request.Name)
	if status != credentialSourceStatusProvisioning || version != 2 {
		t.Fatalf("failed rotation state = status %q version %d, want provisioning/2", status, version)
	}
	if got := rawCredentialSourceVersionCount(t, repo.pool, source.ID); got != 2 {
		t.Fatalf("failed rotation version count = %d, want old plus staged version", got)
	}
	bindings, err := repo.GetBindings(ctx, teamID, "sandbox-1")
	if err != nil {
		t.Fatalf("get bindings after failed rotation: %v", err)
	}
	if bindings == nil || len(bindings.Bindings) != 1 || bindings.Bindings[0].SourceVersion != 1 {
		t.Fatalf("binding after failed rotation = %#v, want old version 1", bindings)
	}
	if resolved, err := repo.GetSourceVersion(ctx, source.ID, 1); err != nil || resolved != nil {
		t.Fatalf("runtime lookup during provisioning = (%#v, %v), want fail closed nil", resolved, err)
	}

	rotated, err := repo.PutSource(ctx, teamID, request)
	if err != nil {
		t.Fatalf("retry managed vault source rotation: %v", err)
	}
	if rotated.Status != credentialSourceStatusActive || rotated.CurrentVersion != 2 {
		t.Fatalf("retried rotation = %#v, want active version 2", rotated)
	}
	if got := rawCredentialSourceVersionCount(t, repo.pool, source.ID); got != 1 {
		t.Fatalf("retried rotation version count = %d, want current only", got)
	}
	bindings, err = repo.GetBindings(ctx, teamID, "sandbox-1")
	if err != nil {
		t.Fatalf("get bindings after retry: %v", err)
	}
	if bindings == nil || len(bindings.Bindings) != 1 || bindings.Bindings[0].SourceVersion != 2 {
		t.Fatalf("binding after retry = %#v, want version 2", bindings)
	}
	snapshot := fakeVault.snapshot()
	if len(snapshot.dataWritePaths) != 3 ||
		snapshot.dataWritePaths[0] != snapshot.dataWritePaths[1] ||
		snapshot.dataWritePaths[1] != snapshot.dataWritePaths[2] {
		t.Fatalf("managed Vault rotation paths = %#v, want one stable path", snapshot.dataWritePaths)
	}
}

func TestRepositoryManagedVaultDeleteFailureRetainsDeletingAndQuotaUntilRetry(t *testing.T) {
	ctx := context.Background()
	repo, pool := newRepositoryTestStore(t)
	quotaRepo := teamquota.NewRepository(pool)
	repo.teamQuotaStore = quotaRepo
	fakeVault := newRepositoryFakeVault(t)
	repo.defaultStorageKind = CredentialSourceStorageKindHashiCorpVault
	repo.vaultResolver = fakeVault.resolver

	teamID := "team-vault-delete-" + uuid.NewString()
	request := staticHeadersSourceWriteRequest("github-source", "token")
	request.StorageKind = CredentialSourceStorageKindHashiCorpVault
	if _, err := repo.PutSource(ctx, teamID, request); err != nil {
		t.Fatalf("put managed vault source: %v", err)
	}

	fakeVault.failNextMetadataDelete()
	if err := repo.DeleteSource(ctx, teamID, request.Name); err == nil {
		t.Fatal("delete managed vault source succeeded, want injected Vault failure")
	}
	sourceID, status, version := rawCredentialSourceState(t, pool, teamID, request.Name)
	if sourceID <= 0 || status != credentialSourceStatusDeleting || version != 1 {
		t.Fatalf("failed delete state = id %d status %q version %d, want deleting version 1", sourceID, status, version)
	}
	if got := committedControlPlaneObjects(t, quotaRepo, teamID); got != 2 {
		t.Fatalf("committed control-plane objects after failed delete = %d, want 2", got)
	}
	if source, err := repo.GetSourceByRef(ctx, teamID, request.Name); err != nil || source != nil {
		t.Fatalf("runtime source during deletion = (%#v, %v), want nil", source, err)
	}

	if err := repo.DeleteSource(ctx, teamID, request.Name); err != nil {
		t.Fatalf("retry managed vault source deletion: %v", err)
	}
	var sourceCount int
	if err := pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM credential_sources
		WHERE team_id = $1 AND name = $2
	`, teamID, request.Name).Scan(&sourceCount); err != nil {
		t.Fatalf("count deleted source: %v", err)
	}
	if sourceCount != 0 {
		t.Fatalf("source count after retried delete = %d, want 0", sourceCount)
	}
	if got := committedControlPlaneObjects(t, quotaRepo, teamID); got != 0 {
		t.Fatalf("committed control-plane objects after retried delete = %d, want 0", got)
	}
	snapshot := fakeVault.snapshot()
	if len(snapshot.metadataDeletePaths) != 2 ||
		snapshot.metadataDeletePaths[0] != snapshot.metadataDeletePaths[1] {
		t.Fatalf("managed Vault delete retry paths = %#v, want one stable path", snapshot.metadataDeletePaths)
	}
}

func TestRepositoryManagedVaultDeleteChecksBindingsBeforeVault(t *testing.T) {
	ctx := context.Background()
	repo, _ := newRepositoryTestStore(t)
	fakeVault := newRepositoryFakeVault(t)
	repo.defaultStorageKind = CredentialSourceStorageKindHashiCorpVault
	repo.vaultResolver = fakeVault.resolver

	teamID := "team-vault-in-use-" + uuid.NewString()
	request := staticHeadersSourceWriteRequest("github-source", "token")
	request.StorageKind = CredentialSourceStorageKindHashiCorpVault
	metadata, err := repo.PutSource(ctx, teamID, request)
	if err != nil {
		t.Fatalf("put managed vault source: %v", err)
	}
	source, err := repo.GetSourceByRef(ctx, teamID, request.Name)
	if err != nil || source == nil {
		t.Fatalf("get managed source = (%#v, %v)", source, err)
	}
	if err := repo.UpsertBindings(ctx, &BindingRecord{
		TeamID:    teamID,
		SandboxID: "sandbox-1",
		Bindings: []CredentialBinding{{
			Ref:           "github-api",
			SourceRef:     request.Name,
			SourceID:      source.ID,
			SourceVersion: metadata.CurrentVersion,
		}},
	}); err != nil {
		t.Fatalf("bind managed source: %v", err)
	}

	if err := repo.DeleteSource(ctx, teamID, request.Name); !errors.Is(err, ErrCredentialSourceInUse) {
		t.Fatalf("delete in-use source error = %v, want ErrCredentialSourceInUse", err)
	}
	_, status, _ := rawCredentialSourceState(t, repo.pool, teamID, request.Name)
	if status != credentialSourceStatusActive {
		t.Fatalf("in-use source status = %q, want active", status)
	}
	snapshot := fakeVault.snapshot()
	if len(snapshot.metadataDeletePaths) != 0 {
		t.Fatalf("in-use source triggered Vault delete: %#v", snapshot.metadataDeletePaths)
	}
}

func TestRepositoryExternalVaultReferenceIsReadOnly(t *testing.T) {
	ctx := context.Background()
	repo, pool := newRepositoryTestStore(t)
	fakeVault := newRepositoryFakeVault(t)
	repo.defaultStorageKind = CredentialSourceStorageKindHashiCorpVault
	repo.vaultResolver = fakeVault.resolver

	teamID := "team-vault-external-" + uuid.NewString()
	request := staticHeadersSourceWriteRequest("github-source", "token")
	request.StorageKind = CredentialSourceStorageKindHashiCorpVault
	request.ExternalRef = &CredentialSourceExternalRefSpec{
		Provider:   CredentialSourceExternalProviderHashiCorpVault,
		Connection: "default",
		Mount:      "secret",
		Path:       "sandbox0/credential-sources/" + teamID + "/external",
	}
	if _, err := repo.PutSource(ctx, teamID, request); !errors.Is(err, ErrCredentialSourceExternalRefReadOnly) {
		t.Fatalf("externalRef plus spec error = %v, want ErrCredentialSourceExternalRefReadOnly", err)
	}
	var sourceCount int
	if err := pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM credential_sources
		WHERE team_id = $1
	`, teamID).Scan(&sourceCount); err != nil {
		t.Fatalf("count rejected external source: %v", err)
	}
	if sourceCount != 0 {
		t.Fatalf("source count after rejected externalRef plus spec = %d, want 0", sourceCount)
	}

	request.Spec = CredentialSourceSecretSpec{}
	if _, err := repo.PutSource(ctx, teamID, request); err != nil {
		t.Fatalf("put read-only external Vault reference: %v", err)
	}
	if err := repo.DeleteSource(ctx, teamID, request.Name); err != nil {
		t.Fatalf("delete read-only external Vault reference: %v", err)
	}
	snapshot := fakeVault.snapshot()
	if len(snapshot.metadataWritePaths) != 0 ||
		len(snapshot.dataWritePaths) != 0 ||
		len(snapshot.metadataDeletePaths) != 0 {
		t.Fatalf("Sandbox0 wrote external Vault reference: %#v", snapshot)
	}
}

func TestRepositoryRejectsCredentialSourceStorageAndManagementModeChanges(t *testing.T) {
	ctx := context.Background()
	repo, _ := newRepositoryTestStore(t)
	fakeVault := newRepositoryFakeVault(t)
	repo.vaultResolver = fakeVault.resolver

	teamID := "team-source-immutable-" + uuid.NewString()
	if _, err := repo.PutSource(ctx, teamID, staticHeadersSourceWriteRequest("plaintext-source", "token")); err != nil {
		t.Fatalf("put plaintext source: %v", err)
	}
	toVault := staticHeadersSourceWriteRequest("plaintext-source", "token")
	toVault.StorageKind = CredentialSourceStorageKindHashiCorpVault
	if _, err := repo.PutSource(ctx, teamID, toVault); !errors.Is(err, ErrCredentialSourceStorageKindImmutable) {
		t.Fatalf("storage kind change error = %v, want ErrCredentialSourceStorageKindImmutable", err)
	}

	external := &CredentialSourceWriteRequest{
		Name:         "external-source",
		ResolverKind: "static_headers",
		StorageKind:  CredentialSourceStorageKindHashiCorpVault,
		ExternalRef: &CredentialSourceExternalRefSpec{
			Provider:   CredentialSourceExternalProviderHashiCorpVault,
			Connection: "default",
			Mount:      "secret",
			Path:       "sandbox0/credential-sources/" + teamID + "/external",
		},
	}
	if _, err := repo.PutSource(ctx, teamID, external); err != nil {
		t.Fatalf("put external source: %v", err)
	}
	toManaged := staticHeadersSourceWriteRequest("external-source", "token")
	toManaged.StorageKind = CredentialSourceStorageKindHashiCorpVault
	if _, err := repo.PutSource(ctx, teamID, toManaged); !errors.Is(err, ErrCredentialSourceManagementModeImmutable) {
		t.Fatalf("management mode change error = %v, want ErrCredentialSourceManagementModeImmutable", err)
	}
	snapshot := fakeVault.snapshot()
	if len(snapshot.metadataWritePaths) != 0 || len(snapshot.dataWritePaths) != 0 {
		t.Fatalf("immutable source changes wrote Vault: %#v", snapshot)
	}
}

func TestRepositoryControlPlaneQuotaUsesExactBindingCardinality(t *testing.T) {
	ctx := context.Background()
	repo, pool := newRepositoryTestStore(t)
	quotaRepo := teamquota.NewRepository(pool)
	repo.teamQuotaStore = quotaRepo
	teamID := "team-binding-quota-" + uuid.NewString()
	if err := quotaRepo.UnsafePutTeamPolicyForTest(ctx, teamID, teamquota.Policy{
		Key:   teamquota.KeyControlPlaneObjectCount,
		Kind:  teamquota.KindCapacity,
		Limit: 4,
	}); err != nil {
		t.Fatalf("set team quota: %v", err)
	}

	sourceMetadata, err := repo.PutSource(ctx, teamID, staticHeadersSourceWriteRequest("binding-source", "token"))
	if err != nil {
		t.Fatalf("put source: %v", err)
	}
	source, err := repo.GetSourceByRef(ctx, teamID, sourceMetadata.Name)
	if err != nil || source == nil {
		t.Fatalf("get source = (%#v, %v)", source, err)
	}
	binding := func(ref string) CredentialBinding {
		return CredentialBinding{
			Ref:           ref,
			SourceRef:     source.Name,
			SourceID:      source.ID,
			SourceVersion: source.CurrentVersion,
			Projection: ProjectionSpec{
				Type: CredentialProjectionTypeHTTPHeaders,
				HTTPHeaders: &HTTPHeadersProjection{
					Headers: []ProjectedHeader{{
						Name:          "Authorization",
						ValueTemplate: "Bearer {{ .token }}",
					}},
				},
			},
		}
	}
	record := &BindingRecord{
		TeamID:    teamID,
		SandboxID: "sandbox-1",
		Bindings:  []CredentialBinding{binding("one"), binding("two")},
	}
	if err := repo.UpsertBindings(ctx, record); err != nil {
		t.Fatalf("put two bindings: %v", err)
	}
	record.Bindings = append(record.Bindings, binding("three"))
	if err := repo.UpsertBindings(ctx, record); !teamquota.IsExceeded(err) {
		t.Fatalf("put third binding error = %v, want quota exceeded", err)
	}
	stored, err := repo.GetBindings(ctx, record.TeamID, record.SandboxID)
	if err != nil {
		t.Fatalf("get bindings: %v", err)
	}
	if stored == nil || len(stored.Bindings) != 2 {
		t.Fatalf("stored bindings = %#v, want original two", stored)
	}
	if err := repo.DeleteBindings(ctx, record.TeamID, record.SandboxID); err != nil {
		t.Fatalf("delete bindings: %v", err)
	}
	statuses, err := quotaRepo.ListStatus(ctx, record.TeamID)
	if err != nil {
		t.Fatalf("list team quota status: %v", err)
	}
	for _, status := range statuses {
		if status.Key == teamquota.KeyControlPlaneObjectCount && status.Committed != 2 {
			t.Fatalf("committed control-plane objects = %d, want source pair only", status.Committed)
		}
	}
}

func TestRepositoryRejectsOversizedObjectsBeforePostgresWrite(t *testing.T) {
	ctx := context.Background()
	repo, pool := newRepositoryTestStore(t)
	if repo == nil {
		return
	}

	var sourceCountBefore int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM credential_sources`).Scan(&sourceCountBefore); err != nil {
		t.Fatalf("count credential sources before rejected write: %v", err)
	}
	_, err := repo.PutSource(ctx, "team-1", &CredentialSourceWriteRequest{
		Name:         "oversized",
		ResolverKind: "static_username_password",
		Spec: CredentialSourceSecretSpec{
			StaticUsernamePassword: &StaticUsernamePasswordSourceSpec{
				Password: strings.Repeat("p", int(MaxCredentialSecretBytes)+1),
			},
		},
	})
	if !resourceguard.IsTooLarge(err) {
		t.Fatalf("PutSource() error = %v, want TooLargeError", err)
	}
	var sourceCountAfter int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM credential_sources`).Scan(&sourceCountAfter); err != nil {
		t.Fatalf("count credential sources after rejected write: %v", err)
	}
	if sourceCountAfter != sourceCountBefore {
		t.Fatalf("credential source count = %d, want unchanged %d", sourceCountAfter, sourceCountBefore)
	}

	var bindingCountBefore int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM sandbox_egress_credential_bindings`).Scan(&bindingCountBefore); err != nil {
		t.Fatalf("count bindings before rejected write: %v", err)
	}
	bindings := make([]CredentialBinding, MaxCredentialBindingCount+1)
	err = repo.UpsertBindings(ctx, &BindingRecord{
		TeamID:    "team-1",
		SandboxID: "sandbox-oversized",
		Bindings:  bindings,
	})
	if !resourceguard.IsTooLarge(err) {
		t.Fatalf("UpsertBindings() error = %v, want TooLargeError", err)
	}
	var bindingCountAfter int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM sandbox_egress_credential_bindings`).Scan(&bindingCountAfter); err != nil {
		t.Fatalf("count bindings after rejected write: %v", err)
	}
	if bindingCountAfter != bindingCountBefore {
		t.Fatalf("binding count = %d, want unchanged %d", bindingCountAfter, bindingCountBefore)
	}
}

func newRepositoryTestStore(t *testing.T) (*Repository, *pgxpool.Pool) {
	t.Helper()

	dbURL := os.Getenv("INTEGRATION_DATABASE_URL")
	if dbURL == "" {
		dbURL = os.Getenv("TEST_DATABASE_URL")
	}
	if dbURL == "" {
		t.Skip("missing INTEGRATION_DATABASE_URL or TEST_DATABASE_URL")
		return nil, nil
	}

	ctx := context.Background()
	schema := fmt.Sprintf("egressauth_test_%s", strings.ReplaceAll(uuid.NewString(), "-", ""))
	adminPool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		t.Fatalf("connect test database: %v", err)
	}
	t.Cleanup(adminPool.Close)
	if _, err := adminPool.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatalf("create schema: %v", err)
	}

	pool, err := dbpool.New(ctx, dbpool.Options{
		DatabaseURL:     dbURL,
		Schema:          schema,
		DefaultMaxConns: 10,
		DefaultMinConns: 1,
	})
	if err != nil {
		t.Fatalf("connect schema-scoped pool: %v", err)
	}
	t.Cleanup(pool.Close)
	t.Cleanup(func() {
		_, _ = adminPool.Exec(ctx, "DROP SCHEMA IF EXISTS "+schema+" CASCADE")
	})

	if _, err := pool.Exec(ctx, `
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';
`); err != nil {
		t.Fatalf("create updated_at trigger helper: %v", err)
	}
	if err := migrate.Up(ctx, pool, ".",
		migrate.WithBaseFS(migrations.FS),
		migrate.WithSchema(schema),
		migrate.WithTableName("goose_egressauth_test"),
		migrate.WithLogger(testMigrateLogger{}),
	); err != nil {
		t.Fatalf("migrate egressauth schema: %v", err)
	}
	if err := teamquota.RunMigrations(ctx, pool, nil); err != nil {
		t.Fatalf("migrate team quota schema: %v", err)
	}
	if err := teamquota.NewRepository(pool).UnsafeReplaceDefaultPoliciesForTest(
		ctx,
		teamquotatestutil.CompleteDefaultPolicies(),
	); err != nil {
		t.Fatalf("configure team quota defaults: %v", err)
	}

	return NewRepository(
		pool,
		WithDefaultStorageKind(CredentialSourceStorageKindPlaintextPG),
		WithTeamQuotaStore(teamquotatestutil.NewPermissiveCapacityStore()),
	), pool
}

func staticHeadersSourceWriteRequest(name, token string) *CredentialSourceWriteRequest {
	return &CredentialSourceWriteRequest{
		Name:         name,
		ResolverKind: "static_headers",
		Spec: CredentialSourceSecretSpec{
			StaticHeaders: &StaticHeadersSourceSpec{
				Values: map[string]string{"token": token},
			},
		},
	}
}

type repositoryFakeVault struct {
	resolver *VaultResolver

	mu                    sync.Mutex
	dataWriteFailures     int
	metadataDeleteFailure int
	metadataWritePaths    []string
	dataWritePaths        []string
	metadataDeletePaths   []string
}

type repositoryFakeVaultSnapshot struct {
	metadataWritePaths  []string
	dataWritePaths      []string
	metadataDeletePaths []string
}

func newRepositoryFakeVault(t *testing.T) *repositoryFakeVault {
	t.Helper()
	fake := &repositoryFakeVault{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fake.mu.Lock()
		defer fake.mu.Unlock()
		switch {
		case r.Method == http.MethodPost && strings.Contains(r.URL.Path, "/metadata/"):
			fake.metadataWritePaths = append(fake.metadataWritePaths, r.URL.Path)
			var body struct {
				MaxVersions int `json:"max_versions"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				http.Error(w, "invalid metadata request", http.StatusBadRequest)
				return
			}
			if body.MaxVersions != 1 {
				http.Error(w, "unexpected max_versions", http.StatusBadRequest)
				return
			}
			w.WriteHeader(http.StatusNoContent)
		case r.Method == http.MethodPost && strings.Contains(r.URL.Path, "/data/"):
			fake.dataWritePaths = append(fake.dataWritePaths, r.URL.Path)
			if fake.dataWriteFailures > 0 {
				fake.dataWriteFailures--
				http.Error(w, "injected data write failure", http.StatusInternalServerError)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{})
		case r.Method == http.MethodDelete && strings.Contains(r.URL.Path, "/metadata/"):
			fake.metadataDeletePaths = append(fake.metadataDeletePaths, r.URL.Path)
			if fake.metadataDeleteFailure > 0 {
				fake.metadataDeleteFailure--
				http.Error(w, "injected metadata delete failure", http.StatusInternalServerError)
				return
			}
			w.WriteHeader(http.StatusNoContent)
		default:
			http.Error(w, "unexpected Vault request", http.StatusNotFound)
		}
	}))
	t.Cleanup(server.Close)

	tokenFile := filepath.Join(t.TempDir(), "vault-token")
	if err := os.WriteFile(tokenFile, []byte("test-token"), 0o600); err != nil {
		t.Fatalf("write Vault token: %v", err)
	}
	resolver, err := NewVaultResolver([]VaultConnectionConfig{{
		Name:                "default",
		Address:             server.URL,
		TokenFile:           tokenFile,
		DefaultMount:        "secret",
		AllowedPathPrefixes: []string{"sandbox0/credential-sources/{{teamID}}/"},
	}})
	if err != nil {
		t.Fatalf("new fake Vault resolver: %v", err)
	}
	fake.resolver = resolver
	return fake
}

func (f *repositoryFakeVault) failNextDataWrite() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.dataWriteFailures++
}

func (f *repositoryFakeVault) failNextMetadataDelete() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.metadataDeleteFailure++
}

func (f *repositoryFakeVault) snapshot() repositoryFakeVaultSnapshot {
	f.mu.Lock()
	defer f.mu.Unlock()
	return repositoryFakeVaultSnapshot{
		metadataWritePaths:  append([]string(nil), f.metadataWritePaths...),
		dataWritePaths:      append([]string(nil), f.dataWritePaths...),
		metadataDeletePaths: append([]string(nil), f.metadataDeletePaths...),
	}
}

func rawCredentialSourceState(
	t *testing.T,
	pool *pgxpool.Pool,
	teamID, name string,
) (int64, string, int64) {
	t.Helper()
	var (
		sourceID int64
		status   string
		version  int64
	)
	if err := pool.QueryRow(context.Background(), `
		SELECT id, status, current_version
		FROM credential_sources
		WHERE team_id = $1 AND name = $2
	`, teamID, name).Scan(&sourceID, &status, &version); err != nil {
		t.Fatalf("read raw credential source state: %v", err)
	}
	return sourceID, status, version
}

func rawCredentialSourceVersionCount(t *testing.T, pool *pgxpool.Pool, sourceID int64) int {
	t.Helper()
	var count int
	if err := pool.QueryRow(context.Background(), `
		SELECT COUNT(*)
		FROM credential_source_versions
		WHERE source_id = $1
	`, sourceID).Scan(&count); err != nil {
		t.Fatalf("count raw credential source versions: %v", err)
	}
	return count
}

func committedControlPlaneObjects(
	t *testing.T,
	quotaRepo *teamquota.Repository,
	teamID string,
) int64 {
	t.Helper()
	statuses, err := quotaRepo.ListStatus(context.Background(), teamID)
	if err != nil {
		t.Fatalf("list team quota status: %v", err)
	}
	for _, status := range statuses {
		if status.Key == teamquota.KeyControlPlaneObjectCount {
			return status.Committed
		}
	}
	t.Fatalf("control-plane object quota status is missing")
	return 0
}
