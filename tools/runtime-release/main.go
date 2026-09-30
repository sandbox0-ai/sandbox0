// Runtime release is a local operator of admission authority. It never calls
// pause/resume or changes an existing sandbox assignment.
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/cacheprewarm"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/service"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"gopkg.in/yaml.v3"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "runtime release failed:", err)
		os.Exit(1)
	}
}
func run() error {
	phase := flag.String("phase", "status", "status, inventory, pool-snapshot, pin, authorize-probe, probe, activate, retire-idle")
	configPath := flag.String("config", "/etc/sandbox0/manager.yaml", "manager config, local to control host")
	cluster := flag.String("cluster", "", "exact cluster ID")
	node := flag.String("node", "", "exact Nomad node ID")
	uid := flag.String("node-uid", "", "durable node identity")
	boot := flag.String("node-boot", "", "exact worker boot ID")
	manifest := flag.String("manifest", "", "verified immutable worker artifact JSON")
	source := flag.String("source", "", "exact source commit")
	bundle := flag.String("bundle-sha256", "", "exact worker bundle SHA-256")
	operation := flag.String("operation-id", "", "stable release or synthetic probe operation ID")
	proof := flag.String("probe-operation-id", "", "successful candidate command probe operation")
	compatibility := flag.String("compatibility", "", "exact slot compatibility digest")
	revision := flag.Int64("expected-revision", -1, "compare-and-swap predecessor revision")
	minNodes := flag.Int("minimum-ready-nodes", 1, "candidate ready node floor")
	minSlots := flag.Int("minimum-ready-slots", 2, "candidate spare carrier floor")
	poolID := flag.String("pool", "", "elastic pool ID for idle retirement")
	template := flag.String("template", "default", "canary template")
	signingKey := flag.String("signing-key", internalauth.DefaultInternalJWTPrivateKeyPath, "local manager signing key")
	flag.Parse()
	if *cluster == "" {
		return errors.New("cluster is required")
	}
	data, err := os.ReadFile(*configPath)
	if err != nil {
		return errors.New("manager configuration unavailable")
	}
	var cfg struct {
		DatabaseURL string `yaml:"database_url"`
		HTTPPort    int    `yaml:"http_port"`
	}
	if yaml.Unmarshal([]byte(os.ExpandEnv(string(data))), &cfg) != nil || cfg.DatabaseURL == "" {
		return errors.New("invalid manager configuration")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	pc, err := pgxpool.ParseConfig(cfg.DatabaseURL)
	if err != nil {
		return errors.New("invalid manager database configuration")
	}
	pc.ConnConfig.RuntimeParams["application_name"] = "sandbox0-runtime-release"
	pc.ConnConfig.RuntimeParams["statement_timeout"] = "60000"
	db, err := pgxpool.NewWithConfig(ctx, pc)
	if err != nil {
		return errors.New("manager database unavailable")
	}
	defer db.Close()
	store := sandboxstore.NewPGSandboxStore(db)
	var result any
	switch *phase {
	case "inventory":
		result, err = store.GetRuntimeReleaseInventory(ctx, *cluster)
	case "pool-snapshot":
		result, err = store.GetRuntimeNodePoolSnapshot(ctx, *poolID)
	case "status":
		result, err = store.GetRuntimeReleasePolicy(ctx, *cluster)
	case "pin":
		var artifact sandboxstore.RuntimeReleaseArtifact
		var payload []byte
		payload, err = os.ReadFile(*manifest)
		if err == nil && len(payload) <= 65536 {
			decoder := json.NewDecoder(bytes.NewReader(payload))
			decoder.DisallowUnknownFields()
			err = decoder.Decode(&artifact)
			if err == nil {
				if decoder.Decode(&struct{}{}) != io.EOF {
					err = errors.New("manifest has trailing data")
				}
			}
		} else {
			err = errors.New("runtime manifest unavailable or oversized")
		}
		if err == nil {
			result, err = store.PinRuntimeNodeReleaseArtifact(ctx, *cluster, *uid, artifact)
		}
	case "authorize-probe":
		grant := sandboxstore.RuntimeReleaseProbe{OperationID: *operation, ClusterID: *cluster, NodeID: *node, NodeUID: *uid, NodeBootID: *boot, SourceCommit: *source, BundleSHA256: *bundle}
		err = store.AuthorizeRuntimeReleaseProbe(ctx, grant)
		result = grant
	case "probe":
		if cfg.HTTPPort < 1 || cfg.HTTPPort > 65535 {
			return errors.New("invalid manager HTTP port")
		}
		result, err = probeCandidate(ctx, store, *signingKey, cfg.HTTPPort, *operation, *node, *uid, *boot, *template)
	case "activate":
		if *proof == "" {
			return errors.New("a successful exact candidate command probe is required")
		}
		result, err = store.ActivateRuntimeRelease(ctx, sandboxstore.ActivateRuntimeReleaseRequest{ClusterID: *cluster, SourceCommit: *source, BundleSHA256: *bundle, OperationID: *operation, ExpectedRevision: *revision, CompatibilityDigest: *compatibility, MinimumReadyNodes: *minNodes, MinimumReadySlots: *minSlots, ProbeOperationID: *proof})
	case "retire-idle":
		result, err = store.RetireIdlePredecessorRuntimeNodes(ctx, *poolID)
	default:
		return errors.New("unknown runtime release phase")
	}
	if err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(result)
}

func probeCandidate(ctx context.Context, store *sandboxstore.PGSandboxStore, keyPath string, port int, operation, node, uid, boot, template string) (any, error) {
	key, err := internalauth.LoadEd25519PrivateKeyFromFile(keyPath)
	if err != nil {
		return nil, errors.New("manager signing key unavailable")
	}
	signer := internalauth.NewGenerator(internalauth.GeneratorConfig{Caller: internalauth.ServiceClusterGateway, PrivateKey: key, TTL: 30 * time.Second})
	token, err := signer.Generate(internalauth.ServiceManager, "sandbox0-runtime-release-probe", "", internalauth.GenerateOptions{Permissions: []string{"runtime:release-probe"}, Audit: &internalauth.AuditContext{OperationID: operation}})
	if err != nil {
		return nil, err
	}
	payload, err := json.Marshal(map[string]any{"template": template, "target": service.ClaimNodeTarget{NodeID: node, NodeUID: uid, NodeBootID: boot}})
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, fmt.Sprintf("http://127.0.0.1:%d/internal/v1/runtime-release/probe", port), bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(internalauth.DefaultTokenHeader, token)
	client := &http.Client{Timeout: 30 * time.Second}
	response, err := client.Do(req)
	if err != nil {
		return nil, errors.New("candidate claim request failed")
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, 65537))
	if err != nil || len(body) > 65536 {
		return nil, errors.New("invalid candidate claim response")
	}
	claim, apiErr, err := spec.DecodeResponse[service.ClaimResponse](bytes.NewReader(body))
	if err != nil || apiErr != nil || response.StatusCode != http.StatusCreated || claim == nil || claim.SandboxID == "" {
		return nil, errors.New("candidate claim did not reach command ready")
	}
	// Always request cleanup, including failed commands; the hard TTL bounds a
	// lost process. Only the dedicated synthetic sandbox is touched.
	cleanup := func() error {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		cleanupToken, e := signer.Generate(internalauth.ServiceManager, "sandbox0-runtime-release-probe", "", internalauth.GenerateOptions{})
		if e != nil {
			return e
		}
		request, e := http.NewRequestWithContext(cleanupCtx, http.MethodDelete, fmt.Sprintf("http://127.0.0.1:%d/api/v1/sandboxes/%s", port, claim.SandboxID), nil)
		if e != nil {
			return e
		}
		request.Header.Set(internalauth.DefaultTokenHeader, cleanupToken)
		res, e := client.Do(request)
		if e != nil {
			return errors.New("candidate cleanup request failed")
		}
		res.Body.Close()
		if res.StatusCode < 200 || res.StatusCode >= 300 {
			return errors.New("candidate cleanup was rejected")
		}
		for {
			record, e := store.GetSandbox(cleanupCtx, claim.SandboxID)
			if e != nil {
				return errors.New("candidate cleanup verification failed")
			}
			if record == nil || !record.DeletedAt.IsZero() {
				return nil
			}
			select {
			case <-cleanupCtx.Done():
				return errors.New("candidate cleanup did not complete")
			case <-time.After(250 * time.Millisecond):
			}
		}
	}
	cleaned := false
	defer func() {
		if !cleaned {
			_ = cleanup()
		}
	}()
	commandToken, err := internalauth.NewGenerator(internalauth.GeneratorConfig{Caller: internalauth.ServiceManager, PrivateKey: key, TTL: 30 * time.Second}).Generate(internalauth.ServiceProcd, "sandbox0-runtime-release-probe", "", internalauth.GenerateOptions{SandboxID: claim.SandboxID})
	if err != nil {
		return nil, err
	}
	command, err := cacheprewarm.NewCommandClient(client).CreateCommand(ctx, claim.ProcdAddress, commandToken, []string{"sh", "-c", "printf sandbox0-runtime-release-probe"})
	if err != nil || command == nil || command.Stdout == nil || strings.TrimSpace(*command.Stdout) != "sandbox0-runtime-release-probe" {
		return nil, errors.New("authenticated candidate command failed")
	}
	sum := sha256.Sum256([]byte(*command.Stdout))
	digest := hex.EncodeToString(sum[:])
	if err = store.CompleteRuntimeReleaseProbe(ctx, operation, digest); err != nil {
		return nil, err
	}
	if err = cleanup(); err != nil {
		return nil, err
	}
	cleaned = true
	return map[string]any{"operation_id": operation, "sandbox_id": claim.SandboxID, "command_stdout_sha256": digest, "passed": true}, nil
}
