package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/sandbox0-ai/sandbox0/internal/clickhouseobjectgc"
)

type options struct {
	mode              string
	bucket            string
	prefix            string
	region            string
	endpoint          string
	pathStyle         bool
	livePathsFile     string
	manifestFile      string
	minObjectAge      time.Duration
	minManifestAge    time.Duration
	maxDeleteFraction float64
	minLivePaths      int
	confirmPrefix     string
	overwriteManifest bool
}

func main() {
	if err := run(context.Background(), parseOptions()); err != nil {
		fmt.Fprintln(os.Stderr, "clickhouse-oss-gc:", err)
		os.Exit(1)
	}
}

func parseOptions() options {
	var cfg options
	flag.StringVar(&cfg.mode, "mode", "scan", "scan or apply")
	flag.StringVar(&cfg.bucket, "bucket", "", "object storage bucket")
	flag.StringVar(&cfg.prefix, "prefix", "", "non-root ClickHouse object prefix")
	flag.StringVar(&cfg.region, "region", "us-east-1", "S3 signing region")
	flag.StringVar(&cfg.endpoint, "endpoint", "", "S3-compatible endpoint")
	flag.BoolVar(&cfg.pathStyle, "path-style", true, "use path-style S3 requests")
	flag.StringVar(&cfg.livePathsFile, "live-paths", "", "newline-delimited union of ClickHouse live remote paths")
	flag.StringVar(&cfg.manifestFile, "manifest", "", "candidate manifest path")
	flag.DurationVar(&cfg.minObjectAge, "min-object-age", 72*time.Hour, "minimum orphan object age")
	flag.DurationVar(&cfg.minManifestAge, "min-manifest-age", time.Hour, "minimum delay between scan and apply")
	flag.Float64Var(&cfg.maxDeleteFraction, "max-delete-fraction", 0.25, "maximum candidate/listed object ratio allowed on apply")
	flag.IntVar(&cfg.minLivePaths, "min-live-paths", 1, "minimum live paths required on apply")
	flag.StringVar(&cfg.confirmPrefix, "confirm-prefix", "", "must exactly match --prefix in apply mode")
	flag.BoolVar(&cfg.overwriteManifest, "overwrite-manifest", false, "replace an existing scan manifest")
	flag.Parse()
	return cfg
}

func run(ctx context.Context, cfg options) error {
	cfg.mode = strings.ToLower(strings.TrimSpace(cfg.mode))
	if cfg.mode != "scan" && cfg.mode != "apply" {
		return fmt.Errorf("unsupported mode %q", cfg.mode)
	}
	if strings.TrimSpace(cfg.livePathsFile) == "" {
		return fmt.Errorf("--live-paths is required")
	}
	if strings.TrimSpace(cfg.manifestFile) == "" {
		return fmt.Errorf("--manifest is required")
	}
	livePaths, err := readLivePaths(cfg.livePathsFile, cfg.bucket)
	if err != nil {
		return err
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(
		ctx,
		awsconfig.WithRegion(strings.TrimSpace(cfg.region)),
		awsconfig.WithRequestChecksumCalculation(aws.RequestChecksumCalculationWhenRequired),
		awsconfig.WithResponseChecksumValidation(aws.ResponseChecksumValidationWhenRequired),
	)
	if err != nil {
		return fmt.Errorf("load S3 configuration: %w", err)
	}
	client := s3.NewFromConfig(awsCfg, func(options *s3.Options) {
		options.UsePathStyle = cfg.pathStyle
		if endpoint := strings.TrimRight(strings.TrimSpace(cfg.endpoint), "/"); endpoint != "" {
			options.BaseEndpoint = aws.String(endpoint)
		}
	})
	reconciler, err := clickhouseobjectgc.New(client, cfg.bucket, cfg.prefix)
	if err != nil {
		return err
	}

	if cfg.mode == "scan" {
		manifest, err := reconciler.Scan(ctx, livePaths, cfg.minObjectAge)
		if err != nil {
			return err
		}
		if err := writeManifest(cfg.manifestFile, manifest, cfg.overwriteManifest); err != nil {
			return err
		}
		return printJSON(struct {
			Mode             string    `json:"mode"`
			Bucket           string    `json:"bucket"`
			Prefix           string    `json:"prefix"`
			GeneratedAt      time.Time `json:"generated_at"`
			Cutoff           time.Time `json:"cutoff"`
			LivePaths        int       `json:"live_paths"`
			ListedObjects    int       `json:"listed_objects"`
			ListedBytes      int64     `json:"listed_bytes"`
			CandidateObjects int       `json:"candidate_objects"`
			CandidateBytes   int64     `json:"candidate_bytes"`
			Manifest         string    `json:"manifest"`
		}{
			Mode:             "scan",
			Bucket:           manifest.Bucket,
			Prefix:           manifest.Prefix,
			GeneratedAt:      manifest.GeneratedAt,
			Cutoff:           manifest.Cutoff,
			LivePaths:        manifest.LivePathCount,
			ListedObjects:    manifest.ListedObjectCount,
			ListedBytes:      manifest.ListedBytes,
			CandidateObjects: len(manifest.Candidates),
			CandidateBytes:   manifest.CandidateBytes,
			Manifest:         cfg.manifestFile,
		})
	}

	if strings.Trim(strings.TrimSpace(cfg.confirmPrefix), "/") != strings.Trim(strings.TrimSpace(cfg.prefix), "/") {
		return fmt.Errorf("--confirm-prefix must exactly match --prefix")
	}
	manifest, err := readManifest(cfg.manifestFile)
	if err != nil {
		return err
	}
	result, err := reconciler.Delete(ctx, manifest, livePaths, clickhouseobjectgc.DeleteOptions{
		Now:               time.Now().UTC(),
		MinObjectAge:      cfg.minObjectAge,
		MinManifestAge:    cfg.minManifestAge,
		MaxDeleteFraction: cfg.maxDeleteFraction,
		MinLivePaths:      cfg.minLivePaths,
	})
	if err != nil {
		return err
	}
	return printJSON(struct {
		Mode   string                           `json:"mode"`
		Result *clickhouseobjectgc.DeleteResult `json:"result"`
	}{
		Mode:   "apply",
		Result: result,
	})
}

func readLivePaths(path, bucket string) (map[string]struct{}, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open live paths: %w", err)
	}
	defer file.Close()
	prefix := "oss://" + strings.TrimSpace(bucket) + "/"
	paths := make(map[string]struct{})
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		path := strings.TrimSpace(scanner.Text())
		path = strings.TrimPrefix(path, prefix)
		if path != "" {
			paths[path] = struct{}{}
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read live paths: %w", err)
	}
	return paths, nil
}

func writeManifest(path string, manifest *clickhouseobjectgc.Manifest, overwrite bool) error {
	if !overwrite {
		if _, err := os.Stat(path); err == nil {
			return fmt.Errorf("manifest %q already exists; use --overwrite-manifest to replace it", path)
		} else if !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("inspect manifest path: %w", err)
		}
	}
	dir := filepath.Dir(path)
	temp, err := os.CreateTemp(dir, ".clickhouse-oss-gc-*.json")
	if err != nil {
		return fmt.Errorf("create manifest: %w", err)
	}
	tempPath := temp.Name()
	defer os.Remove(tempPath)
	if err := temp.Chmod(0o600); err != nil {
		temp.Close()
		return fmt.Errorf("protect manifest: %w", err)
	}
	encoder := json.NewEncoder(temp)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(manifest); err != nil {
		temp.Close()
		return fmt.Errorf("encode manifest: %w", err)
	}
	if err := temp.Sync(); err != nil {
		temp.Close()
		return fmt.Errorf("sync manifest: %w", err)
	}
	if err := temp.Close(); err != nil {
		return fmt.Errorf("close manifest: %w", err)
	}
	if err := os.Rename(tempPath, path); err != nil {
		return fmt.Errorf("commit manifest: %w", err)
	}
	return nil
}

func readManifest(path string) (*clickhouseobjectgc.Manifest, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open manifest: %w", err)
	}
	defer file.Close()
	manifest := &clickhouseobjectgc.Manifest{}
	if err := json.NewDecoder(file).Decode(manifest); err != nil {
		return nil, fmt.Errorf("decode manifest: %w", err)
	}
	return manifest, nil
}

func printJSON(value any) error {
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(value)
}
