package clickhouseobjectgc

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

const (
	ManifestVersion    = 1
	maxDeleteBatchSize = 1000
)

type objectClient interface {
	ListObjectsV2(context.Context, *s3.ListObjectsV2Input, ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
	DeleteObjects(context.Context, *s3.DeleteObjectsInput, ...func(*s3.Options)) (*s3.DeleteObjectsOutput, error)
}

type Candidate struct {
	Key          string    `json:"key"`
	Size         int64     `json:"size"`
	LastModified time.Time `json:"last_modified"`
	ETag         string    `json:"etag,omitempty"`
}

type Manifest struct {
	Version           int         `json:"version"`
	Bucket            string      `json:"bucket"`
	Prefix            string      `json:"prefix"`
	GeneratedAt       time.Time   `json:"generated_at"`
	Cutoff            time.Time   `json:"cutoff"`
	LivePathCount     int         `json:"live_path_count"`
	ListedObjectCount int         `json:"listed_object_count"`
	ListedBytes       int64       `json:"listed_bytes"`
	CandidateBytes    int64       `json:"candidate_bytes"`
	Candidates        []Candidate `json:"candidates"`
}

type DeleteOptions struct {
	Now               time.Time
	MinObjectAge      time.Duration
	MinManifestAge    time.Duration
	MaxDeleteFraction float64
	MinLivePaths      int
}

type DeleteResult struct {
	ManifestCandidates int   `json:"manifest_candidates"`
	SkippedLive        int   `json:"skipped_live"`
	DeletedObjects     int   `json:"deleted_objects"`
	DeletedBytes       int64 `json:"deleted_bytes"`
}

type Reconciler struct {
	client objectClient
	bucket string
	prefix string
	now    func() time.Time
}

func New(client objectClient, bucket, prefix string) (*Reconciler, error) {
	if client == nil {
		return nil, fmt.Errorf("object client is required")
	}
	bucket = strings.TrimSpace(bucket)
	if bucket == "" {
		return nil, fmt.Errorf("bucket is required")
	}
	normalizedPrefix, err := normalizePrefix(prefix)
	if err != nil {
		return nil, err
	}
	return &Reconciler{
		client: client,
		bucket: bucket,
		prefix: normalizedPrefix,
		now: func() time.Time {
			return time.Now().UTC()
		},
	}, nil
}

// Scan lists the exact ClickHouse prefix and records only objects that are
// older than minAge and absent from the union of live remote-path snapshots.
func (r *Reconciler) Scan(ctx context.Context, livePaths map[string]struct{}, minAge time.Duration) (*Manifest, error) {
	if minAge <= 0 {
		return nil, fmt.Errorf("minimum object age must be positive")
	}
	if len(livePaths) == 0 {
		return nil, fmt.Errorf("at least one live ClickHouse remote path is required")
	}
	now := r.timestamp()
	manifest := &Manifest{
		Version:       ManifestVersion,
		Bucket:        r.bucket,
		Prefix:        r.prefix,
		GeneratedAt:   now,
		Cutoff:        now.Add(-minAge),
		LivePathCount: len(livePaths),
		Candidates:    make([]Candidate, 0),
	}

	var continuationToken *string
	for {
		output, err := r.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(r.bucket),
			Prefix:            aws.String(r.prefix),
			ContinuationToken: continuationToken,
		})
		if err != nil {
			return nil, fmt.Errorf("list ClickHouse objects: %w", err)
		}
		for _, object := range output.Contents {
			key := aws.ToString(object.Key)
			if !strings.HasPrefix(key, r.prefix) {
				return nil, fmt.Errorf("object %q is outside prefix %q", key, r.prefix)
			}
			size := aws.ToInt64(object.Size)
			modified := aws.ToTime(object.LastModified).UTC()
			manifest.ListedObjectCount++
			manifest.ListedBytes += size
			if _, live := livePaths[key]; live || !modified.Before(manifest.Cutoff) {
				continue
			}
			manifest.Candidates = append(manifest.Candidates, Candidate{
				Key:          key,
				Size:         size,
				LastModified: modified,
				ETag:         strings.Trim(aws.ToString(object.ETag), `"`),
			})
			manifest.CandidateBytes += size
		}
		if !aws.ToBool(output.IsTruncated) {
			break
		}
		if output.NextContinuationToken == nil || aws.ToString(output.NextContinuationToken) == "" {
			return nil, fmt.Errorf("truncated object listing has no continuation token")
		}
		continuationToken = output.NextContinuationToken
	}
	sort.Slice(manifest.Candidates, func(i, j int) bool {
		return manifest.Candidates[i].Key < manifest.Candidates[j].Key
	})
	return manifest, nil
}

// Delete removes candidates from an earlier manifest after rechecking the
// current live set and every destructive guardrail.
func (r *Reconciler) Delete(
	ctx context.Context,
	manifest *Manifest,
	livePaths map[string]struct{},
	options DeleteOptions,
) (*DeleteResult, error) {
	if options.Now.IsZero() {
		options.Now = r.timestamp()
	}
	if options.MinLivePaths <= 0 {
		options.MinLivePaths = 1
	}
	if err := r.validateDelete(manifest, livePaths, options); err != nil {
		return nil, err
	}
	cutoff := options.Now.UTC().Add(-options.MinObjectAge)
	eligible := make([]Candidate, 0, len(manifest.Candidates))
	result := &DeleteResult{ManifestCandidates: len(manifest.Candidates)}
	for _, candidate := range manifest.Candidates {
		if !strings.HasPrefix(candidate.Key, r.prefix) {
			return nil, fmt.Errorf("candidate %q is outside prefix %q", candidate.Key, r.prefix)
		}
		if _, live := livePaths[candidate.Key]; live {
			result.SkippedLive++
			continue
		}
		if !candidate.LastModified.Before(cutoff) {
			return nil, fmt.Errorf("candidate %q is newer than the object-age cutoff", candidate.Key)
		}
		eligible = append(eligible, candidate)
	}

	for len(eligible) > 0 {
		chunkSize := min(len(eligible), maxDeleteBatchSize)
		chunk := eligible[:chunkSize]
		objects := make([]types.ObjectIdentifier, 0, len(chunk))
		for _, candidate := range chunk {
			objects = append(objects, types.ObjectIdentifier{Key: aws.String(candidate.Key)})
		}
		quiet := true
		output, err := r.client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
			Bucket: aws.String(r.bucket),
			Delete: &types.Delete{
				Objects: objects,
				Quiet:   &quiet,
			},
		})
		if err != nil {
			return nil, fmt.Errorf("delete ClickHouse orphan batch: %w", err)
		}
		if len(output.Errors) > 0 {
			first := output.Errors[0]
			return nil, fmt.Errorf(
				"delete ClickHouse orphan %q: %s: %s",
				aws.ToString(first.Key),
				aws.ToString(first.Code),
				aws.ToString(first.Message),
			)
		}
		for _, candidate := range chunk {
			result.DeletedObjects++
			result.DeletedBytes += candidate.Size
		}
		eligible = eligible[chunkSize:]
	}
	return result, nil
}

func (r *Reconciler) validateDelete(
	manifest *Manifest,
	livePaths map[string]struct{},
	options DeleteOptions,
) error {
	if manifest == nil {
		return fmt.Errorf("manifest is required")
	}
	if manifest.Version != ManifestVersion {
		return fmt.Errorf("unsupported manifest version %d", manifest.Version)
	}
	if manifest.Bucket != r.bucket || manifest.Prefix != r.prefix {
		return fmt.Errorf(
			"manifest target %s/%s does not match %s/%s",
			manifest.Bucket,
			manifest.Prefix,
			r.bucket,
			r.prefix,
		)
	}
	if options.MinObjectAge <= 0 {
		return fmt.Errorf("minimum object age must be positive")
	}
	if options.MinManifestAge < 0 {
		return fmt.Errorf("minimum manifest age cannot be negative")
	}
	if manifest.GeneratedAt.After(options.Now.UTC().Add(-options.MinManifestAge)) {
		return fmt.Errorf("manifest is younger than %s", options.MinManifestAge)
	}
	if len(livePaths) < options.MinLivePaths {
		return fmt.Errorf("live path count %d is below required minimum %d", len(livePaths), options.MinLivePaths)
	}
	if options.MaxDeleteFraction <= 0 || options.MaxDeleteFraction > 1 {
		return fmt.Errorf("maximum delete fraction must be in (0, 1]")
	}
	if manifest.ListedObjectCount <= 0 {
		return fmt.Errorf("manifest has no listed objects")
	}
	fraction := float64(len(manifest.Candidates)) / float64(manifest.ListedObjectCount)
	if fraction > options.MaxDeleteFraction {
		return fmt.Errorf(
			"candidate fraction %.4f exceeds maximum %.4f",
			fraction,
			options.MaxDeleteFraction,
		)
	}
	return nil
}

func normalizePrefix(prefix string) (string, error) {
	prefix = strings.Trim(strings.TrimSpace(prefix), "/")
	if prefix == "" {
		return "", fmt.Errorf("non-root prefix is required")
	}
	if strings.Contains(prefix, "..") {
		return "", fmt.Errorf("prefix must not contain '..'")
	}
	return prefix + "/", nil
}

func (r *Reconciler) timestamp() time.Time {
	if r == nil || r.now == nil {
		return time.Now().UTC()
	}
	return r.now().UTC()
}
