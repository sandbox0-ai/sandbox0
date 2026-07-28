package clickhouseobjectgc

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type fakeObjectClient struct {
	listOutputs []*s3.ListObjectsV2Output
	listIndex   int
	deleteSizes []int
}

func (f *fakeObjectClient) ListObjectsV2(
	context.Context,
	*s3.ListObjectsV2Input,
	...func(*s3.Options),
) (*s3.ListObjectsV2Output, error) {
	if f.listIndex >= len(f.listOutputs) {
		return nil, fmt.Errorf("unexpected list call %d", f.listIndex)
	}
	output := f.listOutputs[f.listIndex]
	f.listIndex++
	return output, nil
}

func (f *fakeObjectClient) DeleteObjects(
	_ context.Context,
	input *s3.DeleteObjectsInput,
	_ ...func(*s3.Options),
) (*s3.DeleteObjectsOutput, error) {
	f.deleteSizes = append(f.deleteSizes, len(input.Delete.Objects))
	return &s3.DeleteObjectsOutput{}, nil
}

func TestScanRequiresAgeAndAbsenceFromLiveSet(t *testing.T) {
	now := time.Date(2026, 7, 28, 10, 0, 0, 0, time.UTC)
	old := now.Add(-96 * time.Hour)
	recent := now.Add(-time.Hour)
	truncated := true
	client := &fakeObjectClient{listOutputs: []*s3.ListObjectsV2Output{
		{
			Contents: []types.Object{
				testObject("clickhouse/region/live", 10, old),
				testObject("clickhouse/region/orphan-a", 20, old),
				testObject("clickhouse/region/recent", 30, recent),
			},
			IsTruncated:           &truncated,
			NextContinuationToken: aws.String("next"),
		},
		{
			Contents: []types.Object{
				testObject("clickhouse/region/orphan-b", 40, old),
			},
		},
	}}
	reconciler, err := New(client, "bucket-1", "clickhouse/region")
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	reconciler.now = func() time.Time { return now }

	manifest, err := reconciler.Scan(context.Background(), map[string]struct{}{
		"clickhouse/region/live": {},
	}, 72*time.Hour)
	if err != nil {
		t.Fatalf("Scan() error = %v", err)
	}
	if manifest.ListedObjectCount != 4 || manifest.ListedBytes != 100 {
		t.Fatalf("listed totals = (%d, %d)", manifest.ListedObjectCount, manifest.ListedBytes)
	}
	if len(manifest.Candidates) != 2 || manifest.CandidateBytes != 60 {
		t.Fatalf("candidate totals = (%d, %d)", len(manifest.Candidates), manifest.CandidateBytes)
	}
	if manifest.Candidates[0].Key != "clickhouse/region/orphan-a" ||
		manifest.Candidates[1].Key != "clickhouse/region/orphan-b" {
		t.Fatalf("candidates = %#v", manifest.Candidates)
	}
}

func TestDeleteRechecksLiveSetAndChunksRequests(t *testing.T) {
	now := time.Date(2026, 7, 28, 10, 0, 0, 0, time.UTC)
	old := now.Add(-96 * time.Hour)
	candidates := make([]Candidate, 0, 1002)
	for index := 0; index < 1002; index++ {
		candidates = append(candidates, Candidate{
			Key:          fmt.Sprintf("clickhouse/region/object-%04d", index),
			Size:         2,
			LastModified: old,
		})
	}
	manifest := &Manifest{
		Version:           ManifestVersion,
		Bucket:            "bucket-1",
		Prefix:            "clickhouse/region/",
		GeneratedAt:       now.Add(-2 * time.Hour),
		ListedObjectCount: 2000,
		Candidates:        candidates,
	}
	client := &fakeObjectClient{}
	reconciler, err := New(client, "bucket-1", "clickhouse/region")
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	result, err := reconciler.Delete(
		context.Background(),
		manifest,
		map[string]struct{}{candidates[0].Key: {}},
		DeleteOptions{
			Now:               now,
			MinObjectAge:      72 * time.Hour,
			MinManifestAge:    time.Hour,
			MaxDeleteFraction: 0.6,
			MinLivePaths:      1,
		},
	)
	if err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	if result.SkippedLive != 1 || result.DeletedObjects != 1001 || result.DeletedBytes != 2002 {
		t.Fatalf("delete result = %#v", result)
	}
	if len(client.deleteSizes) != 2 || client.deleteSizes[0] != 1000 || client.deleteSizes[1] != 1 {
		t.Fatalf("delete request sizes = %v", client.deleteSizes)
	}
}

func TestDeleteRejectsUnexpectedCandidateFraction(t *testing.T) {
	now := time.Now().UTC()
	client := &fakeObjectClient{}
	reconciler, err := New(client, "bucket-1", "clickhouse/region")
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	_, err = reconciler.Delete(
		context.Background(),
		&Manifest{
			Version:           ManifestVersion,
			Bucket:            "bucket-1",
			Prefix:            "clickhouse/region/",
			GeneratedAt:       now.Add(-2 * time.Hour),
			ListedObjectCount: 2,
			Candidates: []Candidate{{
				Key:          "clickhouse/region/orphan",
				LastModified: now.Add(-96 * time.Hour),
			}},
		},
		map[string]struct{}{"clickhouse/region/live": {}},
		DeleteOptions{
			Now:               now,
			MinObjectAge:      72 * time.Hour,
			MinManifestAge:    time.Hour,
			MaxDeleteFraction: 0.25,
			MinLivePaths:      1,
		},
	)
	if err == nil {
		t.Fatal("Delete() accepted an excessive candidate fraction")
	}
}

func TestNewRejectsRootPrefix(t *testing.T) {
	if _, err := New(&fakeObjectClient{}, "bucket-1", "/"); err == nil {
		t.Fatal("New() accepted a root prefix")
	}
}

func testObject(key string, size int64, modified time.Time) types.Object {
	return types.Object{
		Key:          aws.String(key),
		Size:         aws.Int64(size),
		LastModified: aws.Time(modified),
	}
}
