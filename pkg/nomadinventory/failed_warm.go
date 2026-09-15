package nomadinventory

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/google/uuid"
)

// FailedWarmPage reads positive scheduling candidates, never physical cleanup
// evidence. A small page and cursor bound each pass without collecting the
// historical allocation catalog. Validate the entire page before any mutation.
func FailedWarmPage(ctx context.Context, client *http.Client, baseURL *url.URL, base, after string, headers http.Header) ([]Allocation, string, error) {
	const limit = 8
	if client == nil || baseURL == nil || base == "" || base != strings.TrimSpace(base) || len(base) > 256 || len(after) > 4096 {
		return nil, "", errors.New("invalid failed carrier inventory request")
	}
	jobs := make([]string, WarmJobShardCount)
	for shard := range WarmJobShardCount {
		id, _ := WarmJobID(base, shard)
		jobs[shard] = "JobID == " + strconv.Quote(id)
	}
	query := url.Values{
		"filter":    {`ClientStatus == "failed" and DesiredStatus == "run" and NextAllocation == "" and (` + strings.Join(jobs, " or ") + ")"},
		"namespace": {"default"}, "per_page": {strconv.Itoa(limit)},
		"resources": {"false"}, "task_states": {"false"},
	}
	if after != "" {
		query.Set("next_token", after)
	}
	target := *baseURL
	target.Path = strings.TrimSuffix(target.Path, "/") + "/v1/allocations"
	target.RawQuery = query.Encode()
	var batch []Allocation
	next, err := readPage(ctx, client, target.String(), headers, &batch)
	if err != nil {
		return nil, "", err
	}
	if len(batch) > limit || len(next) > 4096 || (next != "" && (next == after || len(batch) == 0)) {
		return nil, "", errors.New("failed carrier inventory exceeded its bounds or repeated its cursor")
	}
	seen := make(map[string]bool, len(batch))
	for _, a := range batch {
		allocationID, err := uuid.Parse(a.ID)
		if err != nil || allocationID.String() != a.ID || seen[a.ID] || a.NodeID == "" || a.NodeID != strings.TrimSpace(a.NodeID) || len(a.NodeID) > 512 || strings.ContainsAny(a.NodeID, "/\\?#\x00") ||
			a.Namespace != "default" || !IsWarmJob(base, a.JobID) || a.ClientStatus != "failed" || a.DesiredStatus != "run" || a.NextAllocation != "" {
			return nil, "", errors.New("failed carrier inventory returned an inexact candidate")
		}
		seen[a.ID] = true
	}
	return batch, next, nil
}
