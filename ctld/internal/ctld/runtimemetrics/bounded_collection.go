// Copyright 2026 Sandbox0 Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package runtimemetrics

import (
	"context"
	"sync"
	"time"
)

type boundedCollectionResult[T any] struct {
	value T
	err   error
}

func runBoundedCollection[T any](
	ctx context.Context,
	targets []T,
	maxConcurrency int,
	requestTimeout time.Duration,
	collect func(context.Context, T) (T, error),
) ([]boundedCollectionResult[T], int) {
	results := make([]boundedCollectionResult[T], len(targets))
	semaphore := make(chan struct{}, maxConcurrency)
	var wait sync.WaitGroup
	dispatched := 0
dispatch:
	for index := range targets {
		if ctx.Err() != nil {
			break
		}
		select {
		case semaphore <- struct{}{}:
			if ctx.Err() != nil {
				<-semaphore
				break dispatch
			}
		case <-ctx.Done():
			break dispatch
		}
		dispatched++
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			defer func() { <-semaphore }()
			requestCtx, cancel := context.WithTimeout(ctx, requestTimeout)
			defer cancel()
			results[index].value, results[index].err = collect(requestCtx, targets[index])
		}(index)
	}
	wait.Wait()
	return results, dispatched
}

func rotateCollectionTargets[T any](targets []T, cursor int) ([]T, int) {
	if len(targets) == 0 {
		return nil, 0
	}
	start := cursor % len(targets)
	if start == 0 {
		return targets, start
	}
	ordered := make([]T, 0, len(targets))
	ordered = append(ordered, targets[start:]...)
	ordered = append(ordered, targets[:start]...)
	return ordered, start
}

func nextCollectionTargetCursor(targetCount, start, dispatched int) int {
	if targetCount == 0 || dispatched >= targetCount {
		return 0
	}
	return (start + dispatched) % targetCount
}

func boundedJitterDelay(interval, jitter time.Duration, random func() float64) time.Duration {
	if jitter <= 0 {
		return interval
	}
	value := random()
	if value < 0 {
		value = 0
	}
	if value > 1 {
		value = 1
	}
	offset := time.Duration((value*2 - 1) * float64(jitter))
	return interval + offset
}
