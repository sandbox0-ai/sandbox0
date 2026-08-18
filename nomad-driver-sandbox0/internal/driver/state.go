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

package driver

import "sync"

type taskStore struct {
	mu      sync.RWMutex
	entries map[string]*taskHandle
}

func newTaskStore() *taskStore {
	return &taskStore{entries: make(map[string]*taskHandle)}
}

func (s *taskStore) Set(id string, handle *taskHandle) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries[id] = handle
}

func (s *taskStore) Get(id string) (*taskHandle, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	handle, ok := s.entries[id]
	return handle, ok
}

func (s *taskStore) Delete(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.entries, id)
}
