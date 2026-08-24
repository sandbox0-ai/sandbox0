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

import "github.com/sandbox0-ai/sandbox0/pkg/hostmount"

type Mounter = hostmount.Mounter
type systemMounter = hostmount.System

func validateRootfsPath(source, allowedRoot string) (string, error) {
	return hostmount.ValidateRootFSPath(source, allowedRoot)
}

func validateExistingPath(source, allowedRoot string) (string, error) {
	return hostmount.ValidateExistingPath(source, allowedRoot)
}

func startsWithDotDot(path string) bool {
	return hostmount.StartsWithDotDot(path)
}
