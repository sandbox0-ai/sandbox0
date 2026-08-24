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

package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/sandbox0-ai/sandbox0/pkg/objectstore"

	"github.com/sandbox0-ai/sandbox0/nomad-driver-sandbox0/internal/rootfsbuilder"
)

func main() {
	source := flag.String("source", "", "absolute source root directory")
	output := flag.String("output", "", "output GenerationDescriptor JSON path")
	rootfsID := flag.String("rootfs-id", "", "region-scoped RootFS ID")
	image := flag.String("image", "", "temporary XFS image path")
	logicalSize := flag.Int64("logical-size", 300<<20, "logical XFS size in bytes")
	objectType := flag.String("object-type", "s3", "object storage type")
	bucket := flag.String("bucket", "", "object storage bucket")
	endpoint := flag.String("endpoint", "", "object storage endpoint")
	region := flag.String("region", "us-east-1", "object storage region")
	accessKey := flag.String("access-key", "", "object storage access key")
	secretKey := flag.String("secret-key", "", "object storage secret key")
	prefix := flag.String("prefix", "nomad-gvisor-poc/rootfs", "immutable object prefix")
	flag.Parse()

	if err := run(*source, *output, *rootfsID, *image, *logicalSize, *objectType, *bucket,
		*endpoint, *region, *accessKey, *secretKey, *prefix); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(source, output, rootfsID, image string, logicalSize int64, objectType, bucket,
	endpoint, region, accessKey, secretKey, prefix string) error {
	if source == "" || output == "" || rootfsID == "" || image == "" || bucket == "" {
		return fmt.Errorf("-source, -output, -rootfs-id, -image, and -bucket are required")
	}
	absoluteSource, err := filepath.Abs(source)
	if err != nil {
		return err
	}
	absoluteOutput, err := filepath.Abs(output)
	if err != nil {
		return err
	}
	absoluteImage, err := filepath.Abs(image)
	if err != nil {
		return err
	}
	store, err := objectstore.Create(objectstore.Config{
		Type: objectType, Bucket: bucket, Region: region, Endpoint: endpoint,
		AccessKey: accessKey, SecretKey: secretKey,
	})
	if err != nil {
		return err
	}
	if err := store.Create(); err != nil && !strings.Contains(strings.ToLower(err.Error()), "alreadyownedbyyou") {
		return fmt.Errorf("create bucket: %w", err)
	}
	conditional, ok := store.(objectstore.ContextConditionalStore)
	if !ok || !objectstore.SupportsContextConditionalCreate(store) {
		return fmt.Errorf("object store %s does not support contextual conditional access", store)
	}
	descriptor, err := rootfsbuilder.Build(context.Background(), conditional, rootfsbuilder.Options{
		SourceRoot: absoluteSource, ImagePath: absoluteImage, LogicalSize: logicalSize,
		RootFSID: rootfsID, ObjectPrefix: prefix,
	})
	if err != nil {
		return err
	}
	payload, err := json.MarshalIndent(descriptor, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(absoluteOutput, append(payload, '\n'), 0o600); err != nil {
		return err
	}
	fmt.Printf("generation=%s block-root=%s output=%s\n",
		descriptor.GenerationID, descriptor.CurrentBlockHead, absoluteOutput)
	return nil
}
