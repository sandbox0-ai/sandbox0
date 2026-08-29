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
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/nodeenrollment"
	"github.com/sandbox0-ai/sandbox0/pkg/nodebootstrap"
)

const maxArchiveBytes = 8 << 20

type identityFlags struct {
	nodeName       string
	nodeID         string
	nodeUID        string
	agentUID       string
	privateIP      string
	allocationCIDR string
	regionID       string
	clusterID      string
}

func (i *identityFlags) register(flags *flag.FlagSet) {
	flags.StringVar(&i.nodeName, "node-name", "", "exact Nomad node name")
	flags.StringVar(&i.nodeID, "node-id", "", "exact Nomad node ID")
	flags.StringVar(&i.nodeUID, "node-uid", "", "durable Sandbox0 node UID")
	flags.StringVar(&i.agentUID, "agent-uid", "", "ctld agent UID")
	flags.StringVar(&i.privateIP, "private-ip", "", "node private IPv4 address")
	flags.StringVar(&i.allocationCIDR, "allocation-cidr", "", "node allocation CIDR")
	flags.StringVar(&i.regionID, "region-id", "", "Sandbox0 region ID")
	flags.StringVar(&i.clusterID, "cluster-id", "", "Sandbox0 cluster ID")
}

func main() {
	if len(os.Args) < 2 {
		fatal(errors.New("usage: node-runtime-config <render|install> [flags]"))
	}
	var err error
	switch os.Args[1] {
	case "render":
		err = render(os.Args[2:])
	case "install":
		err = install(os.Args[2:])
	default:
		err = errors.New("usage: node-runtime-config <render|install> [flags]")
	}
	if err != nil {
		fatal(err)
	}
}

func render(arguments []string) error {
	flags := flag.NewFlagSet("render", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	var identity identityFlags
	identity.register(flags)
	var templateFile, output, authorityURL, authorityPeerURI string
	flags.StringVar(&templateFile, "template", "", "control-plane template archive")
	flags.StringVar(&output, "output", "", "rendered archive destination")
	flags.StringVar(&authorityURL, "manager-authority-url", "", "manager authority URL")
	flags.StringVar(&authorityPeerURI, "manager-authority-peer-uri", "", "manager authority SPIFFE URI")
	if err := flags.Parse(arguments); err != nil || flags.NArg() != 0 {
		return errors.New("invalid render arguments")
	}
	renderer, err := nodeenrollment.NewRuntimeConfigTemplateFromFile(
		templateFile, authorityURL, authorityPeerURI,
	)
	if err != nil {
		return err
	}
	payload, err := renderer.Render(nodeenrollment.RuntimeConfigIdentity{
		NodeName: identity.nodeName, NodeID: identity.nodeID, NodeUID: identity.nodeUID,
		AgentUID: identity.agentUID, PrivateIP: identity.privateIP,
		AllocationCIDR: identity.allocationCIDR, RegionID: identity.regionID,
		ClusterID: identity.clusterID,
	})
	if err != nil {
		return err
	}
	return writeArchive(output, payload)
}

func install(arguments []string) error {
	flags := flag.NewFlagSet("install", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	var identity identityFlags
	identity.register(flags)
	var archive string
	flags.StringVar(&archive, "archive", "", "rendered node archive")
	if err := flags.Parse(arguments); err != nil || flags.NArg() != 0 {
		return errors.New("invalid install arguments")
	}
	payload, err := readArchive(archive)
	if err != nil {
		return err
	}
	return nodebootstrap.InstallRenderedRuntimeConfig(payload, identity.nodeName,
		identity.nodeID, identity.nodeUID, identity.regionID, identity.clusterID,
		identity.allocationCIDR)
}

func readArchive(path string) ([]byte, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 ||
		info.Size() <= 0 || info.Size() > maxArchiveBytes {
		return nil, errors.New("rendered node archive is unsafe")
	}
	return os.ReadFile(path)
}

func writeArchive(path string, payload []byte) error {
	path = strings.TrimSpace(path)
	if !filepath.IsAbs(path) || filepath.Clean(path) != path || len(payload) == 0 ||
		len(payload) > maxArchiveBytes {
		return errors.New("rendered node archive destination or size is invalid")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	if info, err := os.Lstat(path); err == nil && !info.Mode().IsRegular() {
		return errors.New("refusing to replace a non-regular rendered archive")
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	file, err := os.CreateTemp(filepath.Dir(path), ".node-runtime-config.")
	if err != nil {
		return err
	}
	temporary := file.Name()
	defer os.Remove(temporary)
	if err := file.Chmod(0o600); err != nil {
		file.Close()
		return err
	}
	if _, err := file.Write(payload); err != nil {
		file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	return os.Rename(temporary, path)
}

func fatal(err error) {
	fmt.Fprintf(os.Stderr, "node-runtime-config: %v\n", err)
	os.Exit(1)
}
