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
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/hashicorp/go-hclog"
	"github.com/sandbox0-ai/sandbox0/nomad-driver-sandbox0/internal/driver"
)

func main() {
	var config driver.PluginConfig
	var nomadConfig driver.NomadAllocationConfig
	var devices string
	socket := flag.String("socket", "/run/sandbox0/rootfs-sessiond.sock", "root-owned Unix API socket")
	flag.StringVar(&config.RunscPath, "runsc", "/usr/local/bin/runsc", "runsc binary")
	flag.StringVar(&config.RunscRoot, "runsc-root", "/run/sandbox0/runsc", "runsc state root")
	flag.StringVar(&config.Platform, "platform", "systrap", "gVisor platform")
	flag.StringVar(&config.Overlay2, "overlay2", "none", "gVisor overlay2 mode")
	flag.StringVar(&config.FileAccess, "file-access", "shared", "gVisor file access mode")
	flag.BoolVar(&config.DirectFS, "directfs", true, "enable gVisor DirectFS")
	flag.StringVar(&config.RootFSStatePath, "state", "/var/lib/sandbox0/rootfs-sessions.db", "durable session journal")
	flag.StringVar(&config.RootFSBranchRoot, "branch-root", "/var/lib/sandbox0/rootfs-branches", "durable branch journal root")
	flag.Int64Var(&config.RootFSMaxDirtyTailBytes, "max-dirty-tail-bytes", 10<<30, "maximum unpublished local dirty-tail payload per session")
	flag.StringVar(&config.RootFSMountRoot, "mount-root", "/run/sandbox0/rootfs", "boot-local storage mount root")
	flag.StringVar(&config.RootFSConsumerMountRoot, "consumer-mount-root", "/opt/nomad", "allowed Nomad task mount root")
	flag.StringVar(&config.RootFSConsumerNetNSRoot, "consumer-netns-root", "/var/run/netns", "allowed Nomad network namespace root")
	flag.StringVar(&devices, "nbd-devices", "/dev/nbd0", "comma-separated NBD device paths")
	flag.StringVar(&config.RootFSObjectType, "object-type", "s3", "object-store type")
	flag.StringVar(&config.RootFSObjectBucket, "object-bucket", "", "RootFS object bucket")
	flag.StringVar(&config.RootFSObjectRegion, "object-region", "us-east-1", "object-store region")
	flag.StringVar(&config.RootFSObjectEndpoint, "object-endpoint", "", "object-store endpoint")
	flag.StringVar(&config.RootFSAuthorityURL, "authority-url", "", "regional writer authority HTTPS origin")
	flag.StringVar(&config.RootFSAuthorityCAFile, "authority-ca", "", "writer authority CA file")
	flag.StringVar(&config.RootFSAuthorityClientCertFile, "authority-cert", "", "writer authority client certificate")
	flag.StringVar(&config.RootFSAuthorityClientKeyFile, "authority-key", "", "writer authority client key")
	flag.StringVar(&config.RootFSAuthorityTokenFile, "authority-token", "", "projected writer authority token")
	flag.StringVar(&nomadConfig.Address, "nomad-address", "http://127.0.0.1:4646", "Nomad HTTP(S) origin")
	flag.StringVar(&nomadConfig.ClusterID, "cluster-id", "", "regional data-plane cluster ID")
	flag.StringVar(&nomadConfig.NodeID, "nomad-node-id", "", "Nomad client node ID")
	flag.StringVar(&nomadConfig.TokenFile, "nomad-token-file", "", "Nomad ACL token file")
	flag.StringVar(&nomadConfig.CAFile, "nomad-ca", "", "Nomad HTTPS CA file")
	flag.StringVar(&nomadConfig.CertFile, "nomad-cert", "", "Nomad HTTPS client certificate")
	flag.StringVar(&nomadConfig.KeyFile, "nomad-key", "", "Nomad HTTPS client key")
	flag.StringVar(&config.RuntimeSlotJournalPath, "runtime-slot-journal", "/var/lib/sandbox0/runtime-slots.db", "durable runtime slot cleanup journal")
	flag.BoolVar(&nomadConfig.RuntimeSlotChannelEnabled, "runtime-slot-node-channel", false, "enable the authenticated outbound regional node channel")
	flag.StringVar(&nomadConfig.RuntimeSlotNodeUID, "runtime-slot-node-uid", "", "authenticated regional node UID")
	flag.StringVar(&nomadConfig.RuntimeSlotChannelPeerURISAN, "runtime-slot-channel-peer-uri-san", "", "exact regional node-channel SPIFFE URI SAN")
	flag.StringVar(&nomadConfig.RuntimeSlotControlRoot, "runtime-slot-control-root", "/var/run/sandbox0/nomad-slots", "allowed root-owned task control socket directory")
	flag.StringVar(&nomadConfig.RuntimeSlotCtldNetworkSocket, "runtime-slot-ctld-network-socket", "/run/sandbox0/ctld-runtime-slot-network.sock", "root-owned ctld runtime-slot network control socket")
	flag.Parse()

	config.RootFSEnabled = true
	config.RootFSObjectAccessKey = os.Getenv("SANDBOX0_ROOTFS_OBJECT_ACCESS_KEY")
	config.RootFSObjectSecretKey = os.Getenv("SANDBOX0_ROOTFS_OBJECT_SECRET_KEY")
	for _, device := range strings.Split(devices, ",") {
		if device = strings.TrimSpace(device); device != "" {
			config.RootFSNBDDevices = append(config.RootFSNBDDevices, device)
		}
	}
	logger := hclog.New(&hclog.LoggerOptions{Name: "nomad-rootfs-sessiond", Level: hclog.Info})
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := driver.RunRootFSSessionDaemon(ctx, config, *socket, nomadConfig, logger); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "nomad-rootfs-sessiond: %v\n", err)
		os.Exit(1)
	}
}
